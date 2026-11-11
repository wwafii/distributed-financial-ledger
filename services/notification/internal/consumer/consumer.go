package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/sender"
)

// NotificationConfig holds backoff and routing configuration for notification worker
type NotificationConfig struct {
	MaxRetries        int
	InitialBackoff    time.Duration
	BackoffMultiplier float64
	WebhookURL        string
	DLQTopic          string
	SleepFn           func(d time.Duration) // for testability without real delay
}

// DefaultConfig provides production-grade resilience defaults
func DefaultConfig() NotificationConfig {
	return NotificationConfig{
		MaxRetries:        3,
		InitialBackoff:    500 * time.Millisecond,
		BackoffMultiplier: 2.0,
		WebhookURL:        "http://localhost:8088/api/v1/webhook",
		DLQTopic:          "transaction.dlq",
		SleepFn:           time.Sleep,
	}
}

// ConfigFromEnv loads config from environment variables with safe defaults
func ConfigFromEnv() NotificationConfig {
	cfg := DefaultConfig()

	if val := os.Getenv("NOTIFICATION_MAX_RETRIES"); val != "" {
		if r, err := strconv.Atoi(val); err == nil && r >= 0 {
			cfg.MaxRetries = r
		}
	}
	if val := os.Getenv("NOTIFICATION_INITIAL_BACKOFF_MS"); val != "" {
		if ms, err := strconv.Atoi(val); err == nil && ms > 0 {
			cfg.InitialBackoff = time.Duration(ms) * time.Millisecond
		}
	}
	if val := os.Getenv("WEBHOOK_TARGET_URL"); val != "" {
		cfg.WebhookURL = val
	}
	if val := os.Getenv("KAFKA_DLQ_TOPIC"); val != "" {
		cfg.DLQTopic = val
	}

	return cfg
}

type NotificationConsumer struct {
	reader    *kafka.Reader
	sender    sender.WebhookClient
	publisher pkgkafka.EventPublisher
	logger    *slog.Logger
	cfg       NotificationConfig
}

func NewNotificationConsumer(
	reader *kafka.Reader,
	s sender.WebhookClient,
	publisher pkgkafka.EventPublisher,
	logger *slog.Logger,
	cfg NotificationConfig,
) *NotificationConsumer {
	if cfg.SleepFn == nil {
		cfg.SleepFn = time.Sleep
	}
	return &NotificationConsumer{
		reader:    reader,
		sender:    s,
		publisher: publisher,
		logger:    logger,
		cfg:       cfg,
	}
}

// ProcessMessage dispatches webhook with exponential backoff and routes poison messages to DLQ
func (c *NotificationConsumer) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	ctx = telemetry.ExtractKafka(ctx, &msg)
	tracer := telemetry.Tracer("notification-consumer")
	ctx, span := tracer.Start(ctx, "Kafka Consumer transaction.created")
	defer span.End()

	var lastErr error
	var attempts int

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		attempts = attempt + 1

		err := c.sender.Send(ctx, c.cfg.WebhookURL, msg.Value)
		if err == nil {
			telemetry.NotificationsTotal.WithLabelValues("success").Inc()
			c.logger.Info("webhook notification delivered successfully",
				"topic", msg.Topic,
				"key", string(msg.Key),
				"attempts", attempts,
			)
			return nil
		}

		lastErr = err

		// If failure is non-retryable (e.g., HTTP 4xx Bad Request / Client Error), abort immediately to DLQ
		if !sender.IsRetryable(err) {
			telemetry.NotificationsTotal.WithLabelValues("client_error").Inc()
			c.logger.Warn("non-retryable webhook failure detected, routing to DLQ directly",
				"topic", msg.Topic,
				"key", string(msg.Key),
				"error", err,
			)
			break
		}

		// If transient failure and we have remaining attempts, backoff and retry
		if attempt < c.cfg.MaxRetries {
			telemetry.NotificationRetriesTotal.Inc()
			backoff := time.Duration(float64(c.cfg.InitialBackoff) * math.Pow(c.cfg.BackoffMultiplier, float64(attempt)))
			c.logger.Warn("transient webhook failure, scheduling exponential retry",
				"attempt", attempts,
				"max_retries", c.cfg.MaxRetries,
				"backoff", backoff.String(),
				"error", err,
			)
			c.cfg.SleepFn(backoff)
		}
	}

	// All retries exhausted or non-retryable error: Route to Dead Letter Queue (DLQ)
	telemetry.NotificationDLQTotal.Inc()
	c.logger.Error("🚨 ALL RETRIES EXHAUSTED: routing poison message to DLQ",
		"topic", msg.Topic,
		"key", string(msg.Key),
		"attempts", attempts,
		"dlq_topic", c.cfg.DLQTopic,
		"final_error", lastErr,
	)

	dlqMsg := domain.DLQMessage{
		OriginalTopic: msg.Topic,
		OriginalKey:   string(msg.Key),
		Payload:       string(msg.Value),
		ErrorReason:   lastErr.Error(),
		RetryCount:    attempts,
		FailedAt:      time.Now().Format(time.RFC3339),
	}

	dlqBytes, err := json.Marshal(dlqMsg)
	if err != nil {
		c.logger.Error("failed to serialize DLQ message", "error", err)
		return nil
	}

	if c.publisher != nil {
		pubErr := c.publisher.Publish(ctx, c.cfg.DLQTopic, string(msg.Key), dlqBytes)
		if pubErr != nil {
			c.logger.Error("failed to dispatch message to Kafka DLQ", "error", pubErr)
			return fmt.Errorf("failed to publish to DLQ: %w", pubErr)
		}
		c.logger.Info("message archived to Dead Letter Queue successfully",
			"dlq_topic", c.cfg.DLQTopic,
			"key", string(msg.Key),
		)
	}

	return nil
}

// Start begins consuming Kafka messages until context cancellation
func (c *NotificationConsumer) Start(ctx context.Context) {
	c.logger.Info("notification worker started, consuming from Kafka topic...",
		"topic", c.reader.Config().Topic,
		"dlq_topic", c.cfg.DLQTopic,
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("notification consumer stopped cleanly")
				return
			}
			c.logger.Error("error fetching kafka message in notification worker", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := c.ProcessMessage(ctx, msg); err != nil {
			c.logger.Error("error processing notification message", "error", err)
		}

		// Always commit message offset after delivery OR DLQ routing to prevent partition blocking
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit kafka offset in notification worker", "error", err)
		}
	}
}
