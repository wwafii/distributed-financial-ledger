package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/sender"
	otelcodes "go.opentelemetry.io/otel/codes"
)

// DefaultEqualJitter applies AWS Equal Jitter to eliminate thundering herd:
// sleep = (backoff / 2) + rand(0, backoff / 2)
// This guarantees sleep is within [0.5 * backoff, 1.0 * backoff],
// preventing traffic stampedes while maintaining monotonic progression across retry attempts.
func DefaultEqualJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	jitter := time.Duration(rand.Int64N(int64(half)))
	return half + jitter
}

// NotificationConfig holds backoff, routing, and concurrency configuration for notification worker
type NotificationConfig struct {
	MaxRetries        int
	InitialBackoff    time.Duration
	BackoffMultiplier float64
	WebhookURL        string
	DLQTopic          string
	SleepFn           func(d time.Duration)               // for testability without real delay
	JitterFn          func(d time.Duration) time.Duration // full/equal jitter function
	WorkerPoolSize    int                                 // concurrent worker pool size
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
		JitterFn:          DefaultEqualJitter,
		WorkerPoolSize:    5,
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
	if val := os.Getenv("NOTIFICATION_CONCURRENCY"); val != "" {
		if c, err := strconv.Atoi(val); err == nil && c > 0 {
			cfg.WorkerPoolSize = c
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
	if cfg.JitterFn == nil {
		cfg.JitterFn = DefaultEqualJitter
	}
	if cfg.WorkerPoolSize <= 0 {
		cfg.WorkerPoolSize = 5
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

		// If transient failure and we have remaining attempts, backoff with jitter and retry
		if attempt < c.cfg.MaxRetries {
			telemetry.NotificationRetriesTotal.Inc()
			baseBackoff := time.Duration(float64(c.cfg.InitialBackoff) * math.Pow(c.cfg.BackoffMultiplier, float64(attempt)))
			backoff := baseBackoff
			if c.cfg.JitterFn != nil {
				backoff = c.cfg.JitterFn(baseBackoff)
			}

			c.logger.Warn("transient webhook failure, scheduling exponential retry with jitter",
				"attempt", attempts,
				"max_retries", c.cfg.MaxRetries,
				"base_backoff", baseBackoff.String(),
				"jittered_backoff", backoff.String(),
				"error", err,
			)
			c.cfg.SleepFn(backoff)
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
			span.RecordError(pubErr)
			span.SetStatus(otelcodes.Error, pubErr.Error())
			c.logger.Error("failed to dispatch message to Kafka DLQ", "error", pubErr)
			return fmt.Errorf("failed to publish to DLQ: %w", pubErr)
		}
		c.logger.Info("message archived to Dead Letter Queue successfully",
			"dlq_topic", c.cfg.DLQTopic,
			"key", string(msg.Key),
		)
	}

	span.SetStatus(otelcodes.Ok, "routed_to_dlq")
	return nil
}

// Start begins consuming Kafka messages with a concurrent worker pool until context cancellation
func (c *NotificationConsumer) Start(ctx context.Context) {
	c.logger.Info("notification worker pool started, consuming from Kafka topic...",
		"topic", c.reader.Config().Topic,
		"dlq_topic", c.cfg.DLQTopic,
		"concurrency", c.cfg.WorkerPoolSize,
	)

	sem := make(chan struct{}, c.cfg.WorkerPoolSize)
	var wg sync.WaitGroup

WorkerLoop:
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("notification consumer stopped cleanly, draining workers...")
				break WorkerLoop
			}
			c.logger.Error("error fetching kafka message in notification worker", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break WorkerLoop
		}

		wg.Add(1)
		go func(m kafka.Message) {
			defer func() {
				<-sem
				wg.Done()
			}()

			if err := c.ProcessMessage(ctx, m); err != nil {
				// Zero Data Loss: If DLQ routing fails, DO NOT commit the offset so Kafka redelivers!
				c.logger.Error("error processing notification message, skipping commit to prevent silent data loss", "error", err)
				return
			}

			// Commit offset only after successful delivery or successful DLQ routing
			if err := c.reader.CommitMessages(ctx, m); err != nil {
				c.logger.Error("failed to commit kafka offset in notification worker", "error", err)
			}
		}(msg)
	}

	wg.Wait()
	c.logger.Info("notification worker pool fully drained")
}
