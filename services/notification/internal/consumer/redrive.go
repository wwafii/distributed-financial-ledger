package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/domain"
)

// DLQRedriverConfig holds settings for replaying poisoned messages back to the active stream
type DLQRedriverConfig struct {
	DLQTopic       string
	TargetOverride string // optional override; if empty, uses OriginalTopic from DLQ metadata
	PollTimeout    time.Duration
}

// DLQReader abstracts kafka.Reader for deterministic unit testing
type DLQReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// DLQRedriver reads messages from Dead Letter Queue and safely re-publishes them to active topics
type DLQRedriver struct {
	reader    DLQReader
	publisher pkgkafka.EventPublisher
	logger    *slog.Logger
	cfg       DLQRedriverConfig
}

func NewDLQRedriver(
	reader DLQReader,
	publisher pkgkafka.EventPublisher,
	logger *slog.Logger,
	cfg DLQRedriverConfig,
) *DLQRedriver {
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 3 * time.Second
	}
	return &DLQRedriver{
		reader:    reader,
		publisher: publisher,
		logger:    logger,
		cfg:       cfg,
	}
}

// RedriveBatch consumes up to maxMessages from DLQ and re-publishes them to the designated topic
func (r *DLQRedriver) RedriveBatch(ctx context.Context, maxMessages int) (int, error) {
	if maxMessages <= 0 {
		maxMessages = 100
	}

	redrivenCount := 0

	for redrivenCount < maxMessages {
		fetchCtx, cancel := context.WithTimeout(ctx, r.cfg.PollTimeout)
		msg, err := r.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				// No more messages available in DLQ within poll timeout
				break
			}
			return redrivenCount, fmt.Errorf("failed to fetch message from DLQ: %w", err)
		}

		// Parse DLQ envelope
		var dlqEnvelope domain.DLQMessage
		if err := json.Unmarshal(msg.Value, &dlqEnvelope); err != nil {
			r.logger.Error("malformed DLQ message detected, committing without replay",
				"key", string(msg.Key),
				"error", err,
			)
			_ = r.reader.CommitMessages(ctx, msg)
			continue
		}

		targetTopic := dlqEnvelope.OriginalTopic
		if r.cfg.TargetOverride != "" {
			targetTopic = r.cfg.TargetOverride
		}

		// Republish original payload back to target topic
		err = r.publisher.Publish(ctx, targetTopic, dlqEnvelope.OriginalKey, []byte(dlqEnvelope.Payload))
		if err != nil {
			return redrivenCount, fmt.Errorf("failed to re-publish DLQ message %s to %s: %w", dlqEnvelope.OriginalKey, targetTopic, err)
		}

		// Commit offset in DLQ topic only AFTER successful publication
		if err := r.reader.CommitMessages(ctx, msg); err != nil {
			return redrivenCount, fmt.Errorf("failed to commit DLQ message offset: %w", err)
		}

		redrivenCount++
		r.logger.Info("DLQ message re-driven successfully",
			"key", dlqEnvelope.OriginalKey,
			"target_topic", targetTopic,
			"original_error", dlqEnvelope.ErrorReason,
			"replayed_at", time.Now().Format(time.RFC3339),
		)
	}

	return redrivenCount, nil
}
