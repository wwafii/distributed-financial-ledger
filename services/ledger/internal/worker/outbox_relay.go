package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
)

// OutboxRelayConfig holds configuration for polling relay
type OutboxRelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	MaxRetries   int
}

func DefaultConfig() OutboxRelayConfig {
	return OutboxRelayConfig{
		BatchSize:    50,
		PollInterval: 500 * time.Millisecond,
		MaxRetries:   5,
	}
}

// OutboxRelay polls pending outbox events using SKIP LOCKED and publishes to Kafka
type OutboxRelay struct {
	pool      *pgxpool.Pool
	publisher pkgkafka.EventPublisher
	logger    *slog.Logger
	cfg       OutboxRelayConfig
}

func NewOutboxRelay(
	pool *pgxpool.Pool,
	publisher pkgkafka.EventPublisher,
	logger *slog.Logger,
	cfg OutboxRelayConfig,
) *OutboxRelay {
	return &OutboxRelay{
		pool:      pool,
		publisher: publisher,
		logger:    logger,
		cfg:       cfg,
	}
}

type outboxItem struct {
	ID          uuid.UUID
	EventType   string
	AggregateID string
	Payload     []byte
	RetryCount  int
}

// ProcessBatch claims up to BatchSize pending events using SELECT ... FOR UPDATE SKIP LOCKED
func (r *OutboxRelay) ProcessBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("failed to begin outbox transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch and lock unclaimed pending records (concurrency-safe across multiple instances)
	query := `
		SELECT id, event_type, aggregate_id, payload, retry_count
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, query, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to query outbox events: %w", err)
	}
	defer rows.Close()

	var items []outboxItem
	for rows.Next() {
		var item outboxItem
		if err := rows.Scan(&item.ID, &item.EventType, &item.AggregateID, &item.Payload, &item.RetryCount); err != nil {
			return 0, fmt.Errorf("failed to scan outbox item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}
	rows.Close() // Close rows explicitly before executing updates in same tx

	if len(items) == 0 {
		return 0, nil
	}

	for _, item := range items {
		// Extract incoming W3C trace context from outbox event payload if available
		publishCtx := ctx
		var payloadMap map[string]interface{}
		if err := json.Unmarshal(item.Payload, &payloadMap); err == nil {
			if traceCtxRaw, ok := payloadMap["trace_context"].(map[string]interface{}); ok {
				traceMap := make(map[string]string, len(traceCtxRaw))
				for k, v := range traceCtxRaw {
					if strVal, isStr := v.(string); isStr {
						traceMap[k] = strVal
					}
				}
				publishCtx = telemetry.ExtractMap(ctx, traceMap)
			}
		}

		tracer := telemetry.Tracer("outbox-relay")
		publishCtx, span := tracer.Start(publishCtx, "Outbox Relay Publish: "+item.EventType)

		// Publish event to Kafka
		publishErr := r.publisher.Publish(publishCtx, item.EventType, item.AggregateID, item.Payload)
		span.End()

		if publishErr != nil {
			r.logger.Error("failed to publish outbox event",
				"event_id", item.ID,
				"event_type", item.EventType,
				"retry_count", item.RetryCount,
				"error", publishErr,
			)

			newRetry := item.RetryCount + 1
			newStatus := "PENDING"
			if newRetry >= r.cfg.MaxRetries {
				newStatus = "FAILED"
				r.logger.Warn("outbox event marked as FAILED after max retries", "event_id", item.ID)
			}

			_, _ = tx.Exec(ctx, `
				UPDATE outbox_events 
				SET status = $1, retry_count = $2 
				WHERE id = $3
			`, newStatus, newRetry, item.ID)
			continue
		}

		// Mark as PUBLISHED with published_at timestamp
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events 
			SET status = 'PUBLISHED', published_at = NOW() 
			WHERE id = $1
		`, item.ID)
		if err != nil {
			return 0, fmt.Errorf("failed to mark event as published: %w", err)
		}

		r.logger.Info("outbox event dispatched successfully",
			"event_id", item.ID,
			"event_type", item.EventType,
			"aggregate_id", item.AggregateID,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit outbox batch update: %w", err)
	}

	return len(items), nil
}

// Start runs the background polling loop until context cancellation
func (r *OutboxRelay) Start(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.logger.Info("starting outbox relay worker loop",
		"poll_interval", r.cfg.PollInterval,
		"batch_size", r.cfg.BatchSize,
	)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("stopping outbox relay worker gracefully...")
			return
		case <-ticker.C:
			count, err := r.ProcessBatch(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				r.logger.Error("outbox relay batch processing error", "error", err)
			}
			if count > 0 {
				r.logger.Debug("processed outbox batch", "count", count)
			}
		}
	}
}
