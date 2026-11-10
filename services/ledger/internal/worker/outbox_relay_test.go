package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/worker"
)

const testDBURL = "postgres://postgres:postgres@localhost:5433/ledger_db?sslmode=disable"

func setupTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDBURL)
	if err != nil {
		t.Skipf("Skipping test: PostgreSQL not available: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping test: PostgreSQL ping failed: %v", err)
	}
	return pool
}

type mockPublisher struct {
	mu        sync.Mutex
	published []string
	fail      bool
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("simulated broker failure")
	}
	m.published = append(m.published, key)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func TestOutboxRelay_ProcessBatch_Success(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	log := logger.New("test-relay", nil)

	// 1. Insert a PENDING outbox event
	eventID := uuid.New()
	aggregateID := "tx-" + uuid.New().String()
	payload := `{"test":"data"}`

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, status, created_at)
		VALUES ($1, 'transaction.created', 'TRANSACTION', $2, $3, 'PENDING', NOW())
	`, eventID, aggregateID, payload)
	if err != nil {
		t.Fatalf("failed to insert test outbox event: %v", err)
	}

	// Clean up after test
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM outbox_events WHERE id = $1", eventID)
	}()

	pub := &mockPublisher{}
	relay := worker.NewOutboxRelay(pool, pub, log, worker.OutboxRelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		MaxRetries:   3,
	})

	count, err := relay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected at least 1 event processed, got 0")
	}

	// 2. Verify status changed to PUBLISHED
	var status string
	var publishedAt *time.Time
	err = pool.QueryRow(ctx, "SELECT status, published_at FROM outbox_events WHERE id = $1", eventID).Scan(&status, &publishedAt)
	if err != nil {
		t.Fatalf("failed to query updated event: %v", err)
	}

	if status != "PUBLISHED" {
		t.Errorf("expected status 'PUBLISHED', got %s", status)
	}
	if publishedAt == nil {
		t.Errorf("expected published_at to be non-nil")
	}
}

func TestOutboxRelay_ProcessBatch_RetryOnFailure(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	log := logger.New("test-relay", nil)

	eventID := uuid.New()
	aggregateID := "tx-fail-" + uuid.New().String()
	payload := `{"test":"failure"}`

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, status, retry_count, created_at)
		VALUES ($1, 'transaction.created', 'TRANSACTION', $2, $3, 'PENDING', 0, NOW())
	`, eventID, aggregateID, payload)
	if err != nil {
		t.Fatalf("failed to insert test outbox event: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM outbox_events WHERE id = $1", eventID)
	}()

	pub := &mockPublisher{fail: true}
	relay := worker.NewOutboxRelay(pool, pub, log, worker.OutboxRelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		MaxRetries:   3,
	})

	count, err := relay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected at least 1 event processed")
	}

	var status string
	var retryCount int
	err = pool.QueryRow(ctx, "SELECT status, retry_count FROM outbox_events WHERE id = $1", eventID).Scan(&status, &retryCount)
	if err != nil {
		t.Fatalf("failed to query failed event: %v", err)
	}

	if status != "PENDING" {
		t.Errorf("expected status still 'PENDING', got %s", status)
	}
	if retryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", retryCount)
	}
}
