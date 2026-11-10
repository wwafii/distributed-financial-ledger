package idempotency_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
)

const testRedisAddr = "localhost:6380"

func setupTestRedis(t *testing.T) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping Redis test; Redis not reachable on %s: %v", testRedisAddr, err)
	}
	return rdb
}

func TestIdempotencyEngine_Lifecycle(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	engine := idempotency.NewEngine(rdb)

	key := "test-idemp-" + uuid.New().String()

	// 1. First attempt: should acquire lock
	result1, err := engine.AcquireOrGet(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	if !result1.Acquired {
		t.Fatalf("expected lock to be acquired on first attempt")
	}

	// 2. Second attempt while in-flight: should NOT acquire lock and return PROCESSING
	result2, err := engine.AcquireOrGet(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	if result2.Acquired {
		t.Fatalf("expected lock NOT to be acquired while in-flight")
	}
	if result2.Record.Status != idempotency.StatusProcessing {
		t.Fatalf("expected status PROCESSING, got %s", result2.Record.Status)
	}

	// 3. Complete transaction: save response body and status code
	sampleBody := []byte(`{"success":true,"transaction_id":"tx-999"}`)
	err = engine.SaveResponse(ctx, key, 201, sampleBody, 10*time.Second)
	if err != nil {
		t.Fatalf("save response failed: %v", err)
	}

	// 4. Third attempt after completion: should return COMPLETED with cached response body
	result3, err := engine.AcquireOrGet(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("third acquire failed: %v", err)
	}
	if result3.Acquired {
		t.Fatalf("expected lock NOT to be acquired on completed key")
	}
	if result3.Record.Status != idempotency.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", result3.Record.Status)
	}
	if result3.Record.StatusCode != 201 {
		t.Fatalf("expected cached status code 201, got %d", result3.Record.StatusCode)
	}
	if string(result3.Record.Body) != string(sampleBody) {
		t.Fatalf("expected body %s, got %s", sampleBody, result3.Record.Body)
	}

	// 5. Cleanup
	_ = engine.Release(ctx, key)
}
