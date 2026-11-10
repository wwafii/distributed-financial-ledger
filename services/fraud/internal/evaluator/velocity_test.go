package evaluator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/evaluator"
)

const testRedisAddr = "localhost:6380"

func setupTestRedis(t *testing.T) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test: Redis not reachable: %v", err)
	}
	return rdb
}

func TestVelocityEvaluator_NormalTransactions(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-TEST-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:      60 * time.Second,
		MaxAllowedInWindow:  5,
		HighAmountThreshold: 10000000.0,
	})

	// Run 5 rapid transactions (should all be clean / non-fraud)
	for i := 1; i <= 5; i++ {
		evt := &domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        10000.0,
		}
		res, err := eval.Evaluate(ctx, evt)
		if err != nil {
			t.Fatalf("unexpected error on tx %d: %v", i, err)
		}
		if res.IsFraud {
			t.Fatalf("expected tx %d to be non-fraud, but got flagged: %s", i, res.Reason)
		}
		if res.VelocityCount != int64(i) {
			t.Errorf("expected count=%d, got %d", i, res.VelocityCount)
		}
	}
}

func TestVelocityEvaluator_VelocityAttackDetected(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-ATTACK-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:      60 * time.Second,
		MaxAllowedInWindow:  5,
		HighAmountThreshold: 10000000.0,
	})

	// 5 transactions allowed
	for i := 1; i <= 5; i++ {
		evt := &domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        50000.0,
		}
		_, err := eval.Evaluate(ctx, evt)
		if err != nil {
			t.Fatalf("error on tx %d: %v", i, err)
		}
	}

	// 6th transaction triggers velocity breach (> 5 transactions in 60s)
	sixthTx := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        50000.0,
	}
	res, err := eval.Evaluate(ctx, sixthTx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !res.IsFraud {
		t.Fatalf("expected 6th transaction to be flagged as fraud!")
	}
	if res.VelocityCount != 6 {
		t.Errorf("expected velocity count 6, got %d", res.VelocityCount)
	}
}

func TestVelocityEvaluator_HighAmountThreshold(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-WHALE-" + uuid.New().String()

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:      60 * time.Second,
		MaxAllowedInWindow:  5,
		HighAmountThreshold: 50000000.0,
	})

	evt := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        99000000.0, // Exceeds 50M threshold
	}

	res, err := eval.Evaluate(ctx, evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !res.IsFraud {
		t.Fatalf("expected high amount transaction to be flagged as fraud!")
	}
}
