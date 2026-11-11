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

func TestVelocityEvaluator_NormalTransactions_Allow(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-TEST-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	// Normal config with relaxed burst for test loop
	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           10, // set higher for rapid loop testing
		CumulativeAmountThreshold: 100000000.0,
		HighAmountThreshold:       50000000.0,
	})

	// Run 8 rapid transactions (below sustained threshold 10)
	for i := 1; i <= 8; i++ {
		evt := &domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        10000.0,
		}
		res, err := eval.Evaluate(ctx, evt)
		if err != nil {
			t.Fatalf("unexpected error on tx %d: %v", i, err)
		}
		if res.Action != domain.ActionAllow {
			t.Fatalf("expected tx %d action ALLOW, got %s (reason: %s)", i, res.Action, res.Reason)
		}
		if res.IsFraud {
			t.Fatalf("expected tx %d to be non-fraud, but got flagged: %s", i, res.Reason)
		}
		if res.VelocityCount != int64(i) {
			t.Errorf("expected count=%d, got %d", i, res.VelocityCount)
		}
	}
}

func TestVelocityEvaluator_AntiBotBurstDetected_Freeze(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-BOT-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           3, // Max 3 rapid transactions in 10s
		CumulativeAmountThreshold: 100000000.0,
		HighAmountThreshold:       50000000.0,
	})

	// First 3 rapid transactions pass with ALLOW
	for i := 1; i <= 3; i++ {
		evt := &domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        50000.0,
		}
		res, err := eval.Evaluate(ctx, evt)
		if err != nil {
			t.Fatalf("error on tx %d: %v", i, err)
		}
		if res.Action != domain.ActionAllow {
			t.Fatalf("expected tx %d to pass with ALLOW, got: %s", i, res.Action)
		}
	}

	// 4th rapid transaction within 10 seconds triggers anti-bot burst FREEZE
	fourthTx := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        50000.0,
	}
	res, err := eval.Evaluate(ctx, fourthTx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Action != domain.ActionFreeze {
		t.Fatalf("expected 4th rapid transaction to trigger FREEZE, got %s", res.Action)
	}
	if !res.IsFraud {
		t.Fatalf("expected is_fraud to be true for freeze action")
	}
	if res.RiskScore < 90 {
		t.Errorf("expected risk score >= 90 for bot attack, got %d", res.RiskScore)
	}
	if res.BurstCount != 4 {
		t.Errorf("expected burst count 4, got %d", res.BurstCount)
	}
}

func TestVelocityEvaluator_CumulativeAmountExceeded_Hold(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-SMURF-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           10,          // relaxed burst to test amount
		CumulativeAmountThreshold: 50000000.0,  // 50 Million cumulative cap
		HighAmountThreshold:       100000000.0, // High single threshold
	})

	// Tx 1: 20 Million (Clean -> ALLOW)
	tx1 := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        20000000.0,
	}
	res1, _ := eval.Evaluate(ctx, tx1)
	if res1.Action != domain.ActionAllow {
		t.Fatalf("expected tx1 to ALLOW, got: %s", res1.Action)
	}

	// Tx 2: 20 Million (Clean, cumulative = 40M <= 50M -> ALLOW)
	tx2 := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        20000000.0,
	}
	res2, _ := eval.Evaluate(ctx, tx2)
	if res2.Action != domain.ActionAllow {
		t.Fatalf("expected tx2 to ALLOW, got: %s", res2.Action)
	}

	// Tx 3: 20 Million (Cumulative = 60M > 50M -> Action HOLD, NOT freeze!)
	tx3 := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        20000000.0,
	}
	res3, _ := eval.Evaluate(ctx, tx3)
	if res3.Action != domain.ActionHold {
		t.Fatalf("expected tx3 to trigger HOLD, got %s", res3.Action)
	}
	if res3.IsFraud {
		t.Fatalf("expected is_fraud to be false for HOLD (we hold for review, do not blunt-freeze!)")
	}
}

func TestVelocityEvaluator_SustainedVelocityAttack_Freeze(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-SUSTAINED-" + uuid.New().String()
	defer rdb.Del(ctx, fmt.Sprintf("fraud:velocity:%s", accNum))

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        5,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           10, // burst relaxed to isolate sustained check
		CumulativeAmountThreshold: 100000000.0,
		HighAmountThreshold:       50000000.0,
	})

	// 5 transactions allowed
	for i := 1; i <= 5; i++ {
		evt := &domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        5000.0,
		}
		_, err := eval.Evaluate(ctx, evt)
		if err != nil {
			t.Fatalf("error on tx %d: %v", i, err)
		}
	}

	// 6th transaction triggers sustained velocity breach (> 5 transactions in 60s -> FREEZE)
	sixthTx := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        5000.0,
	}
	res, err := eval.Evaluate(ctx, sixthTx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Action != domain.ActionFreeze {
		t.Fatalf("expected 6th transaction to trigger FREEZE, got %s", res.Action)
	}
	if !res.IsFraud {
		t.Fatalf("expected is_fraud to be true for sustained breach freeze!")
	}
	if res.VelocityCount != 6 {
		t.Errorf("expected velocity count 6, got %d", res.VelocityCount)
	}
}

func TestVelocityEvaluator_HighAmountThreshold_Challenge(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	accNum := "ACC-CAR-BUYER-" + uuid.New().String()

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           5,
		CumulativeAmountThreshold: 200000000.0, // High cumulative
		HighAmountThreshold:       50000000.0,  // 50 Million single threshold
	})

	evt := &domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        85000000.0, // Buying car: Rp 85 Juta (Exceeds 50M threshold)
	}

	res, err := eval.Evaluate(ctx, evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should trigger CHALLENGE (Step-Up Auth), NOT freeze!
	if res.Action != domain.ActionChallenge {
		t.Fatalf("expected car purchase to trigger CHALLENGE, got %s", res.Action)
	}
	if res.IsFraud {
		t.Fatalf("expected is_fraud to be false for car purchase (should ask biometric/OTP, NOT freeze!)")
	}
	if res.RiskScore != 50 {
		t.Errorf("expected risk score 50, got %d", res.RiskScore)
	}
}
