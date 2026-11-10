package evaluator

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/domain"
)

type VelocityConfig struct {
	WindowDuration      time.Duration
	MaxAllowedInWindow  int64
	HighAmountThreshold float64
}

func DefaultConfig() VelocityConfig {
	return VelocityConfig{
		WindowDuration:      60 * time.Second,
		MaxAllowedInWindow:  5,
		HighAmountThreshold: 50000000.0, // Rp 50.000.000 single transaction threshold
	}
}

type VelocityEvaluator struct {
	rdb *redis.Client
	cfg VelocityConfig
}

func NewVelocityEvaluator(rdb *redis.Client, cfg VelocityConfig) *VelocityEvaluator {
	return &VelocityEvaluator{
		rdb: rdb,
		cfg: cfg,
	}
}

// Evaluate performs sub-5ms sliding-window velocity check using Redis Sorted Sets (ZSET)
func (e *VelocityEvaluator) Evaluate(ctx context.Context, evt *domain.TransactionCreatedEvent) (*domain.FraudCheckResult, error) {
	// Rule 1: High Single-Transaction Threshold
	if evt.Amount >= e.cfg.HighAmountThreshold {
		return &domain.FraudCheckResult{
			IsFraud:       true,
			Reason:        fmt.Sprintf("Single transaction amount (%.2f) exceeds threshold (%.2f)", evt.Amount, e.cfg.HighAmountThreshold),
			AccountNumber: evt.SourceAccount,
		}, nil
	}

	key := fmt.Sprintf("fraud:velocity:%s", evt.SourceAccount)
	now := time.Now().UnixMilli()
	windowStart := now - e.cfg.WindowDuration.Milliseconds()

	// Atomic pipeline execution in Redis
	pipe := e.rdb.TxPipeline()

	// 1. Evict entries outside the sliding window
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", windowStart))

	// 2. Add current transaction with timestamp as score
	member := fmt.Sprintf("%s:%f:%d", evt.TransactionID, evt.Amount, now)
	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: member,
	})

	// 3. Count remaining elements in window
	cardCmd := pipe.ZCard(ctx, key)

	// 4. Set TTL to keep Redis memory bounded
	pipe.Expire(ctx, key, e.cfg.WindowDuration*2)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis velocity pipeline error: %w", err)
	}

	velocityCount := cardCmd.Val()

	// Rule 2: Velocity Limit Check (> maxAllowed transactions in window)
	if velocityCount > e.cfg.MaxAllowedInWindow {
		return &domain.FraudCheckResult{
			IsFraud:       true,
			Reason:        fmt.Sprintf("Velocity limit exceeded: %d transactions in %s (max allowed: %d)", velocityCount, e.cfg.WindowDuration, e.cfg.MaxAllowedInWindow),
			VelocityCount: velocityCount,
			AccountNumber: evt.SourceAccount,
		}, nil
	}

	return &domain.FraudCheckResult{
		IsFraud:       false,
		VelocityCount: velocityCount,
		AccountNumber: evt.SourceAccount,
	}, nil
}
