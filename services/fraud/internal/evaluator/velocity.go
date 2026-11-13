package evaluator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/domain"
)

// VelocityConfig defines multi-tier fraud & velocity thresholds
type VelocityConfig struct {
	WindowDuration            time.Duration // Sustained velocity window (default: 60s)
	MaxAllowedInWindow        int64         // Sustained velocity limit (default: 10 tx)
	BurstWindowDuration       time.Duration // Anti-bot ultra-short window (default: 10s)
	MaxBurstAllowed           int64         // Anti-bot limit (default: 3 tx in 10s)
	CumulativeAmountThreshold float64       // Volume drain limit in window (default: Rp 100.000.000)
	HighAmountThreshold       float64       // Single transaction threshold (default: Rp 50.000.000)
}

// DefaultConfig provides industry best-practice defaults for financial transactions
func DefaultConfig() VelocityConfig {
	return VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,          // Tolerant for normal human/merchant behavior
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           3,           // Anti-bot limit: > 3 tx in 10s is physically impossible for humans
		CumulativeAmountThreshold: 100000000.0, // Rp 100.000.000 cumulative velocity threshold (triggers hold/review)
		HighAmountThreshold:       50000000.0,  // Rp 50.000.000 single transaction threshold (triggers step-up challenge)
	}
}

// ConfigFromEnv loads configuration from environment variables with safe defaults
func ConfigFromEnv() VelocityConfig {
	cfg := DefaultConfig()

	if val := os.Getenv("FRAUD_WINDOW_DURATION_SEC"); val != "" {
		if sec, err := strconv.Atoi(val); err == nil && sec > 0 {
			cfg.WindowDuration = time.Duration(sec) * time.Second
		}
	}
	if val := os.Getenv("FRAUD_MAX_IN_WINDOW"); val != "" {
		if count, err := strconv.ParseInt(val, 10, 64); err == nil && count > 0 {
			cfg.MaxAllowedInWindow = count
		}
	}
	if val := os.Getenv("FRAUD_BURST_WINDOW_SEC"); val != "" {
		if sec, err := strconv.Atoi(val); err == nil && sec > 0 {
			cfg.BurstWindowDuration = time.Duration(sec) * time.Second
		}
	}
	if val := os.Getenv("FRAUD_MAX_BURST"); val != "" {
		if count, err := strconv.ParseInt(val, 10, 64); err == nil && count > 0 {
			cfg.MaxBurstAllowed = count
		}
	}
	if val := os.Getenv("FRAUD_CUMULATIVE_AMOUNT_THRESHOLD"); val != "" {
		if amt, err := strconv.ParseFloat(val, 64); err == nil && amt > 0 {
			cfg.CumulativeAmountThreshold = amt
		}
	}
	if val := os.Getenv("FRAUD_HIGH_AMOUNT_THRESHOLD"); val != "" {
		if amt, err := strconv.ParseFloat(val, 64); err == nil && amt > 0 {
			cfg.HighAmountThreshold = amt
		}
	}

	return cfg
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

// Evaluate performs sub-5ms multi-tier sliding-window fraud checks using Redis Sorted Sets (ZSET)
func (e *VelocityEvaluator) Evaluate(ctx context.Context, evt *domain.TransactionCreatedEvent) (*domain.FraudCheckResult, error) {
	key := fmt.Sprintf("fraud:velocity:%s", evt.SourceAccount)

	// Stream Correctness: Prioritize Event Time over Processing Time to prevent false-positive
	// bot flags when consumer lag occurs or when pods restart.
	var eventMillis int64
	if evt.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, evt.Timestamp); err == nil {
			eventMillis = t.UnixMilli()
		}
	}
	if eventMillis <= 0 {
		eventMillis = time.Now().UnixMilli()
	}
	now := eventMillis

	windowStart := now - e.cfg.WindowDuration.Milliseconds()
	burstStart := now - e.cfg.BurstWindowDuration.Milliseconds()

	// Atomic pipeline execution in Redis
	pipe := e.rdb.TxPipeline()

	// 1. Evict entries outside the sliding window
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", windowStart))

	// 2. Add current transaction with timestamp as score
	member := fmt.Sprintf("%s:%.4f:%d", evt.TransactionID, evt.Amount, now)
	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: member,
	})

	// 3. Count total elements in sustained window
	cardCmd := pipe.ZCard(ctx, key)

	// 4. Count elements in burst window (anti-bot check)
	burstCmd := pipe.ZCount(ctx, key, fmt.Sprintf("%d", burstStart), "+inf")

	// 5. Fetch all members in active window to compute cumulative volume
	rangeCmd := pipe.ZRange(ctx, key, 0, -1)

	// 6. Set TTL to keep Redis memory bounded
	pipe.Expire(ctx, key, e.cfg.WindowDuration*2)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis velocity pipeline error: %w", err)
	}

	velocityCount := cardCmd.Val()
	burstCount := burstCmd.Val()

	var cumulativeAmount float64
	for _, item := range rangeCmd.Val() {
		parts := strings.Split(item, ":")
		if len(parts) >= 2 {
			if amt, err := strconv.ParseFloat(parts[1], 64); err == nil {
				cumulativeAmount += amt
			}
		}
	}

	// -------------------------------------------------------------------------
	// ADVANCED ACTION MATRIX & RISK SCORING (FAANG / Banking Standard)
	// -------------------------------------------------------------------------

	// TIER 1: Anti-Bot Burst Attack (Unmistakable bot violation of human physical limits)
	// Action: FREEZE account immediately via gRPC call!
	if e.cfg.MaxBurstAllowed > 0 && burstCount > e.cfg.MaxBurstAllowed {
		return &domain.FraudCheckResult{
			Action:           domain.ActionFreeze,
			RiskScore:        95,
			IsFraud:          true,
			Reason:           fmt.Sprintf("Anti-bot burst limit exceeded: %d transactions in %s (max allowed: %d)", burstCount, e.cfg.BurstWindowDuration, e.cfg.MaxBurstAllowed),
			VelocityCount:    velocityCount,
			BurstCount:       burstCount,
			CumulativeAmount: cumulativeAmount,
			AccountNumber:    evt.SourceAccount,
		}, nil
	}

	// TIER 2: Sustained Extreme Velocity Attack (> MaxAllowedInWindow within 60s)
	// Action: FREEZE account immediately via gRPC call!
	if e.cfg.MaxAllowedInWindow > 0 && velocityCount > e.cfg.MaxAllowedInWindow {
		return &domain.FraudCheckResult{
			Action:           domain.ActionFreeze,
			RiskScore:        85,
			IsFraud:          true,
			Reason:           fmt.Sprintf("Sustained velocity limit exceeded: %d transactions in %s (max allowed: %d)", velocityCount, e.cfg.WindowDuration, e.cfg.MaxAllowedInWindow),
			VelocityCount:    velocityCount,
			BurstCount:       burstCount,
			CumulativeAmount: cumulativeAmount,
			AccountNumber:    evt.SourceAccount,
		}, nil
	}

	// TIER 3: Cumulative Amount Velocity Exceeded (Structuring / Multiple split payments)
	// Action: HOLD transaction temporarily for review / user confirmation. Do NOT freeze account!
	if e.cfg.CumulativeAmountThreshold > 0 && cumulativeAmount > e.cfg.CumulativeAmountThreshold {
		return &domain.FraudCheckResult{
			Action:           domain.ActionHold,
			RiskScore:        65,
			IsFraud:          false,
			Reason:           fmt.Sprintf("Cumulative velocity volume exceeded: %.2f in %s (threshold: %.2f) - placed on temporary review hold", cumulativeAmount, e.cfg.WindowDuration, e.cfg.CumulativeAmountThreshold),
			VelocityCount:    velocityCount,
			BurstCount:       burstCount,
			CumulativeAmount: cumulativeAmount,
			AccountNumber:    evt.SourceAccount,
		}, nil
	}

	// TIER 4: Single High-Amount Transaction (Whale transfer / Vehicle / Property purchase)
	// Action: CHALLENGE user with Step-Up Auth (Biometrics / OTP SMS). Do NOT freeze account!
	if e.cfg.HighAmountThreshold > 0 && evt.Amount >= e.cfg.HighAmountThreshold {
		return &domain.FraudCheckResult{
			Action:           domain.ActionChallenge,
			RiskScore:        50,
			IsFraud:          false,
			Reason:           fmt.Sprintf("Single transaction amount (%.2f) exceeds high threshold (%.2f) - step-up 2FA/biometric verification required", evt.Amount, e.cfg.HighAmountThreshold),
			VelocityCount:    velocityCount,
			BurstCount:       burstCount,
			CumulativeAmount: cumulativeAmount,
			AccountNumber:    evt.SourceAccount,
		}, nil
	}

	// TIER 5: Clean Legitimate Transaction
	// Action: ALLOW directly without any friction
	return &domain.FraudCheckResult{
		Action:           domain.ActionAllow,
		RiskScore:        10,
		IsFraud:          false,
		VelocityCount:    velocityCount,
		BurstCount:       burstCount,
		CumulativeAmount: cumulativeAmount,
		AccountNumber:    evt.SourceAccount,
	}, nil
}
