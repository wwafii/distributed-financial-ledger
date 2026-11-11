package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/evaluator"
)

type FraudConsumer struct {
	reader       *kafka.Reader
	evaluator    *evaluator.VelocityEvaluator
	ledgerClient ledgerv1.LedgerServiceClient
	publisher    pkgkafka.EventPublisher
	logger       *slog.Logger
}

func NewFraudConsumer(
	reader *kafka.Reader,
	eval *evaluator.VelocityEvaluator,
	ledgerClient ledgerv1.LedgerServiceClient,
	publisher pkgkafka.EventPublisher,
	logger *slog.Logger,
) *FraudConsumer {
	return &FraudConsumer{
		reader:       reader,
		evaluator:    eval,
		ledgerClient: ledgerClient,
		publisher:    publisher,
		logger:       logger,
	}
}

// ProcessMessage processes a single transaction event from Kafka through the multi-tier action matrix
func (c *FraudConsumer) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	var evt domain.TransactionCreatedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		c.logger.Error("failed to deserialize transaction created event", "error", err, "payload", string(msg.Value))
		return nil // skip poison pill to avoid infinite loop
	}

	result, err := c.evaluator.Evaluate(ctx, &evt)
	if err != nil {
		c.logger.Error("fraud evaluation error", "error", err, "account", evt.SourceAccount)
		return fmt.Errorf("evaluator error: %w", err)
	}

	switch result.Action {
	case domain.ActionFreeze:
		// TIER 1 & 2: EMERGENCY BOT / VELOCITY ATTACK
		c.logger.Warn("🚨 EMERGENCY: BOT/VELOCITY FRAUD DETECTED! Freezing account immediately...",
			"account", result.AccountNumber,
			"action", result.Action,
			"risk_score", result.RiskScore,
			"reason", result.Reason,
			"velocity_count", result.VelocityCount,
			"burst_count", result.BurstCount,
			"transaction_id", evt.TransactionID,
		)

		// 1. Send gRPC call to Core Ledger to Freeze Account immediately
		freezeResp, freezeErr := c.ledgerClient.FreezeAccount(ctx, &ledgerv1.FreezeAccountRequest{
			AccountNumber: result.AccountNumber,
			Reason:        result.Reason,
		})
		if freezeErr != nil {
			c.logger.Error("failed to freeze account via gRPC", "account", result.AccountNumber, "error", freezeErr)
		} else {
			c.logger.Info("account frozen successfully via gRPC",
				"account", freezeResp.AccountNumber,
				"status", freezeResp.Status,
			)
		}

		// 2. Publish fraud.flagged event to Kafka for downstream security / audit
		if c.publisher != nil {
			flaggedEvt := domain.FraudFlaggedEvent{
				AccountNumber: result.AccountNumber,
				Action:        result.Action,
				RiskScore:     result.RiskScore,
				Reason:        result.Reason,
				FlaggedAt:     time.Now().Format(time.RFC3339),
				TransactionID: evt.TransactionID,
			}
			payload, _ := json.Marshal(flaggedEvt)
			_ = c.publisher.Publish(ctx, "fraud.flagged", result.AccountNumber, payload)
		}

	case domain.ActionChallenge:
		// TIER 4: JUMBO / HIGH AMOUNT TRANSACTION (e.g. buying car, property)
		// Do NOT freeze account! Request Step-Up Biometric or OTP 2FA.
		c.logger.Info("⚠️ STEP-UP AUTH REQUIRED: High amount detected (e.g. vehicle/property purchase), requesting biometric/OTP verification without freezing",
			"account", result.AccountNumber,
			"action", result.Action,
			"risk_score", result.RiskScore,
			"amount", evt.Amount,
			"reason", result.Reason,
			"transaction_id", evt.TransactionID,
		)

		if c.publisher != nil {
			challengeEvt := domain.FraudChallengeEvent{
				AccountNumber: result.AccountNumber,
				TransactionID: evt.TransactionID,
				Amount:        evt.Amount,
				ChallengeType: "BIOMETRIC_OR_OTP",
				RiskScore:     result.RiskScore,
				Reason:        result.Reason,
				CreatedAt:     time.Now().Format(time.RFC3339),
			}
			payload, _ := json.Marshal(challengeEvt)
			_ = c.publisher.Publish(ctx, "fraud.challenge_required", result.AccountNumber, payload)
		}

	case domain.ActionHold:
		// TIER 3: RAPID CUMULATIVE VOLUME (e.g. structuring, installment payments)
		// Do NOT freeze account! Place on temporary review hold.
		c.logger.Warn("⏳ TRANSACTION REVIEW HOLD: Cumulative volume velocity limit reached, placing on temporary hold without freezing",
			"account", result.AccountNumber,
			"action", result.Action,
			"risk_score", result.RiskScore,
			"cumulative_amount", result.CumulativeAmount,
			"reason", result.Reason,
			"transaction_id", evt.TransactionID,
		)

		if c.publisher != nil {
			holdEvt := domain.FraudHoldEvent{
				AccountNumber:    result.AccountNumber,
				TransactionID:    evt.TransactionID,
				CumulativeAmount: result.CumulativeAmount,
				RiskScore:        result.RiskScore,
				Reason:           result.Reason,
				HoldDurationSec:  600, // 10 minutes temporary hold
				CreatedAt:        time.Now().Format(time.RFC3339),
			}
			payload, _ := json.Marshal(holdEvt)
			_ = c.publisher.Publish(ctx, "fraud.review_hold", result.AccountNumber, payload)
		}

	default: // domain.ActionAllow
		c.logger.Debug("transaction passed all fraud checks (ALLOW)",
			"account", evt.SourceAccount,
			"action", result.Action,
			"velocity_count", result.VelocityCount,
			"burst_count", result.BurstCount,
			"cumulative_amount", result.CumulativeAmount,
		)
	}

	return nil
}

// Start begins consuming from Kafka until context is cancelled
func (c *FraudConsumer) Start(ctx context.Context) {
	c.logger.Info("fraud engine consumer started, listening on topic 'transaction.created'...")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("fraud consumer stopped cleanly")
				return
			}
			c.logger.Error("error fetching kafka message", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := c.ProcessMessage(ctx, msg); err != nil {
			c.logger.Error("error processing message", "error", err)
		}

		// Commit offset
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit kafka offset", "error", err)
		}
	}
}
