package consumer_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/consumer"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/evaluator"
	"google.golang.org/grpc"
)

type mockLedgerServiceClient struct {
	freezeCalls int64
	frozenAcc   string
}

func (m *mockLedgerServiceClient) CreateTransaction(ctx context.Context, in *ledgerv1.CreateTransactionRequest, opts ...grpc.CallOption) (*ledgerv1.CreateTransactionResponse, error) {
	return nil, nil
}

func (m *mockLedgerServiceClient) GetAccountBalance(ctx context.Context, in *ledgerv1.GetAccountBalanceRequest, opts ...grpc.CallOption) (*ledgerv1.GetAccountBalanceResponse, error) {
	return nil, nil
}

func (m *mockLedgerServiceClient) FreezeAccount(ctx context.Context, in *ledgerv1.FreezeAccountRequest, opts ...grpc.CallOption) (*ledgerv1.FreezeAccountResponse, error) {
	atomic.AddInt64(&m.freezeCalls, 1)
	m.frozenAcc = in.AccountNumber
	return &ledgerv1.FreezeAccountResponse{
		Success:       true,
		AccountNumber: in.AccountNumber,
		Status:        "FROZEN",
		Message:       "account successfully frozen",
	}, nil
}

const testRedisAddr = "localhost:6380"

func TestFraudConsumer_ProcessMessage_TriggersFreeze(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test: Redis not reachable: %v", err)
	}
	defer rdb.Close()

	log := logger.New("test-fraud", nil)
	mockClient := &mockLedgerServiceClient{}

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:      60 * time.Second,
		MaxAllowedInWindow:  2, // low threshold for testing
		HighAmountThreshold: 50000000.0,
	})

	c := consumer.NewFraudConsumer(nil, eval, mockClient, nil, log)

	accNum := "ACC-TEST-FRAUD-" + uuid.New().String()

	// Send 3 events (1st and 2nd pass, 3rd triggers fraud freeze)
	for i := 1; i <= 3; i++ {
		evt := domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        1000.0,
		}
		data, _ := json.Marshal(evt)
		msg := kafka.Message{
			Value: data,
			Time:  time.Now(),
		}
		err := c.ProcessMessage(context.Background(), msg)
		if err != nil {
			t.Fatalf("unexpected ProcessMessage error: %v", err)
		}
	}

	if atomic.LoadInt64(&mockClient.freezeCalls) != 1 {
		t.Fatalf("expected FreezeAccount to be called exactly once, got %d", mockClient.freezeCalls)
	}
	if mockClient.frozenAcc != accNum {
		t.Errorf("expected frozen account %s, got %s", accNum, mockClient.frozenAcc)
	}
}
