package consumer_test

import (
	"context"
	"encoding/json"
	"sync"
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

type mockPublisher struct {
	mu            sync.Mutex
	publishedList []struct {
		Topic   string
		Key     string
		Payload []byte
	}
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedList = append(m.publishedList, struct {
		Topic   string
		Key     string
		Payload []byte
	}{Topic: topic, Key: key, Payload: payload})
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
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
	mockPub := &mockPublisher{}

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        2, // low threshold for testing
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           2,
		HighAmountThreshold:       50000000.0,
		CumulativeAmountThreshold: 100000000.0,
	})

	c := consumer.NewFraudConsumer(nil, eval, mockClient, mockPub, log)
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

func TestFraudConsumer_HighAmount_TriggersChallenge_NoFreeze(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test: Redis not reachable: %v", err)
	}
	defer rdb.Close()

	log := logger.New("test-fraud", nil)
	mockClient := &mockLedgerServiceClient{}
	mockPub := &mockPublisher{}

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           5,
		HighAmountThreshold:       50000000.0, // 50M threshold
		CumulativeAmountThreshold: 200000000.0,
	})

	c := consumer.NewFraudConsumer(nil, eval, mockClient, mockPub, log)
	accNum := "ACC-BUY-CAR-" + uuid.New().String()

	// Single high transaction: Buying a car (Rp 75 Juta)
	evt := domain.TransactionCreatedEvent{
		TransactionID: uuid.New().String(),
		SourceAccount: accNum,
		Amount:        75000000.0,
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

	// CRITICAL ASSERTION: The car buyer MUST NOT be frozen!
	if atomic.LoadInt64(&mockClient.freezeCalls) != 0 {
		t.Fatalf("expected FreezeAccount NOT to be called for car purchase, but got called %d times!", mockClient.freezeCalls)
	}

	// Verify that a Step-Up Challenge event was published to Kafka
	mockPub.mu.Lock()
	defer mockPub.mu.Unlock()
	if len(mockPub.publishedList) == 0 {
		t.Fatalf("expected challenge event to be published to Kafka, but none was sent")
	}
	if mockPub.publishedList[0].Topic != "fraud.challenge_required" {
		t.Errorf("expected topic fraud.challenge_required, got %s", mockPub.publishedList[0].Topic)
	}
}

func TestFraudConsumer_CumulativeAmount_TriggersHold_NoFreeze(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test: Redis not reachable: %v", err)
	}
	defer rdb.Close()

	log := logger.New("test-fraud", nil)
	mockClient := &mockLedgerServiceClient{}
	mockPub := &mockPublisher{}

	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.VelocityConfig{
		WindowDuration:            60 * time.Second,
		MaxAllowedInWindow:        10,
		BurstWindowDuration:       10 * time.Second,
		MaxBurstAllowed:           10, // burst relaxed
		HighAmountThreshold:       100000000.0,
		CumulativeAmountThreshold: 40000000.0, // 40M cumulative limit
	})

	c := consumer.NewFraudConsumer(nil, eval, mockClient, mockPub, log)
	accNum := "ACC-SPLIT-PAY-" + uuid.New().String()

	// 2 split payments of 25M = 50M total (> 40M cumulative limit)
	for i := 1; i <= 2; i++ {
		evt := domain.TransactionCreatedEvent{
			TransactionID: uuid.New().String(),
			SourceAccount: accNum,
			Amount:        25000000.0,
		}
		data, _ := json.Marshal(evt)
		msg := kafka.Message{
			Value: data,
			Time:  time.Now(),
		}
		_ = c.ProcessMessage(context.Background(), msg)
	}

	// CRITICAL ASSERTION: Split payment user MUST NOT be frozen!
	if atomic.LoadInt64(&mockClient.freezeCalls) != 0 {
		t.Fatalf("expected FreezeAccount NOT to be called for split payments, but got called %d times!", mockClient.freezeCalls)
	}

	// Verify that a Review Hold event was published to Kafka
	mockPub.mu.Lock()
	defer mockPub.mu.Unlock()
	foundHold := false
	for _, p := range mockPub.publishedList {
		if p.Topic == "fraud.review_hold" {
			foundHold = true
			break
		}
	}
	if !foundHold {
		t.Errorf("expected fraud.review_hold event to be published for cumulative breach")
	}
}
