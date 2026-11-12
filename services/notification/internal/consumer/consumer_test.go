package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/consumer"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/sender"
)

type mockWebhookClient struct {
	calls        int64
	failCount    int64 // number of times to return error before success
	errToReturn  error
	mu           sync.Mutex
	receivedBody []byte
}

func (m *mockWebhookClient) Send(ctx context.Context, targetURL string, payload []byte) error {
	callIdx := atomic.AddInt64(&m.calls, 1)
	m.mu.Lock()
	m.receivedBody = payload
	m.mu.Unlock()

	if callIdx <= m.failCount {
		return m.errToReturn
	}
	return nil
}

type mockDLQPublisher struct {
	mu            sync.Mutex
	publishedList []struct {
		Topic   string
		Key     string
		Payload []byte
	}
}

func (m *mockDLQPublisher) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedList = append(m.publishedList, struct {
		Topic   string
		Key     string
		Payload []byte
	}{Topic: topic, Key: key, Payload: payload})
	return nil
}

func (m *mockDLQPublisher) Close() error {
	return nil
}

func TestNotificationConsumer_SuccessOnFirstAttempt(t *testing.T) {
	log := logger.New("test-notif", nil)
	client := &mockWebhookClient{failCount: 0}
	pub := &mockDLQPublisher{}

	cfg := consumer.DefaultConfig()
	cfg.SleepFn = func(d time.Duration) {} // no real delay

	c := consumer.NewNotificationConsumer(nil, client, pub, log, cfg)

	msg := kafka.Message{
		Topic: "transaction.created",
		Key:   []byte("tx-123"),
		Value: []byte(`{"amount": 50000.0}`),
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if atomic.LoadInt64(&client.calls) != 1 {
		t.Errorf("expected exactly 1 call, got %d", client.calls)
	}
	if len(pub.publishedList) != 0 {
		t.Errorf("expected 0 DLQ publications on success, got %d", len(pub.publishedList))
	}
}

func TestNotificationConsumer_SuccessAfterTransientRetries(t *testing.T) {
	log := logger.New("test-notif", nil)
	// Fails 2 times with retryable 500 server error, then succeeds on 3rd attempt
	client := &mockWebhookClient{
		failCount:   2,
		errToReturn: &sender.RetryableError{Err: errors.New("temporary server 503 unavailable")},
	}
	pub := &mockDLQPublisher{}

	var sleepCalls int
	cfg := consumer.DefaultConfig()
	cfg.MaxRetries = 3
	cfg.SleepFn = func(d time.Duration) {
		sleepCalls++
	}

	c := consumer.NewNotificationConsumer(nil, client, pub, log, cfg)

	msg := kafka.Message{
		Topic: "transaction.created",
		Key:   []byte("tx-retry"),
		Value: []byte(`{"amount": 100000.0}`),
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if atomic.LoadInt64(&client.calls) != 3 {
		t.Errorf("expected 3 total attempts (2 retries + 1 success), got %d", client.calls)
	}
	if sleepCalls != 2 {
		t.Errorf("expected 2 backoff sleeps, got %d", sleepCalls)
	}
	if len(pub.publishedList) != 0 {
		t.Errorf("expected 0 DLQ publications on eventual success, got %d", len(pub.publishedList))
	}
}

func TestNotificationConsumer_AllRetriesExhausted_RoutesToDLQ(t *testing.T) {
	log := logger.New("test-notif", nil)
	// Fails permanently with retryable errors that never resolve
	client := &mockWebhookClient{
		failCount:   99, // always fails
		errToReturn: &sender.RetryableError{Err: errors.New("upstream gateway timeout 504")},
	}
	pub := &mockDLQPublisher{}

	cfg := consumer.DefaultConfig()
	cfg.MaxRetries = 3
	cfg.DLQTopic = "transaction.dlq"
	cfg.SleepFn = func(d time.Duration) {}

	c := consumer.NewNotificationConsumer(nil, client, pub, log, cfg)

	payload := []byte(`{"transaction_id": "tx-dead-letter-01", "amount": 250000.0}`)
	msg := kafka.Message{
		Topic: "transaction.created",
		Key:   []byte("tx-dead-letter-01"),
		Value: payload,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("ProcessMessage should not return error on DLQ routing (must commit offset): %v", err)
	}

	// MaxRetries = 3 means 1 initial attempt + 3 retries = 4 total attempts
	if atomic.LoadInt64(&client.calls) != 4 {
		t.Errorf("expected 4 total attempts before DLQ routing, got %d", client.calls)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.publishedList) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(pub.publishedList))
	}

	dlqEntry := pub.publishedList[0]
	if dlqEntry.Topic != "transaction.dlq" {
		t.Errorf("expected DLQ topic 'transaction.dlq', got %s", dlqEntry.Topic)
	}
	if dlqEntry.Key != "tx-dead-letter-01" {
		t.Errorf("expected key 'tx-dead-letter-01', got %s", dlqEntry.Key)
	}

	var dlqMsg domain.DLQMessage
	if err := json.Unmarshal(dlqEntry.Payload, &dlqMsg); err != nil {
		t.Fatalf("failed to deserialize DLQ payload: %v", err)
	}

	if dlqMsg.OriginalTopic != "transaction.created" {
		t.Errorf("expected original topic 'transaction.created', got %s", dlqMsg.OriginalTopic)
	}
	if dlqMsg.RetryCount != 4 {
		t.Errorf("expected retry count 4 in DLQ metadata, got %d", dlqMsg.RetryCount)
	}
}

func TestNotificationConsumer_NonRetryable_ImmediatelyRoutesToDLQ(t *testing.T) {
	log := logger.New("test-notif", nil)
	// Non-retryable error (e.g. 400 Bad Request)
	client := &mockWebhookClient{
		failCount:   99,
		errToReturn: &sender.NonRetryableError{Err: errors.New("client error 400 Bad Request")},
	}
	pub := &mockDLQPublisher{}

	cfg := consumer.DefaultConfig()
	cfg.MaxRetries = 5 // even with 5 max retries, non-retryable must abort on attempt 1!
	cfg.SleepFn = func(d time.Duration) {}

	c := consumer.NewNotificationConsumer(nil, client, pub, log, cfg)

	msg := kafka.Message{
		Topic: "transaction.created",
		Key:   []byte("tx-bad-request"),
		Value: []byte(`invalid json`),
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must fail on attempt 1 and NOT waste any retries!
	if atomic.LoadInt64(&client.calls) != 1 {
		t.Errorf("expected exactly 1 attempt before immediate DLQ routing for non-retryable error, got %d", client.calls)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.publishedList) != 1 {
		t.Fatalf("expected 1 message published to DLQ, got %d", len(pub.publishedList))
	}
}

func TestDefaultEqualJitter(t *testing.T) {
	baseDuration := 1000 * time.Millisecond
	half := baseDuration / 2

	for i := 0; i < 50; i++ {
		jittered := consumer.DefaultEqualJitter(baseDuration)
		if jittered < half || jittered > baseDuration {
			t.Errorf("jittered duration %v should be between %v and %v", jittered, half, baseDuration)
		}
	}

	// Edge case: zero or negative duration
	if res := consumer.DefaultEqualJitter(0); res != 0 {
		t.Errorf("expected 0 for zero duration, got %v", res)
	}
}

func TestNotificationConsumer_UsesCustomJitter(t *testing.T) {
	log := logger.New("test-notif", nil)
	client := &mockWebhookClient{
		failCount:   1,
		errToReturn: &sender.RetryableError{Err: errors.New("temporary failure")},
	}
	pub := &mockDLQPublisher{}

	var receivedSleep time.Duration
	cfg := consumer.DefaultConfig()
	cfg.MaxRetries = 1
	cfg.InitialBackoff = 200 * time.Millisecond
	cfg.JitterFn = func(d time.Duration) time.Duration {
		return d + 42*time.Millisecond
	}
	cfg.SleepFn = func(d time.Duration) {
		receivedSleep = d
	}

	c := consumer.NewNotificationConsumer(nil, client, pub, log, cfg)

	msg := kafka.Message{
		Topic: "transaction.created",
		Key:   []byte("tx-jitter"),
		Value: []byte(`{}`),
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedSleep := 200*time.Millisecond + 42*time.Millisecond
	if receivedSleep != expectedSleep {
		t.Errorf("expected sleep with custom jitter %v, got %v", expectedSleep, receivedSleep)
	}
}
