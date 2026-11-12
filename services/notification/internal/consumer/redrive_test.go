package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/consumer"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/domain"
)

type mockReader struct {
	messages []kafka.Message
	idx      int
	commits  []kafka.Message
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.idx >= len(m.messages) {
		return kafka.Message{}, context.DeadlineExceeded
	}
	msg := m.messages[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.commits = append(m.commits, msgs...)
	return nil
}

func TestDLQRedriver_RedriveBatch_Success(t *testing.T) {
	log := logger.New("test-redrive", nil)

	// Prepare 2 DLQ messages
	dlq1 := domain.DLQMessage{
		OriginalTopic: "transaction.created",
		OriginalKey:   "tx-fail-1",
		Payload:       `{"amount": 100000.0}`,
		ErrorReason:   "HTTP 503 Service Unavailable",
		RetryCount:    4,
		FailedAt:      time.Now().Format(time.RFC3339),
	}
	bytes1, _ := json.Marshal(dlq1)

	dlq2 := domain.DLQMessage{
		OriginalTopic: "transaction.created",
		OriginalKey:   "tx-fail-2",
		Payload:       `{"amount": 200000.0}`,
		ErrorReason:   "network timeout",
		RetryCount:    4,
		FailedAt:      time.Now().Format(time.RFC3339),
	}
	bytes2, _ := json.Marshal(dlq2)

	reader := &mockReader{
		messages: []kafka.Message{
			{Topic: "transaction.dlq", Key: []byte("tx-fail-1"), Value: bytes1},
			{Topic: "transaction.dlq", Key: []byte("tx-fail-2"), Value: bytes2},
		},
	}
	publisher := &mockDLQPublisher{}

	redriver := consumer.NewDLQRedriver(reader, publisher, log, consumer.DLQRedriverConfig{
		DLQTopic:    "transaction.dlq",
		PollTimeout: 10 * time.Millisecond,
	})

	count, err := redriver.RedriveBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected redrive error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 redriven messages, got %d", count)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	if len(publisher.publishedList) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(publisher.publishedList))
	}

	// Verify msg 1
	if publisher.publishedList[0].Topic != "transaction.created" {
		t.Errorf("expected topic transaction.created, got %s", publisher.publishedList[0].Topic)
	}
	if publisher.publishedList[0].Key != "tx-fail-1" {
		t.Errorf("expected key tx-fail-1, got %s", publisher.publishedList[0].Key)
	}
	if string(publisher.publishedList[0].Payload) != `{"amount": 100000.0}` {
		t.Errorf("unexpected payload: %s", string(publisher.publishedList[0].Payload))
	}

	// Verify msg 2
	if publisher.publishedList[1].Key != "tx-fail-2" {
		t.Errorf("expected key tx-fail-2, got %s", publisher.publishedList[1].Key)
	}

	// Verify both were committed in DLQ
	if len(reader.commits) != 2 {
		t.Errorf("expected 2 committed DLQ offsets, got %d", len(reader.commits))
	}
}

func TestDLQRedriver_RedriveBatch_WithTopicOverride(t *testing.T) {
	log := logger.New("test-redrive", nil)

	dlq := domain.DLQMessage{
		OriginalTopic: "transaction.created",
		OriginalKey:   "tx-custom",
		Payload:       `{"amount": 50000.0}`,
	}
	b, _ := json.Marshal(dlq)

	reader := &mockReader{
		messages: []kafka.Message{
			{Topic: "transaction.dlq", Key: []byte("tx-custom"), Value: b},
		},
	}
	publisher := &mockDLQPublisher{}

	redriver := consumer.NewDLQRedriver(reader, publisher, log, consumer.DLQRedriverConfig{
		DLQTopic:       "transaction.dlq",
		TargetOverride: "transaction.replayed",
		PollTimeout:    10 * time.Millisecond,
	})

	count, err := redriver.RedriveBatch(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 redriven message, got %d", count)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.publishedList[0].Topic != "transaction.replayed" {
		t.Errorf("expected override topic 'transaction.replayed', got %s", publisher.publishedList[0].Topic)
	}
}

func TestDLQRedriver_HandlesMalformedJSON(t *testing.T) {
	log := logger.New("test-redrive", nil)

	reader := &mockReader{
		messages: []kafka.Message{
			{Topic: "transaction.dlq", Key: []byte("bad-1"), Value: []byte("not valid json")},
		},
	}
	publisher := &mockDLQPublisher{}

	redriver := consumer.NewDLQRedriver(reader, publisher, log, consumer.DLQRedriverConfig{
		DLQTopic:    "transaction.dlq",
		PollTimeout: 10 * time.Millisecond,
	})

	count, err := redriver.RedriveBatch(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Malformed is skipped and committed to prevent blocking
	if count != 0 {
		t.Errorf("expected 0 redriven messages for malformed json, got %d", count)
	}
	if len(reader.commits) != 1 {
		t.Errorf("expected 1 commit for skipped malformed message, got %d", len(reader.commits))
	}
}

func TestDLQRedriver_ReturnsErrorOnPublishFailure(t *testing.T) {
	log := logger.New("test-redrive", nil)

	dlq := domain.DLQMessage{OriginalTopic: "t", OriginalKey: "k", Payload: "p"}
	b, _ := json.Marshal(dlq)

	reader := &mockReader{
		messages: []kafka.Message{
			{Topic: "transaction.dlq", Key: []byte("k"), Value: b},
		},
	}
	failingPublisher := &failingMockPublisher{}

	redriver := consumer.NewDLQRedriver(reader, failingPublisher, log, consumer.DLQRedriverConfig{
		DLQTopic:    "transaction.dlq",
		PollTimeout: 10 * time.Millisecond,
	})

	_, err := redriver.RedriveBatch(context.Background(), 5)
	if err == nil {
		t.Fatalf("expected publish error, got nil")
	}

	// Offset must NOT be committed if publisher fails!
	if len(reader.commits) != 0 {
		t.Errorf("expected 0 commits on publish failure, got %d", len(reader.commits))
	}
}

type failingMockPublisher struct{}

func (f *failingMockPublisher) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	return errors.New("simulated broker out-of-order error")
}

func (f *failingMockPublisher) Close() error { return nil }
