package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
)

// EventPublisher abstracts the message broker dispatch
type EventPublisher interface {
	Publish(ctx context.Context, topic string, key string, payload []byte) error
	Close() error
}

// KafkaPublisher implements EventPublisher using segmentio/kafka-go Writer with OTel trace propagation
type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Synchronous dispatch for reliability
	}
	return &KafkaPublisher{writer: writer}
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	}

	// Propagate W3C distributed trace context across Kafka partitions
	telemetry.InjectKafka(ctx, &msg)

	return p.writer.WriteMessages(ctx, msg)
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
