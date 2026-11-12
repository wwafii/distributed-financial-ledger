package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/consumer"
)

func main() {
	var (
		kafkaBrokers   = flag.String("brokers", "localhost:9094", "Kafka broker addresses separated by comma")
		dlqTopic       = flag.String("dlq-topic", "transaction.dlq", "Dead Letter Queue topic to redrive from")
		targetTopic    = flag.String("target-topic", "", "Target topic to republish messages to (defaults to original topic from DLQ metadata)")
		maxMessages    = flag.Int("max", 50, "Maximum number of DLQ messages to redrive in this batch")
		pollTimeoutSec = flag.Int("timeout-sec", 5, "Polling timeout in seconds when waiting for messages")
	)
	flag.Parse()

	log := logger.New("dlq-redrive-cli", os.Stdout)
	log.Info("starting DLQ re-drive operation...",
		"dlq_topic", *dlqTopic,
		"max_messages", *maxMessages,
		"target_override", *targetTopic,
	)

	brokerList := strings.Split(*kafkaBrokers, ",")
	groupID := fmt.Sprintf("dlq-redriver-%d", time.Now().UnixNano())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          *dlqTopic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset, // Start from beginning of DLQ
	})
	defer reader.Close()

	publisher := pkgkafka.NewKafkaPublisher(brokerList)
	defer publisher.Close()

	redriver := consumer.NewDLQRedriver(reader, publisher, log, consumer.DLQRedriverConfig{
		DLQTopic:       *dlqTopic,
		TargetOverride: *targetTopic,
		PollTimeout:    time.Duration(*pollTimeoutSec) * time.Second,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	count, err := redriver.RedriveBatch(ctx, *maxMessages)
	if err != nil {
		log.Error("DLQ redrive failed with error", "error", err, "redriven_count", count)
		os.Exit(1)
	}

	log.Info("DLQ redrive operation finished successfully",
		"total_redriven", count,
		"dlq_topic", *dlqTopic,
	)
}
