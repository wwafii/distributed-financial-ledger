package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/consumer"
	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/sender"
)

func main() {
	log := logger.New("notification-service", os.Stdout)
	log.Info("starting resilient notification worker...")

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9094"
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "transaction.created"
	}

	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	if kafkaGroupID == "" {
		kafkaGroupID = "notification-workers-v1"
	}

	jaegerEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "localhost:4317"
	}

	shutdownTracer, err := telemetry.InitTracer(context.Background(), "notification-service", jaegerEndpoint, log)
	if err == nil {
		defer shutdownTracer(context.Background())
	}

	// Start Prometheus Metrics HTTP Server on :9103
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9103"
	}
	metricsServer := telemetry.NewMetricsServer(metricsPort)
	go func() {
		log.Info("notification metrics server listening", "port", metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("notification metrics server error", "error", err)
		}
	}()
	defer metricsServer.Close()

	cfg := consumer.ConfigFromEnv()
	brokerList := strings.Split(kafkaBrokers, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Webhook Dispatcher
	webhookSender := sender.NewHTTPWebhookSender(5 * time.Second)

	// 2. Initialize DLQ Kafka Publisher
	publisher := pkgkafka.NewKafkaPublisher(brokerList)
	defer publisher.Close()

	// 3. Initialize Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          kafkaTopic,
		GroupID:        kafkaGroupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	// 4. Start Notification Consumer
	notifConsumer := consumer.NewNotificationConsumer(reader, webhookSender, publisher, log, cfg)
	go notifConsumer.Start(ctx)

	log.Info("notification worker initialized successfully",
		"topic", kafkaTopic,
		"group_id", kafkaGroupID,
		"dlq_topic", cfg.DLQTopic,
		"webhook_url", cfg.WebhookURL,
		"max_retries", cfg.MaxRetries,
	)

	// 5. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down notification worker gracefully...")
	cancel()
	_ = reader.Close()
	log.Info("notification worker stopped cleanly")
}
