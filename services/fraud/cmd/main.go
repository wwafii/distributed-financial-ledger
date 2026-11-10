package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/consumer"
	"github.com/wwafii/distributed-financial-ledger/services/fraud/internal/evaluator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log := logger.New("fraud-service", os.Stdout)
	log.Info("starting real-time fraud engine service...")

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6380"
	}

	ledgerGRPCAddr := os.Getenv("LEDGER_GRPC_ADDR")
	if ledgerGRPCAddr == "" {
		ledgerGRPCAddr = "localhost:9101"
	}

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
		kafkaGroupID = "fraud-engine-v1"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Connect to Redis (for sub-5ms sliding-window velocity checks)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Error("failed to connect to redis", "addr", redisAddr, "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("connected to redis successfully", "addr", redisAddr)

	// 2. Connect to Core Ledger gRPC (for immediate account freeze)
	grpcConn, err := grpc.NewClient(
		ledgerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Error("failed to connect to ledger grpc", "addr", ledgerGRPCAddr, "error", err)
		os.Exit(1)
	}
	defer grpcConn.Close()
	ledgerClient := ledgerv1.NewLedgerServiceClient(grpcConn)
	log.Info("connected to ledger grpc service", "addr", ledgerGRPCAddr)

	// 3. Setup Kafka Reader & Publisher
	brokerList := strings.Split(kafkaBrokers, ",")
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

	publisher := pkgkafka.NewKafkaPublisher(brokerList)
	defer publisher.Close()

	// 4. Initialize Evaluator & Consumer
	eval := evaluator.NewVelocityEvaluator(rdb, evaluator.DefaultConfig())
	fraudConsumer := consumer.NewFraudConsumer(reader, eval, ledgerClient, publisher, log)

	go fraudConsumer.Start(ctx)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down fraud service gracefully...")
	cancel()
	_ = reader.Close()
	log.Info("fraud service stopped cleanly")
}
