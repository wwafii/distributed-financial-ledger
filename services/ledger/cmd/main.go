package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	pkgkafka "github.com/wwafii/distributed-financial-ledger/pkg/kafka"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	grpchandler "github.com/wwafii/distributed-financial-ledger/services/ledger/internal/handler/grpc"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/repository/postgres"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log := logger.New("ledger-service", os.Stdout)
	log.Info("starting ledger service...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5433/ledger_db?sslmode=disable"
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9101"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9094"
	}

	jaegerEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "localhost:4317"
	}

	shutdownTracer, err := telemetry.InitTracer(context.Background(), "ledger-service", jaegerEndpoint, log)
	if err == nil {
		defer shutdownTracer(context.Background())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("failed to connect to postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}
	log.Info("connected to postgresql successfully")

	// 1. Initialize and Start Outbox Relay Background Worker (Phase 3)
	brokerList := strings.Split(kafkaBrokers, ",")
	kafkaPublisher := pkgkafka.NewKafkaPublisher(brokerList)
	defer kafkaPublisher.Close()

	relay := worker.NewOutboxRelay(pool, kafkaPublisher, log, worker.DefaultConfig())
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()

	var relayWg sync.WaitGroup
	relayWg.Add(1)
	go func() {
		defer relayWg.Done()
		relay.Start(relayCtx)
	}()
	log.Info("outbox relay background worker started", "brokers", brokerList)

	// 2. Initialize and Start gRPC Server
	repo := postgres.NewLedgerRepository(pool)
	handler := grpchandler.NewLedgerHandler(repo)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	if err != nil {
		log.Error("failed to listen on grpc port", "port", grpcPort, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	// 3. Start Prometheus Metrics HTTP Server on :9111
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9111"
	}
	metricsServer := telemetry.NewMetricsServer(metricsPort)
	go func() {
		log.Info("ledger metrics server listening", "port", metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("ledger metrics server error", "error", err)
		}
	}()
	defer metricsServer.Close()

	go func() {
		log.Info("gRPC server listening", "port", grpcPort)
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("gRPC server error", "error", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down ledger service gracefully...")
	cancelRelay()
	relayWg.Wait()
	grpcServer.GracefulStop()
	log.Info("ledger service stopped cleanly")
}
