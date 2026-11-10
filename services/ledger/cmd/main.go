package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	grpchandler "github.com/wwafii/distributed-financial-ledger/services/ledger/internal/handler/grpc"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/repository/postgres"
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

	log.Info("shutting down ledger gRPC server gracefully...")
	grpcServer.GracefulStop()
	log.Info("ledger service stopped cleanly")
}
