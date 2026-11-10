package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	httphandler "github.com/wwafii/distributed-financial-ledger/services/gateway/internal/handler/http"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log := logger.New("gateway-service", os.Stdout)
	log.Info("starting api gateway service...")

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8000"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6380"
	}

	ledgerGRPCAddr := os.Getenv("LEDGER_GRPC_ADDR")
	if ledgerGRPCAddr == "" {
		ledgerGRPCAddr = "localhost:9101"
	}

	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("failed to ping redis", "addr", redisAddr, "error", err)
		os.Exit(1)
	}
	log.Info("connected to redis successfully", "addr", redisAddr)

	// 2. Connect to Ledger gRPC Service
	grpcConn, err := grpc.NewClient(
		ledgerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Error("failed to dial ledger grpc server", "addr", ledgerGRPCAddr, "error", err)
		os.Exit(1)
	}
	defer grpcConn.Close()

	ledgerClient := ledgerv1.NewLedgerServiceClient(grpcConn)
	idempEngine := idempotency.NewEngine(rdb)

	handler := httphandler.NewGatewayHandler(ledgerClient, idempEngine, log)

	// 3. Setup Routes (Go 1.22+ Standard Mux)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/transactions", handler.HandleCreateTransaction)
	mux.HandleFunc("GET /api/v1/accounts/{account_number}/balance", handler.HandleGetBalance)
	mux.HandleFunc("POST /api/v1/accounts/{account_number}/freeze", handler.HandleFreezeAccount)
	mux.HandleFunc("GET /healthz", handler.HandleHealthz)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", httpPort),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("HTTP gateway listening", "port", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP gateway server error", "error", err)
		}
	}()

	// 4. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down HTTP gateway gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to shutdown server gracefully", "error", err)
	}
	log.Info("HTTP gateway stopped cleanly")
}
