package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/auth"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	httphandler "github.com/wwafii/distributed-financial-ledger/services/gateway/internal/handler/http"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// securityHeadersMiddleware injects enterprise defense-in-depth HTTP headers
func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

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

	jaegerEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "localhost:4317"
	}

	shutdownTracer, err := telemetry.InitTracer(context.Background(), "api-gateway", jaegerEndpoint, log)
	if err == nil {
		defer shutdownTracer(context.Background())
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

	jwtSecret := []byte(os.Getenv("JWT_SECRET"))
	if len(jwtSecret) == 0 {
		jwtSecret = []byte("default-financial-ledger-jwt-secret-key-32b")
	}

	handler := httphandler.NewGatewayHandler(ledgerClient, idempEngine, log, jwtSecret)

	// 3. Setup Middlewares & Protection Layers
	authEnabled := os.Getenv("AUTH_ENABLED") != "false"
	authMW := auth.RequireAuth(handler.JWTSecret())
	adminMW := auth.RequireRole(auth.RoleAdmin)

	limiter := ratelimit.NewLimiter(rdb, 500, 1*time.Minute)

	// 4. Setup Routes (Go 1.22+ Standard Mux)
	mux := http.NewServeMux()

	// Public Ingress
	mux.HandleFunc("POST /api/v1/auth/token", handler.HandleIssueToken)
	mux.HandleFunc("GET /healthz", handler.HandleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Protected Banking Endpoints with Authentication & RBAC
	if authEnabled {
		mux.Handle("POST /api/v1/transactions", authMW(http.HandlerFunc(handler.HandleCreateTransaction)))
		mux.Handle("GET /api/v1/accounts/{account_number}/balance", authMW(http.HandlerFunc(handler.HandleGetBalance)))
		mux.Handle("POST /api/v1/accounts/{account_number}/freeze", authMW(adminMW(http.HandlerFunc(handler.HandleFreezeAccount))))
		log.Info("enterprise authentication & RBAC enabled on financial endpoints")
	} else {
		mux.HandleFunc("POST /api/v1/transactions", handler.HandleCreateTransaction)
		mux.HandleFunc("GET /api/v1/accounts/{account_number}/balance", handler.HandleGetBalance)
		mux.HandleFunc("POST /api/v1/accounts/{account_number}/freeze", handler.HandleFreezeAccount)
		log.Warn("⚠️ AUTH_ENABLED=false: gateway running in development/unauthenticated mode")
	}

	pipeline := telemetry.MetricsMiddleware(
		securityHeadersMiddleware(
			limiter.Middleware(mux),
		),
	)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", httpPort),
		Handler:      pipeline,
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

	// 5. Graceful Shutdown
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
