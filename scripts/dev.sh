#!/usr/bin/env bash
# =============================================================================
# Distributed Financial Ledger - Local Dev Runner (All Microservices in 1 Command)
# =============================================================================
set -e

# Change directory to project root
cd "$(dirname "$0")/.."

# Load .env if present, otherwise fallback to .env.example
if [ -f .env ]; then
    echo "📄 Loading environment variables from .env..."
    set -a
    source .env
    set +a
elif [ -f .env.example ]; then
    echo "⚠️  No .env file found, using defaults from .env.example..."
    set -a
    source .env.example
    set +a
fi

echo "================================================================="
echo "🏛️  STARTING ALL 4 FINANCIAL LEDGER MICROSERVICES SIMULTANEOUSLY"
echo "================================================================="

PIDS=()

# Trap SIGINT/SIGTERM to gracefully terminate all child background processes
cleanup() {
    echo ""
    echo "🛑 Shutting down all microservices gracefully..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "✅ All microservices stopped cleanly."
}
trap cleanup SIGINT SIGTERM EXIT

# 1. Start Core Ledger Service (gRPC :9101, Metrics :9111)
echo "🚀 [1/4] Starting Core Ledger Service..."
go run ./services/ledger/cmd/main.go &
PIDS+=($!)

# Wait 1s for Ledger gRPC listener to bind
sleep 1

# 2. Start API Gateway Service (HTTP :8000)
echo "🚀 [2/4] Starting API Gateway Service..."
go run ./services/gateway/cmd/main.go &
PIDS+=($!)

# 3. Start Real-time Fraud Engine Service
echo "🚀 [3/4] Starting Real-Time Fraud Engine Service..."
go run ./services/fraud/cmd/main.go &
PIDS+=($!)

# 4. Start Notification Worker Service
echo "🚀 [4/4] Starting Resilient Notification Worker Service..."
go run ./services/notification/cmd/main.go &
PIDS+=($!)

echo "================================================================="
echo "✨ ALL 4 MICROSERVICES ARE RUNNING!"
echo "   - Gateway Ingress: http://localhost:8000"
echo "   - Ledger gRPC:     localhost:9101"
echo "   - Redpanda Web UI: http://localhost:8085"
echo "   - Prometheus:      http://localhost:9090"
echo "   - Grafana:         http://localhost:3001"
echo "   (Press Ctrl+C to terminate all services at once)"
echo "================================================================="

# Wait for all background processes
wait
