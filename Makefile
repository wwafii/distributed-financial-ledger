.PHONY: all help infra infra-down dev test race build k8s clean

all: help

help:
	@echo "================================================================="
	@echo "🏛️  DISTRIBUTED FINANCIAL LEDGER - COMMAND CENTER"
	@echo "================================================================="
	@echo "  make infra       - Start PostgreSQL, Redis, Redpanda, Jaeger, Prometheus, Grafana"
	@echo "  make infra-down  - Stop all background Docker containers"
	@echo "  make dev         - Run all 4 Go microservices simultaneously in 1 terminal"
	@echo "  make test        - Run all automated unit and integration tests"
	@echo "  make race        - Run tests with Go Data Race Detector (-race)"
	@echo "  make build       - Compile all 5 microservice binaries into bin/"
	@echo "  make k8s         - Deploy to Kubernetes cluster using Kustomize"

infra:
	docker compose -f deployments/docker-compose.yml up -d

infra-down:
	docker compose -f deployments/docker-compose.yml down

dev:
	./scripts/dev.sh

test:
	go test -v ./...

race:
	go test -race ./...

build:
	@mkdir -p bin
	go build -o bin/gateway ./services/gateway/cmd/main.go
	go build -o bin/ledger ./services/ledger/cmd/main.go
	go build -o bin/fraud ./services/fraud/cmd/main.go
	go build -o bin/notification ./services/notification/cmd/main.go
	go build -o bin/redrive ./services/notification/cmd/redrive/main.go
	@echo "✅ All 5 binaries compiled successfully into bin/"

k8s:
	kubectl apply -k deployments/k8s/
