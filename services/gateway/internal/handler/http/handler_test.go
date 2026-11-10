package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/logger"
	gatewayhttp "github.com/wwafii/distributed-financial-ledger/services/gateway/internal/handler/http"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
	"google.golang.org/grpc"
)

type mockLedgerClient struct {
	callCount int64
}

func (m *mockLedgerClient) CreateTransaction(ctx context.Context, in *ledgerv1.CreateTransactionRequest, opts ...grpc.CallOption) (*ledgerv1.CreateTransactionResponse, error) {
	atomic.AddInt64(&m.callCount, 1)
	return &ledgerv1.CreateTransactionResponse{
		TransactionId: uuid.New().String(),
		ReferenceId:   in.ReferenceId,
		Status:        "POSTED",
		CreatedAt:     time.Now().Format(time.RFC3339),
	}, nil
}

func (m *mockLedgerClient) GetAccountBalance(ctx context.Context, in *ledgerv1.GetAccountBalanceRequest, opts ...grpc.CallOption) (*ledgerv1.GetAccountBalanceResponse, error) {
	return &ledgerv1.GetAccountBalanceResponse{
		AccountId:     uuid.New().String(),
		AccountNumber: in.AccountNumber,
		HolderName:    "Test Holder",
		Balance:       1000000.0,
		Currency:      "IDR",
		Status:        "ACTIVE",
	}, nil
}

func (m *mockLedgerClient) FreezeAccount(ctx context.Context, in *ledgerv1.FreezeAccountRequest, opts ...grpc.CallOption) (*ledgerv1.FreezeAccountResponse, error) {
	return &ledgerv1.FreezeAccountResponse{
		Success:       true,
		AccountNumber: in.AccountNumber,
		Status:        "FROZEN",
		Message:       "account frozen",
	}, nil
}

const testRedisAddr = "localhost:6380"

func TestGatewayHandler_IdempotentTransfer(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unreachable: %v", err)
	}
	defer rdb.Close()

	mockClient := &mockLedgerClient{}
	idempEngine := idempotency.NewEngine(rdb)
	log := logger.New("gateway-test", os.Stdout)

	handler := gatewayhttp.NewGatewayHandler(mockClient, idempEngine, log)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/transactions", handler.HandleCreateTransaction)

	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Missing Idempotency-Key
	reqNoKey, _ := http.NewRequest("POST", server.URL+"/api/v1/transactions", bytes.NewBuffer([]byte(`{}`)))
	respNoKey, err := http.DefaultClient.Do(reqNoKey)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	respNoKey.Body.Close()
	if respNoKey.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing key, got: %d", respNoKey.StatusCode)
	}

	// 2. First Transfer
	idempKey := "test-key-" + uuid.New().String()
	body := map[string]interface{}{
		"source_account":      "ACC-ALICE-01",
		"destination_account": "ACC-BOB-02",
		"amount":              25000.0,
		"currency":            "IDR",
		"description":         "Transfer test",
	}
	bodyBytes, _ := json.Marshal(body)

	req1, _ := http.NewRequest("POST", server.URL+"/api/v1/transactions", bytes.NewBuffer(bodyBytes))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Idempotency-Key", idempKey)

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created, got: %d", resp1.StatusCode)
	}
	if atomic.LoadInt64(&mockClient.callCount) != 1 {
		t.Errorf("expected mockClient call count 1, got %d", mockClient.callCount)
	}

	// 3. Replay with the same Idempotency-Key
	req2, _ := http.NewRequest("POST", server.URL+"/api/v1/transactions", bytes.NewBuffer(bodyBytes))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", idempKey)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created on replay, got: %d", resp2.StatusCode)
	}
	if resp2.Header.Get("X-Idempotent-Replay") != "true" {
		t.Errorf("expected header X-Idempotent-Replay: true")
	}

	// CRITICAL INVARIANT: Ledger RPC MUST NOT be called a second time!
	if atomic.LoadInt64(&mockClient.callCount) != 1 {
		t.Errorf("FATAL: Ledger RPC was called multiple times! Count: %d", mockClient.callCount)
	}
}
