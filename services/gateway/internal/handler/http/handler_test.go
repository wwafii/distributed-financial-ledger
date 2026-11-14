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
	"github.com/wwafii/distributed-financial-ledger/pkg/auth"
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

func TestGatewayHandler_Security_TokenIssuance(t *testing.T) {
	mockClient := &mockLedgerClient{}
	log := logger.New("gateway-test", os.Stdout)
	secret := []byte("test-jwt-secret-signing-key-123456")
	handler := gatewayhttp.NewGatewayHandler(mockClient, nil, log, secret)

	reqBody := map[string]string{
		"account_number": "ACC-ALICE-01",
		"role":           "customer",
	}
	b, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/auth/token", bytes.NewReader(b))
	rec := httptest.NewRecorder()

	handler.HandleIssueToken(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", rec.Code)
	}

	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if !resp.Success || resp.Data.Token == "" {
		t.Fatalf("expected successful token issuance, got response: %s", rec.Body.String())
	}

	// Verify token decodes properly
	claims, err := auth.ParseToken(resp.Data.Token, secret)
	if err != nil {
		t.Fatalf("failed to verify issued token: %v", err)
	}
	if claims.AccountNumber != "ACC-ALICE-01" {
		t.Errorf("expected claims account ACC-ALICE-01, got: %s", claims.AccountNumber)
	}
}

func TestGatewayHandler_Security_IDOR_Protection(t *testing.T) {
	mockClient := &mockLedgerClient{}
	log := logger.New("gateway-test", os.Stdout)
	secret := []byte("test-jwt-secret-signing-key-123456")
	handler := gatewayhttp.NewGatewayHandler(mockClient, nil, log, secret)

	// Alice's token
	aliceClaims := auth.Claims{
		Subject:       "ACC-ALICE-01",
		AccountNumber: "ACC-ALICE-01",
		Role:          auth.RoleCustomer,
		ExpiresAt:     time.Now().Add(1 * time.Hour).Unix(),
	}
	aliceToken, _ := auth.SignToken(aliceClaims, secret)

	authMW := auth.RequireAuth(secret)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/transactions", authMW(http.HandlerFunc(handler.HandleCreateTransaction)))
	mux.Handle("GET /api/v1/accounts/{account_number}/balance", authMW(http.HandlerFunc(handler.HandleGetBalance)))

	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. IDOR Attempt: Alice tries to transfer money from Bob's account (ACC-BOB-02) -> MUST BE 403 FORBIDDEN
	attackBody := map[string]interface{}{
		"source_account":      "ACC-BOB-02", // Victim account!
		"destination_account": "ACC-ALICE-01",
		"amount":              50000.0,
		"currency":            "IDR",
		"description":         "Malicious IDOR transfer",
	}
	attackBytes, _ := json.Marshal(attackBody)

	req1, _ := http.NewRequest("POST", server.URL+"/api/v1/transactions", bytes.NewBuffer(attackBytes))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Idempotency-Key", "idor-test-key-1")
	req1.Header.Set("Authorization", "Bearer "+aliceToken)

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusForbidden {
		t.Errorf("CRITICAL SECURITY FLAW: IDOR attack expected 403 Forbidden, got: %d", resp1.StatusCode)
	}

	// 2. IDOR Balance Query: Alice tries to inspect Bob's balance -> MUST BE 403 FORBIDDEN
	req2, _ := http.NewRequest("GET", server.URL+"/api/v1/accounts/ACC-BOB-02/balance", nil)
	req2.Header.Set("Authorization", "Bearer "+aliceToken)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("CRITICAL SECURITY FLAW: IDOR balance check expected 403 Forbidden, got: %d", resp2.StatusCode)
	}
}

func TestGatewayHandler_Security_BFLA_FreezeProtection(t *testing.T) {
	mockClient := &mockLedgerClient{}
	log := logger.New("gateway-test", os.Stdout)
	secret := []byte("test-jwt-secret-signing-key-123456")
	handler := gatewayhttp.NewGatewayHandler(mockClient, nil, log, secret)

	customerClaims := auth.Claims{
		Subject:       "ACC-ALICE-01",
		AccountNumber: "ACC-ALICE-01",
		Role:          auth.RoleCustomer,
		ExpiresAt:     time.Now().Add(1 * time.Hour).Unix(),
	}
	customerToken, _ := auth.SignToken(customerClaims, secret)

	adminClaims := auth.Claims{
		Subject:   "admin-user",
		Role:      auth.RoleAdmin,
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	adminToken, _ := auth.SignToken(adminClaims, secret)

	authMW := auth.RequireAuth(secret)
	adminMW := auth.RequireRole(auth.RoleAdmin)

	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/accounts/{account_number}/freeze", authMW(adminMW(http.HandlerFunc(handler.HandleFreezeAccount))))

	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Normal customer attempts to freeze an account -> MUST BE 403 FORBIDDEN
	req1, _ := http.NewRequest("POST", server.URL+"/api/v1/accounts/ACC-BOB-02/freeze", nil)
	req1.Header.Set("Authorization", "Bearer "+customerToken)

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusForbidden {
		t.Errorf("CRITICAL SECURITY FLAW: BFLA freeze attempt expected 403 Forbidden, got: %d", resp1.StatusCode)
	}

	// 2. Admin attempts to freeze an account -> MUST SUCCEED (200 OK)
	req2, _ := http.NewRequest("POST", server.URL+"/api/v1/accounts/ACC-BOB-02/freeze", nil)
	req2.Header.Set("Authorization", "Bearer "+adminToken)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("admin freeze expected 200 OK, got: %d", resp2.StatusCode)
	}
}
