package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wwafii/distributed-financial-ledger/pkg/auth"
)

var testSecret = []byte("super-secret-production-grade-signing-key-32b")

func TestAuth_SignAndParse_Success(t *testing.T) {
	claims := auth.Claims{
		Subject:       "user-123",
		AccountNumber: "ACC-ALICE-01",
		Role:          auth.RoleCustomer,
		IssuedAt:      time.Now().Unix(),
		ExpiresAt:     time.Now().Add(1 * time.Hour).Unix(),
	}

	token, err := auth.SignToken(claims, testSecret)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	parsed, err := auth.ParseToken(token, testSecret)
	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}

	if parsed.Subject != "user-123" {
		t.Errorf("expected subject user-123, got: %s", parsed.Subject)
	}
	if parsed.AccountNumber != "ACC-ALICE-01" {
		t.Errorf("expected account ACC-ALICE-01, got: %s", parsed.AccountNumber)
	}
	if parsed.Role != auth.RoleCustomer {
		t.Errorf("expected role customer, got: %s", parsed.Role)
	}
}

func TestAuth_TamperedToken_Fails(t *testing.T) {
	claims := auth.Claims{
		Subject:       "user-123",
		AccountNumber: "ACC-ALICE-01",
		Role:          auth.RoleCustomer,
		ExpiresAt:     time.Now().Add(1 * time.Hour).Unix(),
	}

	token, _ := auth.SignToken(claims, testSecret)

	// Tamper token payload or signature
	tampered := token[:len(token)-4] + "xxxx"
	_, err := auth.ParseToken(tampered, testSecret)
	if err == nil {
		t.Errorf("expected signature verification failure for tampered token")
	}
}

func TestAuth_ExpiredToken_Fails(t *testing.T) {
	claims := auth.Claims{
		Subject:   "user-123",
		ExpiresAt: time.Now().Add(-1 * time.Minute).Unix(),
	}

	token, _ := auth.SignToken(claims, testSecret)
	_, err := auth.ParseToken(token, testSecret)
	if err != auth.ErrExpiredToken {
		t.Errorf("expected ErrExpiredToken, got: %v", err)
	}
}

func TestAuth_Middleware(t *testing.T) {
	claims := auth.Claims{
		Subject:       "alice",
		AccountNumber: "ACC-ALICE-01",
		Role:          auth.RoleCustomer,
		ExpiresAt:     time.Now().Add(1 * time.Hour).Unix(),
	}
	validToken, _ := auth.SignToken(claims, testSecret)

	adminClaims := auth.Claims{
		Subject:   "admin",
		Role:      auth.RoleAdmin,
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	adminToken, _ := auth.SignToken(adminClaims, testSecret)

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := auth.FromContext(r.Context())
		if c == nil {
			t.Errorf("expected claims in context")
		}
		w.WriteHeader(http.StatusOK)
	})

	authMW := auth.RequireAuth(testSecret)
	adminMW := auth.RequireRole(auth.RoleAdmin)

	// 1. Unauthenticated -> 401
	req1 := httptest.NewRequest("GET", "/test", nil)
	rec1 := httptest.NewRecorder()
	authMW(testHandler).ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got: %d", rec1.Code)
	}

	// 2. Valid Customer -> 200 on basic auth
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.Header.Set("Authorization", "Bearer "+validToken)
	rec2 := httptest.NewRecorder()
	authMW(testHandler).ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got: %d", rec2.Code)
	}

	// 3. Customer accessing Admin route -> 403 Forbidden
	req3 := httptest.NewRequest("POST", "/admin", nil)
	req3.Header.Set("Authorization", "Bearer "+validToken)
	rec3 := httptest.NewRecorder()
	authMW(adminMW(testHandler)).ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden, got: %d", rec3.Code)
	}

	// 4. Admin accessing Admin route -> 200 OK
	req4 := httptest.NewRequest("POST", "/admin", nil)
	req4.Header.Set("Authorization", "Bearer "+adminToken)
	rec4 := httptest.NewRecorder()
	authMW(adminMW(testHandler)).ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got: %d", rec4.Code)
	}
}
