package ratelimit_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/ratelimit"
)

func TestLimiter_InMemorySlidingWindow(t *testing.T) {
	// Limit to 3 requests per 500ms
	limiter := ratelimit.NewLimiter(nil, 3, 500*time.Millisecond)

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := limiter.Middleware(dummyHandler)

	for i := 1; i <= 3; i++ {
		req := httptest.NewRequest("GET", "/api/v1/test", nil)
		req.RemoteAddr = "192.168.1.100:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("request %d expected 200 OK, got: %d", i, rec.Code)
		}
	}

	// 4th request must be rejected with 429 Too Many Requests
	reqBlocked := httptest.NewRequest("GET", "/api/v1/test", nil)
	reqBlocked.RemoteAddr = "192.168.1.100:1234"
	recBlocked := httptest.NewRecorder()
	handler.ServeHTTP(recBlocked, reqBlocked)

	if recBlocked.Code != http.StatusTooManyRequests {
		t.Errorf("4th request expected 429 Too Many Requests, got: %d", recBlocked.Code)
	}

	// Healthz must always be allowed even when limit exceeded
	reqHealth := httptest.NewRequest("GET", "/healthz", nil)
	reqHealth.RemoteAddr = "192.168.1.100:1234"
	recHealth := httptest.NewRecorder()
	handler.ServeHTTP(recHealth, reqHealth)

	if recHealth.Code != http.StatusOK {
		t.Errorf("health check expected 200 OK, got: %d", recHealth.Code)
	}
}
