package sender_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wwafii/distributed-financial-ledger/services/notification/internal/sender"
)

func TestHTTPWebhookSender_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json content-type")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"received"}`))
	}))
	defer ts.Close()

	s := sender.NewHTTPWebhookSender(2 * time.Second)
	err := s.Send(context.Background(), ts.URL, []byte(`{"event":"test"}`))
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestHTTPWebhookSender_ServerError_IsRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway) // HTTP 502
		_, _ = w.Write([]byte(`bad gateway upstream`))
	}))
	defer ts.Close()

	s := sender.NewHTTPWebhookSender(2 * time.Second)
	err := s.Send(context.Background(), ts.URL, []byte(`{"event":"test"}`))
	if err == nil {
		t.Fatalf("expected error on HTTP 502, got nil")
	}

	if !sender.IsRetryable(err) {
		t.Errorf("expected error on 502 to be retryable, got: %v", err)
	}
}

func TestHTTPWebhookSender_ClientError_IsNonRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // HTTP 400
		_, _ = w.Write([]byte(`invalid payload`))
	}))
	defer ts.Close()

	s := sender.NewHTTPWebhookSender(2 * time.Second)
	err := s.Send(context.Background(), ts.URL, []byte(`{"invalid":"json"`))
	if err == nil {
		t.Fatalf("expected error on HTTP 400, got nil")
	}

	if sender.IsRetryable(err) {
		t.Errorf("expected HTTP 400 client error NOT to be retryable, but got retryable: %v", err)
	}
}

func TestHTTPWebhookSender_NetworkTimeout_IsRetryable(t *testing.T) {
	// A server that hangs longer than client timeout
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Short timeout of 10ms
	s := sender.NewHTTPWebhookSender(10 * time.Millisecond)
	err := s.Send(context.Background(), ts.URL, []byte(`{"test":true}`))
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	if !sender.IsRetryable(err) {
		t.Errorf("expected network timeout error to be retryable, got: %v", err)
	}
}
