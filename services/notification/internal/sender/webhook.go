package sender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RetryableError indicates a transient failure that should be retried (e.g., HTTP 5xx, timeout)
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// NonRetryableError indicates a permanent failure (e.g., HTTP 4xx, invalid URL)
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return fmt.Sprintf("non-retryable error: %v", e.Err)
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable determines whether an error is transient and eligible for backoff retry
func IsRetryable(err error) bool {
	var rErr *RetryableError
	return errors.As(err, &rErr)
}

// WebhookClient abstracts the HTTP dispatcher for mockability and testing
type WebhookClient interface {
	Send(ctx context.Context, targetURL string, payload []byte) error
}

// HTTPWebhookSender implements WebhookClient using a standard Go http.Client
type HTTPWebhookSender struct {
	client *http.Client
}

// NewHTTPWebhookSender creates a new HTTPWebhookSender with bounded timeout and tuned connection pool
func NewHTTPWebhookSender(timeout time.Duration) *HTTPWebhookSender {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	// Production-grade Transport: Prevents socket churn and TIME_WAIT exhaustion under load
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	}
	return &HTTPWebhookSender{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

// Send dispatches a JSON POST to targetURL and classifies errors into Retryable vs NonRetryable
func (s *HTTPWebhookSender) Send(ctx context.Context, targetURL string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return &NonRetryableError{Err: fmt.Errorf("failed to create request: %w", err)}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "DistributedLedger-WebhookWorker/1.0")

	resp, err := s.client.Do(req)
	if err != nil {
		// Network errors, connection refused, context deadline exceeded are all retryable
		return &RetryableError{Err: fmt.Errorf("network dispatch failure: %w", err)}
	}
	defer resp.Body.Close()

	bodySnippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// 5xx Server Errors (internal server error, bad gateway, gateway timeout) are transient
	if resp.StatusCode >= 500 {
		return &RetryableError{
			Err: fmt.Errorf("server error status %d: %s", resp.StatusCode, string(bodySnippet)),
		}
	}

	// 4xx Client Errors (bad request, unauthorized, not found) are permanent
	return &NonRetryableError{
		Err: fmt.Errorf("client error status %d: %s", resp.StatusCode, string(bodySnippet)),
	}
}
