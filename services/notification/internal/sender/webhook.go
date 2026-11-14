package sender

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
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

// NonRetryableError indicates a permanent failure (e.g., HTTP 4xx, invalid URL, SSRF violation)
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

// HTTPWebhookSender implements WebhookClient using a tuned Go http.Client with HMAC signing and SSRF validation
type HTTPWebhookSender struct {
	client        *http.Client
	secret        []byte
	allowInternal bool
}

// WebhookSenderOption configures HTTPWebhookSender
type WebhookSenderOption func(*HTTPWebhookSender)

// WithWebhookSecret sets the HMAC-SHA256 shared secret
func WithWebhookSecret(secret []byte) WebhookSenderOption {
	return func(s *HTTPWebhookSender) {
		s.secret = secret
	}
}

// WithAllowInternal toggles whether private/loopback IPs are permitted (for testing/local development)
func WithAllowInternal(allow bool) WebhookSenderOption {
	return func(s *HTTPWebhookSender) {
		s.allowInternal = allow
	}
}

// NewHTTPWebhookSender creates a new HTTPWebhookSender with bounded timeout, connection pool, and options
func NewHTTPWebhookSender(timeout time.Duration, opts ...WebhookSenderOption) *HTTPWebhookSender {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	secret := []byte(os.Getenv("WEBHOOK_SECRET"))
	if len(secret) == 0 {
		secret = []byte("default-webhook-shared-secret-key")
	}

	// In containerized/local environment, allow internal by default unless STRICT_SSRF_ENABLED=true
	allowInternal := true
	if os.Getenv("STRICT_SSRF_ENABLED") == "true" {
		allowInternal = false
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	}

	sender := &HTTPWebhookSender{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		secret:        secret,
		allowInternal: allowInternal,
	}

	for _, opt := range opts {
		opt(sender)
	}

	return sender
}

// isPrivateIP checks if an IP belongs to RFC1918, loopback, or cloud link-local metadata (169.254.169.254)
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return true
	}
	// Cloud metadata 169.254.169.254 check
	if ip.Equal(net.ParseIP("169.254.169.254")) {
		return true
	}
	return false
}

// validateURL checks for SSRF vulnerabilities against dangerous IP addresses
func (s *HTTPWebhookSender) validateURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme: %s (only http and https allowed)", parsed.Scheme)
	}

	if s.allowInternal {
		return nil
	}

	host := parsed.Hostname()
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("failed to resolve host %s: %w", host, err)
	}

	for _, ip := range ips {
		if isPrivateIP(ip) {
			return fmt.Errorf("SSRF protection: webhook target %s resolves to restricted private IP %s", host, ip.String())
		}
	}

	return nil
}

// ComputeSignature calculates HMAC-SHA256 for the webhook payload
func ComputeSignature(payload []byte, timestamp int64, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// Send dispatches a JSON POST to targetURL with HMAC-SHA256 signature and SSRF validation
func (s *HTTPWebhookSender) Send(ctx context.Context, targetURL string, payload []byte) error {
	// 1. SSRF Safety Check
	if err := s.validateURL(targetURL); err != nil {
		return &NonRetryableError{Err: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return &NonRetryableError{Err: fmt.Errorf("failed to create request: %w", err)}
	}

	// 2. Cryptographic HMAC-SHA256 Signing (Stripe/GitHub Webhook Standard)
	nowUnix := time.Now().Unix()
	signature := ComputeSignature(payload, nowUnix, s.secret)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "DistributedLedger-WebhookWorker/1.0")
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(nowUnix, 10))
	req.Header.Set("X-Signature-SHA256", fmt.Sprintf("t=%d,v1=%s", nowUnix, signature))

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
