package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidToken   = errors.New("invalid or malformed auth token")
	ErrExpiredToken   = errors.New("auth token has expired")
	ErrInvalidSignature = errors.New("token signature verification failed")
	ErrMissingAuth    = errors.New("missing Authorization header")
	ErrForbidden      = errors.New("access forbidden: insufficient role permissions")
)

const (
	RoleCustomer = "customer"
	RoleAdmin    = "admin"
	RoleService  = "service"
)

type contextKey string

const claimsContextKey contextKey = "auth_claims"

// Claims represents the cryptographic JWT payload for authenticated fintech users
type Claims struct {
	Subject       string `json:"sub"`            // e.g. user ID or principal
	AccountNumber string `json:"acc,omitempty"`  // associated financial account number
	Role          string `json:"role"`           // customer, admin, or service
	IssuedAt      int64  `json:"iat"`            // Unix timestamp
	ExpiresAt     int64  `json:"exp"`            // Unix timestamp
}

// Valid checks if the token expiration timestamp is in the future
func (c *Claims) Valid() error {
	if c.ExpiresAt > 0 && time.Now().Unix() > c.ExpiresAt {
		return ErrExpiredToken
	}
	return nil
}

// SignToken generates an HMAC-SHA256 signed JWT token string
func SignToken(claims Claims, secret []byte) (string, error) {
	if len(secret) == 0 {
		return "", errors.New("jwt secret cannot be empty")
	}

	headerJSON := `{"alg":"HS256","typ":"JWT"}`
	headerB64 := base64.RawURLEncoding.EncodeToString([]byte(headerJSON))

	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("failed to marshal claims: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	unsignedToken := headerB64 + "." + payloadB64

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsignedToken))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return unsignedToken + "." + sigB64, nil
}

// ParseToken validates the HMAC-SHA256 signature and decodes the Claims
func ParseToken(tokenStr string, secret []byte) (*Claims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	unsignedToken := parts[0] + "." + parts[1]
	providedSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidToken
	}

	// Verify HMAC-SHA256 signature in constant time
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsignedToken))
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(providedSig, expectedSig) {
		return nil, ErrInvalidSignature
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, ErrInvalidToken
	}

	if err := claims.Valid(); err != nil {
		return nil, err
	}

	return &claims, nil
}

// WithClaims injects authenticated claims into context
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// FromContext extracts Claims from request context, returning nil if unauthenticated
func FromContext(ctx context.Context) *Claims {
	if ctx == nil {
		return nil
	}
	if claims, ok := ctx.Value(claimsContextKey).(*Claims); ok {
		return claims
	}
	return nil
}
