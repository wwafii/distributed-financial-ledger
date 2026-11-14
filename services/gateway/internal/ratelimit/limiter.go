package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	rdb        *redis.Client
	limit      int           // max requests per window
	window     time.Duration // sliding window duration
	memMu      sync.Mutex
	memStorage map[string][]time.Time // fallback in-memory sliding window
}

func NewLimiter(rdb *redis.Client, limit int, window time.Duration) *Limiter {
	if limit <= 0 {
		limit = 120 // default 120 requests
	}
	if window <= 0 {
		window = 1 * time.Minute
	}
	return &Limiter{
		rdb:        rdb,
		limit:      limit,
		window:     window,
		memStorage: make(map[string][]time.Time),
	}
}

func getClientIP(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		clientIP := strings.TrimSpace(ips[0])
		if clientIP != "" {
			return clientIP
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

// Allow checks if the request from the client is within the allowed rate limit
func (l *Limiter) Allow(ctx context.Context, clientKey string) (bool, int, time.Duration, error) {
	now := time.Now()

	// 1. If Redis is available, perform Redis sliding window ZSET
	if l.rdb != nil {
		redisKey := fmt.Sprintf("ratelimit:%s", clientKey)
		nowMillis := now.UnixMilli()
		windowStart := nowMillis - l.window.Milliseconds()

		pipe := l.rdb.TxPipeline()
		pipe.ZRemRangeByScore(ctx, redisKey, "-inf", fmt.Sprintf("%d", windowStart))
		pipe.ZAdd(ctx, redisKey, redis.Z{Score: float64(nowMillis), Member: fmt.Sprintf("%d", now.UnixNano())})
		cardCmd := pipe.ZCard(ctx, redisKey)
		pipe.Expire(ctx, redisKey, l.window*2)

		_, err := pipe.Exec(ctx)
		if err == nil {
			count := int(cardCmd.Val())
			remaining := l.limit - count
			if remaining < 0 {
				remaining = 0
			}
			allowed := count <= l.limit
			return allowed, remaining, l.window, nil
		}
		// Redis error: fall through to in-memory fallback
	}

	// 2. In-memory sliding window fallback
	l.memMu.Lock()
	defer l.memMu.Unlock()

	cutoff := now.Add(-l.window)
	timestamps := l.memStorage[clientKey]

	// Filter out expired timestamps
	valid := make([]time.Time, 0, len(timestamps))
	for _, t := range timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	valid = append(valid, now)
	l.memStorage[clientKey] = valid

	count := len(valid)
	remaining := l.limit - count
	if remaining < 0 {
		remaining = 0
	}
	allowed := count <= l.limit

	return allowed, remaining, l.window, nil
}

// Middleware returns an HTTP handler middleware enforcing IP/Token rate limits
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Whitelist health and metrics endpoints from rate limiting
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		clientKey := getClientIP(r)

		allowed, remaining, retryAfter, _ := l.Allow(r.Context(), clientKey)

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(l.limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "Too Many Requests: rate limit exceeded. Please retry later.",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}
