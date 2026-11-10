package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Status string

const (
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
)

type Record struct {
	Status     Status          `json:"status"`
	StatusCode int             `json:"status_code,omitempty"`
	Body       json.RawMessage `json:"body,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

type Engine struct {
	rdb *redis.Client
}

func NewEngine(rdb *redis.Client) *Engine {
	return &Engine{rdb: rdb}
}

// LockResult indicates the outcome of attempting to acquire an idempotency lock
type LockResult struct {
	Acquired bool
	Record   *Record
}

// AcquireOrGet attempts to atomically claim the idempotency key using SET NX
func (e *Engine) AcquireOrGet(ctx context.Context, key string, inFlightTTL time.Duration) (*LockResult, error) {
	redisKey := fmt.Sprintf("idempotency:%s", key)

	initialRecord := Record{
		Status:    StatusProcessing,
		CreatedAt: time.Now(),
	}
	payload, err := json.Marshal(initialRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize initial record: %w", err)
	}

	// Redis atomic SET NX with TTL
	ok, err := e.rdb.SetNX(ctx, redisKey, payload, inFlightTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("redis setnx error: %w", err)
	}

	if ok {
		// Lock acquired successfully!
		return &LockResult{Acquired: true}, nil
	}

	// Key already exists! Fetch existing record
	data, err := e.rdb.Get(ctx, redisKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Key expired between SetNX and Get, retry acquiring
			return e.AcquireOrGet(ctx, key, inFlightTTL)
		}
		return nil, fmt.Errorf("failed to fetch existing idempotency record: %w", err)
	}

	var existing Record
	if err := json.Unmarshal(data, &existing); err != nil {
		return nil, fmt.Errorf("failed to parse existing record: %w", err)
	}

	return &LockResult{
		Acquired: false,
		Record:   &existing,
	}, nil
}

// SaveResponse stores the final HTTP status code and response body for subsequent retries
func (e *Engine) SaveResponse(ctx context.Context, key string, statusCode int, body []byte, completionTTL time.Duration) error {
	redisKey := fmt.Sprintf("idempotency:%s", key)

	record := Record{
		Status:     StatusCompleted,
		StatusCode: statusCode,
		Body:       body,
		CreatedAt:  time.Now(),
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to serialize completion record: %w", err)
	}

	return e.rdb.Set(ctx, redisKey, payload, completionTTL).Err()
}

// Release removes the key if processing failed early before ledger commit
func (e *Engine) Release(ctx context.Context, key string) error {
	redisKey := fmt.Sprintf("idempotency:%s", key)
	return e.rdb.Del(ctx, redisKey).Err()
}
