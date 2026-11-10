package domain

import (
	"time"

	"github.com/google/uuid"
)

type AccountStatus string

const (
	AccountStatusActive AccountStatus = "ACTIVE"
	AccountStatusFrozen AccountStatus = "FROZEN"
	AccountStatusClosed AccountStatus = "CLOSED"
)

type Account struct {
	ID            uuid.UUID     `json:"id"`
	AccountNumber string        `json:"account_number"`
	HolderName    string        `json:"holder_name"`
	Balance       float64       `json:"balance"`
	Currency      string        `json:"currency"`
	Status        AccountStatus `json:"status"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

type EntryDirection string

const (
	DirectionDebit  EntryDirection = "DEBIT"
	DirectionCredit EntryDirection = "CREDIT"
)

type Entry struct {
	ID            uuid.UUID      `json:"id"`
	TransactionID uuid.UUID      `json:"transaction_id"`
	AccountID     uuid.UUID      `json:"account_id"`
	Direction     EntryDirection `json:"direction"`
	Amount        float64        `json:"amount"`
	CreatedAt     time.Time      `json:"created_at"`
}

type TransactionStatus string

const (
	TransactionStatusPending TransactionStatus = "PENDING"
	TransactionStatusPosted  TransactionStatus = "POSTED"
	TransactionStatusFailed  TransactionStatus = "FAILED"
)

type Transaction struct {
	ID          uuid.UUID         `json:"id"`
	ReferenceID string            `json:"reference_id"`
	Description string            `json:"description"`
	Status      TransactionStatus `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	Entries     []Entry           `json:"entries,omitempty"`
}

type OutboxEventStatus string

const (
	OutboxStatusPending   OutboxEventStatus = "PENDING"
	OutboxStatusPublished OutboxEventStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxEventStatus = "FAILED"
)

type OutboxEvent struct {
	ID            uuid.UUID         `json:"id"`
	EventType     string            `json:"event_type"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID   string            `json:"aggregate_id"`
	Payload       []byte            `json:"payload"`
	Status        OutboxEventStatus `json:"status"`
	RetryCount    int               `json:"retry_count"`
	CreatedAt     time.Time         `json:"created_at"`
	PublishedAt   *time.Time        `json:"published_at,omitempty"`
}

// TransferCommand represents the domain intent to transfer funds
type TransferCommand struct {
	ReferenceID      string  `json:"reference_id"`
	SourceAccount    string  `json:"source_account"`
	DestinationAcc   string  `json:"destination_account"`
	Amount           float64 `json:"amount"`
	Currency         string  `json:"currency"`
	Description      string  `json:"description"`
}
