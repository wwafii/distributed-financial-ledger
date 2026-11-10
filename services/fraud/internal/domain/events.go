package domain

// TransactionCreatedEvent matches the payload published by Core Ledger Outbox
type TransactionCreatedEvent struct {
	TransactionID      string  `json:"transaction_id"`
	ReferenceID        string  `json:"reference_id"`
	SourceAccount      string  `json:"source_account"`
	DestinationAccount string  `json:"destination_account"`
	Amount             float64 `json:"amount"`
	Currency           string  `json:"currency"`
	Description        string  `json:"description"`
	Timestamp          string  `json:"timestamp"`
}

// FraudCheckResult describes the verdict of fraud evaluation
type FraudCheckResult struct {
	IsFraud       bool   `json:"is_fraud"`
	Reason        string `json:"reason,omitempty"`
	VelocityCount int64  `json:"velocity_count"`
	AccountNumber string `json:"account_number"`
}

// FraudFlaggedEvent is published when an account is flagged / frozen
type FraudFlaggedEvent struct {
	AccountNumber string `json:"account_number"`
	Reason        string `json:"reason"`
	FlaggedAt     string `json:"flagged_at"`
	TransactionID string `json:"transaction_id,omitempty"`
}
