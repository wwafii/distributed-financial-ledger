package domain

// FraudAction represents the classified decision matrix for a transaction
type FraudAction string

const (
	ActionAllow     FraudAction = "ALLOW"     // Clean transaction: low risk score, proceed directly
	ActionChallenge FraudAction = "CHALLENGE" // Step-up auth required: single large amount (e.g. buying car/property), trigger OTP/Biometrics without freezing
	ActionHold      FraudAction = "HOLD"      // Temporary review hold: rapid cumulative volume reached (e.g. structuring/installment payments), hold without freezing
	ActionFreeze    FraudAction = "FREEZE"    // Emergency mitigation: physical human limit violated (bot/velocity attack), freeze account immediately
)

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

// FraudCheckResult describes the verdict and actionable tier of fraud evaluation
type FraudCheckResult struct {
	Action           FraudAction `json:"action"`
	RiskScore        int         `json:"risk_score"`
	IsFraud          bool        `json:"is_fraud"` // true if Action == ActionFreeze (backward-compatible)
	Reason           string      `json:"reason,omitempty"`
	VelocityCount    int64       `json:"velocity_count"`
	BurstCount       int64       `json:"burst_count,omitempty"`
	CumulativeAmount float64     `json:"cumulative_amount,omitempty"`
	AccountNumber    string      `json:"account_number"`
}

// FraudFlaggedEvent is published when an account is frozen due to bot/velocity attack
type FraudFlaggedEvent struct {
	AccountNumber string      `json:"account_number"`
	Action        FraudAction `json:"action"`
	RiskScore     int         `json:"risk_score"`
	Reason        string      `json:"reason"`
	FlaggedAt     string      `json:"flagged_at"`
	TransactionID string      `json:"transaction_id,omitempty"`
}

// FraudChallengeEvent is published when a large transaction requires 2FA/Biometric step-up verification
type FraudChallengeEvent struct {
	AccountNumber string  `json:"account_number"`
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	ChallengeType string  `json:"challenge_type"` // e.g. "BIOMETRIC_OR_OTP"
	RiskScore     int     `json:"risk_score"`
	Reason        string  `json:"reason"`
	CreatedAt     string  `json:"created_at"`
}

// FraudHoldEvent is published when cumulative transaction volume triggers temporary review hold
type FraudHoldEvent struct {
	AccountNumber    string  `json:"account_number"`
	TransactionID    string  `json:"transaction_id"`
	CumulativeAmount float64 `json:"cumulative_amount"`
	RiskScore        int     `json:"risk_score"`
	Reason           string  `json:"reason"`
	HoldDurationSec  int     `json:"hold_duration_sec"` // e.g. 600 (10 minutes)
	CreatedAt        string  `json:"created_at"`
}
