package domain

// NotificationPayload represents the structure sent to external webhooks
type NotificationPayload struct {
	EventID        string  `json:"event_id"`
	TransactionID  string  `json:"transaction_id"`
	ReferenceID    string  `json:"reference_id"`
	SourceAccount  string  `json:"source_account"`
	DestinationAcc string  `json:"destination_account"`
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	Description    string  `json:"description"`
	Timestamp      string  `json:"timestamp"`
}

// DLQMessage wraps a message that permanently failed downstream delivery
type DLQMessage struct {
	OriginalTopic string `json:"original_topic"`
	OriginalKey   string `json:"original_key"`
	Payload       string `json:"payload"`
	ErrorReason   string `json:"error_reason"`
	RetryCount    int    `json:"retry_count"`
	FailedAt      string `json:"failed_at"`
}
