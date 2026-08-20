package events

import "time"

// TransactionCreatedEvent represents the initial creation fact
type TransactionCreatedEvent struct {
	TrackingID string    `json:"trackingId"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurredAt"`
}

// TransactionStatusChangedEvent represents any subsequent state transition fact
type TransactionStatusChangedEvent struct {
	TrackingID string    `json:"trackingId"`
	OldStatus  string    `json:"oldStatus"`
	NewStatus  string    `json:"newStatus"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurredAt"`
}