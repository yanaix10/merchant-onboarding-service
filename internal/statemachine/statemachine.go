package statemachine

import (
	"context"
	"errors"
	"fmt"

	"merchant-onboarding-service/internal/repository"
	"encoding/json"
    "time"
    "github.com/google/uuid"
    "merchant-onboarding-service/internal/events"
)

var (
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrTerminalState     = errors.New("transaction is in a terminal state")
)

type StateMachine struct {
	repo        *repository.MerchantRepository
	transitions map[repository.TransactionStatus]map[repository.TransactionStatus]bool
}

func NewStateMachine(repo *repository.MerchantRepository) *StateMachine {
	sm := &StateMachine{
		repo:        repo,
		transitions: make(map[repository.TransactionStatus]map[repository.TransactionStatus]bool),
	}

	// Define explicitly allowed transitions
	sm.transitions[repository.StatusReceived] = map[repository.TransactionStatus]bool{
		repository.StatusAccepted:     true, // Success path
		repository.StatusFailed:       true, // Immediate business failure
		repository.StatusRetryPending: true, // Immediate technical failure
	}
	sm.transitions[repository.StatusAccepted] = map[repository.TransactionStatus]bool{
		repository.StatusProcessing: true,
	}
	sm.transitions[repository.StatusProcessing] = map[repository.TransactionStatus]bool{
		repository.StatusCompleted:    true,
		repository.StatusFailed:       true,
		repository.StatusRetryPending: true,
	}
	sm.transitions[repository.StatusRetryPending] = map[repository.TransactionStatus]bool{
		repository.StatusRetrying: true,
	}
	sm.transitions[repository.StatusRetrying] = map[repository.TransactionStatus]bool{
		repository.StatusAccepted: true, // Retry succeeded
		repository.StatusFailed:   true, // Retry failed permanently
		repository.StatusTimeout:  true, // Max retries exceeded
	}

	return sm
}

// Protect Terminal States
func IsTerminal(status repository.TransactionStatus) bool {
	return status == repository.StatusCompleted ||
		status == repository.StatusFailed ||
		status == repository.StatusTimeout
}
func (sm *StateMachine) Transition(
	ctx context.Context,
	tx *repository.MerchantEntity,
	next repository.TransactionStatus,
	reason string,
) error {
	current := tx.Status

	if IsTerminal(current) {
		return fmt.Errorf("%w: %s cannot transition to %s", ErrTerminalState, current, next)
	}

	if allowed := sm.transitions[current][next]; !allowed {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current, next)
	}

	// Create Outbox Event
	eventID := "EVT-" + uuid.New().String()
	eventPayload := events.TransactionStatusChangedEvent{
		TrackingID: tx.TrackingID,
		OldStatus:  string(current),
		NewStatus:  string(next),
		Reason:     reason,
		OccurredAt: time.Now(),
	}
	payloadBytes, _ := json.Marshal(eventPayload)

	// Execute Atomic DB Update + Audit Log + Outbox
	err := sm.repo.UpdateStateAndAudit(
		ctx, 
		tx.TrackingID, 
		current, 
		next, 
		reason,
		eventID,
		"TransactionStatusChangedEvent",
		string(payloadBytes),
	)
	if err != nil {
		return err
	}

	tx.Status = next
	return nil
}