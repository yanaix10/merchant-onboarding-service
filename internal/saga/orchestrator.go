package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"merchant-onboarding-service/internal/repository"

	"github.com/google/uuid"
	"go.uber.org/fx"
)

// Define the steps of our enterprise Onboarding Saga
const (
	StepVerifyIdentity   = "VERIFY_IDENTITY"
	StepFraudCheck       = "FRAUD_CHECK"
	StepSetupSettlement  = "SETUP_SETTLEMENT"
	StepSendNotification = "SEND_NOTIFICATION"
)

type OnboardingSagaOrchestrator struct {
	sagaRepo *repository.SagaRepository
}

func NewOnboardingSagaOrchestrator(lc fx.Lifecycle, sagaRepo *repository.SagaRepository) *OnboardingSagaOrchestrator {
	return &OnboardingSagaOrchestrator{
		sagaRepo: sagaRepo,
	}
}

// Start triggered when the initial transaction is created
func (o *OnboardingSagaOrchestrator) Start(ctx context.Context, trackingID string, merchantData map[string]interface{}) error {
	sagaID := "SAGA-" + uuid.New().String()
	
	// 1. Initialize Saga in DB
	if err := o.sagaRepo.CreateSaga(ctx, sagaID, trackingID, StepVerifyIdentity); err != nil {
		return err
	}

	// 2. Dispatch the first command (Verify Identity) via Outbox/Kafka
	commandPayload, _ := json.Marshal(map[string]interface{}{
		"sagaId":     sagaID,
		"trackingId": trackingID,
		"data":       merchantData,
	})

	return o.dispatchCommand(ctx, sagaID, repository.SagaInProgress, StepVerifyIdentity, repository.ActionForward, "VerifyIdentityCommand", string(commandPayload))
}

// OnStepSuccess handles Kafka events emitted by external microservices when a step succeeds
func (o *OnboardingSagaOrchestrator) OnStepSuccess(ctx context.Context, trackingID string, completedStep string) error {
	saga, err := o.sagaRepo.FindByTrackingID(ctx, trackingID)
	if err != nil {
		return err
	}

	// Determine the next step in the workflow
	var nextStep, nextCommand string
	var nextStatus = repository.SagaInProgress

	switch completedStep {
	case StepVerifyIdentity:
		nextStep = StepFraudCheck
		nextCommand = "PerformFraudCheckCommand"
	case StepFraudCheck:
		nextStep = StepSetupSettlement
		nextCommand = "SetupSettlementCommand"
	case StepSetupSettlement:
		nextStep = StepSendNotification
		nextCommand = "SendNotificationCommand"
	case StepSendNotification:
		// The entire Saga is successfully complete
		return o.sagaRepo.AdvanceSaga(ctx, saga.SagaID, repository.SagaCompleted, completedStep, repository.ActionForward, "SUCCESS", "", "", "")
	default:
		return fmt.Errorf("unknown step completed: %s", completedStep)
	}

	// Dispatch the next command
	payload, _ := json.Marshal(map[string]string{"trackingId": trackingID})
	return o.dispatchCommand(ctx, saga.SagaID, nextStatus, nextStep, repository.ActionForward, nextCommand, string(payload))
}

// OnStepFailed triggers the Compensation workflow (Rolling back previous steps)
func (o *OnboardingSagaOrchestrator) OnStepFailed(ctx context.Context, trackingID string, failedStep string, reason string) error {
	saga, err := o.sagaRepo.FindByTrackingID(ctx, trackingID)
	if err != nil {
		return err
	}

	log.Printf("Saga %s failed at step %s. Initiating compensation. Reason: %s", saga.SagaID, failedStep, reason)

	// Determine which compensation to run based on where we failed
	switch failedStep {
	case StepSetupSettlement:
		// Settlement failed -> We must revert the Fraud Check / Identity status
		payload, _ := json.Marshal(map[string]string{"trackingId": trackingID, "reason": "Settlement Failed"})
		return o.dispatchCommand(ctx, saga.SagaID, repository.SagaCompensating, StepFraudCheck, repository.ActionCompensate, "RevertFraudStatus-Command", string(payload))
		
	case StepFraudCheck:
		// Fraud check failed -> We must revert the Identity Verification
		payload, _ := json.Marshal(map[string]string{"trackingId": trackingID, "reason": "Fraud Check Failed"})
		return o.dispatchCommand(ctx, saga.SagaID, repository.SagaCompensating, StepVerifyIdentity, repository.ActionCompensate, "RevertIdentity-Command", string(payload))

	case StepVerifyIdentity:
		// Failed on step 1. Nothing to compensate. Just mark Saga as Failed.
		return o.sagaRepo.AdvanceSaga(ctx, saga.SagaID, repository.SagaFailed, failedStep, repository.ActionForward, "FAILED", "", "", "")
		
	default:
		return o.sagaRepo.AdvanceSaga(ctx, saga.SagaID, repository.SagaFailed, failedStep, repository.ActionForward, "FAILED", "", "", "")
	}
}

// Helper to bundle the DB save and Outbox dispatch
func (o *OnboardingSagaOrchestrator) dispatchCommand(
	ctx context.Context, sagaID string, status repository.SagaStatus, step string, action repository.SagaAction, commandType string, payload string,
) error {
	outboxEventID := "CMD-" + uuid.New().String()
	return o.sagaRepo.AdvanceSaga(ctx, sagaID, status, step, action, "PENDING", outboxEventID, commandType, payload)
}