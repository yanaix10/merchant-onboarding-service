package scheduler

import (
	"context"
	"log"

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/repository"
	"merchant-onboarding-service/internal/statemachine"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type ReconciliationService struct {
	merchantRepo *repository.MerchantRepository
	reconRepo    *repository.ReconciliationRepository
	client       *client.ProviderClient
	stateMachine *statemachine.StateMachine
	cron         *cron.Cron
}

func NewReconciliationService(
	lc fx.Lifecycle,
	merchantRepo *repository.MerchantRepository,
	reconRepo *repository.ReconciliationRepository,
	client *client.ProviderClient,
	stateMachine *statemachine.StateMachine,
) *ReconciliationService {
	s := &ReconciliationService{
		merchantRepo: merchantRepo,
		reconRepo:    reconRepo,
		client:       client,
		stateMachine: stateMachine,
		cron:         cron.New(),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Run External Reconciliation every hour in production
			// (Set to 1 minute for local testing)
			_, err := s.cron.AddFunc("@every 1h", s.PerformExternalReconciliation)
			s.cron.Start()
			return err
		},
		OnStop: func(ctx context.Context) error {
			s.cron.Stop()
			return nil
		},
	})

	return s
}

func (s *ReconciliationService) PerformExternalReconciliation() {
	ctx := context.Background()
	log.Println("[RECONCILIATION] Starting External Recon Job...")

	// 1. Fetch transactions stuck in non-terminal states
	stuckTxs, err := s.reconRepo.FindStuckTransactions(ctx)
	if err != nil {
		log.Printf("[RECONCILIATION] Failed to fetch stuck transactions: %v", err)
		return
	}

	for _, tx := range stuckTxs {
		if tx.ExternalRequestID == nil || *tx.ExternalRequestID == "" {
			continue // Cannot reconcile without provider reference
		}

		// 2. Fetch Truth from Provider
		providerStatus, err := s.client.GetStatus(ctx, *tx.ExternalRequestID)
		if err != nil {
			log.Printf("[RECONCILIATION] Provider API failed for %s: %v", tx.TrackingID, err)
			continue
		}

		internalState := string(tx.Status)
		externalState := providerStatus.Status

		// 3. Comparison Logic
		if s.isStatusMatch(internalState, externalState) {
			s.reconRepo.SaveResult(ctx, tx.TrackingID, internalState, externalState, repository.ReconMatched, "Status in sync")
			continue
		}

		// 4. Auto-Healing Logic (Safe to correct statuses based on Provider truth)
		if externalState == "SUCCESS" {
			log.Printf("[RECONCILIATION] Auto-Healing %s to COMPLETED", tx.TrackingID)
			
			// We MUST use the StateMachine so the Audit Log and Outbox Events are generated correctly!
			err := s.stateMachine.Transition(ctx, &tx, repository.StatusCompleted, "Auto-healed via External Reconciliation")
			if err != nil {
				log.Printf("Failed to auto-heal %s: %v", tx.TrackingID, err)
				continue
			}
			s.reconRepo.SaveResult(ctx, tx.TrackingID, internalState, externalState, repository.ReconAutoCorrected, "Healed stuck transaction to COMPLETED")

		} else if externalState == "FAILED" {
			log.Printf("[RECONCILIATION] Auto-Healing %s to FAILED", tx.TrackingID)
			
			err := s.stateMachine.Transition(ctx, &tx, repository.StatusFailed, "Auto-healed via External Reconciliation")
			if err == nil {
				s.reconRepo.SaveResult(ctx, tx.TrackingID, internalState, externalState, repository.ReconAutoCorrected, "Healed stuck transaction to FAILED")
			}
			
		} else {
			// 5. Unsafe / Unknown mismatch -> Exception Queue
			log.Printf("[RECONCILIATION] Mismatch requires manual review for %s", tx.TrackingID)
			s.reconRepo.SaveResult(ctx, tx.TrackingID, internalState, externalState, repository.ReconManualReview, "Status mismatch not safe for auto-healing")
			s.reconRepo.SaveException(ctx, tx.TrackingID, "STATUS_MISMATCH")
		}
	}
	
	log.Println("[RECONCILIATION] Job Completed.")
}

func (s *ReconciliationService) isStatusMatch(internal string, external string) bool {
	// Map external provider terminology to our internal state machine
	if internal == "COMPLETED" && external == "SUCCESS" {
		return true
	}
	if internal == "FAILED" && external == "FAILED" {
		return true
	}
	if (internal == "PROCESSING" || internal == "ACCEPTED") && (external == "PROCESSING" || external == "PENDING") {
		return true
	}
	return false
}