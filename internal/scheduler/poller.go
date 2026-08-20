package scheduler

import (
	"context"
	"log"
	"time"

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/repository"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type PollingService struct {
	repo   *repository.MerchantRepository
	client *client.ProviderClient
	cron   *cron.Cron
}

func NewPollingService(lc fx.Lifecycle, repo *repository.MerchantRepository, client *client.ProviderClient) *PollingService {
	s := &PollingService{
		repo:   repo,
		client: client,
		cron:   cron.New(),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Run every 5 minutes
			_, err := s.cron.AddFunc("@every 5m", s.pollPendingTransactions)
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

func (s *PollingService) pollPendingTransactions() {
	ctx := context.Background()
	transactions, err := s.repo.FindEligibleForPolling(ctx)
	if err != nil {
		log.Printf("Error fetching eligible transactions: %v", err)
		return
	}

	for _, tx := range transactions {
		// 1. Timeout Check (> 72 hours)
		age := time.Since(tx.CreatedAt)
		if age.Hours() > 72 {
			s.repo.UpdatePollingStatus(ctx, tx.TrackingID, repository.StatusTimeout, nil, nil)
			continue
		}

		// 2. Fetch Provider Status
		if tx.ExternalRequestID == nil {
			continue
		}
		
		resp, err := s.client.GetStatus(ctx, *tx.ExternalRequestID)
		if err != nil {
			// Technical failure, skip for now, will retry next poll
			continue
		}

		// 3. Map Status
		mappedStatus := mapProviderStatus(resp.Status)

		// 4. Calculate Next Poll or Completion
		var nextPoll *time.Time
		var completedAt *time.Time
		now := time.Now()

		if mappedStatus == repository.StatusCompleted || mappedStatus == repository.StatusFailed {
			completedAt = &now // Final State: Stop polling
		} else {
			np := calculateNextPoll(tx.CreatedAt)
			nextPoll = &np
		}

		// 5. Update Database
		s.repo.UpdatePollingStatus(ctx, tx.TrackingID, mappedStatus, nextPoll, completedAt)
	}
}

// Mapper Layer
func mapProviderStatus(providerStatus string) repository.TransactionStatus {
	switch providerStatus {
	case "APPROVED":
		return repository.StatusCompleted
	case "REJECTED":
		return repository.StatusFailed
	default:
		return repository.StatusProcessing
	}
}

// Dynamic Polling Strategy
func calculateNextPoll(createdAt time.Time) time.Time {
	ageHours := time.Since(createdAt).Hours()
	now := time.Now()

	if ageHours < 2 {
		return now.Add(5 * time.Minute)
	} else if ageHours < 12 {
		return now.Add(15 * time.Minute)
	} else if ageHours < 24 {
		return now.Add(30 * time.Minute)
	}
	return now.Add(1 * time.Hour)
}