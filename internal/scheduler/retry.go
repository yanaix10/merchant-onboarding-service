package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/repository"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type RetryService struct {
	repo   *repository.MerchantRepository
	client *client.ProviderClient
	cron   *cron.Cron
}

func NewRetryService(lc fx.Lifecycle, repo *repository.MerchantRepository, client *client.ProviderClient) *RetryService {
	s := &RetryService{
		repo:   repo,
		client: client,
		cron:   cron.New(),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			_, err := s.cron.AddFunc("@every 1m", s.processRetries) // Check every minute
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

// Exponential Backoff Logic
func (s *RetryService) nextDelay(retryCount int) time.Time {
	now := time.Now()
	switch retryCount {
	case 1:
		return now.Add(1 * time.Minute)
	case 2:
		return now.Add(5 * time.Minute)
	case 3:
		return now.Add(15 * time.Minute)
	case 4:
		return now.Add(30 * time.Minute)
	default:
		return now.Add(1 * time.Hour)
	}
}

func (s *RetryService) processRetries() {
	ctx := context.Background()
	retries, err := s.repo.FindPendingRetries(ctx)
	if err != nil {
		log.Printf("Error fetching retries: %v", err)
		return
	}

	for _, tx := range retries {
		// 1. Mark as RETRYING
		s.repo.MarkForRetry(ctx, tx.TrackingID, repository.StatusRetrying, tx.RetryCount, nil, "Executing Retry")

		// 2. Deserialize Payload
		var providerReq client.ProviderRequest
		json.Unmarshal([]byte(tx.RequestPayload), &providerReq)

		// 3. Submit
		providerResp, err := s.client.Submit(ctx, &providerReq)
		
		newRetryCount := tx.RetryCount + 1

		if err != nil {
			providerErr, isProviderErr := err.(*client.ProviderError)
			
			// Business Error -> Fail Permanently
			if isProviderErr && !providerErr.IsTechnical {
				s.repo.MarkForRetry(ctx, tx.TrackingID, repository.StatusFailed, newRetryCount, nil, providerErr.Message)
				s.repo.SaveRetryAudit(ctx, tx.TrackingID, newRetryCount, "FAILED", providerErr.Message)
				continue
			}

			// Technical Error -> Max attempts check
			if newRetryCount >= 5 {
				s.repo.MarkForRetry(ctx, tx.TrackingID, repository.StatusTimeout, newRetryCount, nil, "Max retries exceeded")
				s.repo.SaveRetryAudit(ctx, tx.TrackingID, newRetryCount, "TIMEOUT", "Max retries exceeded")
				continue
			}

			// Schedule Next Retry
			nextTime := s.nextDelay(newRetryCount)
			s.repo.MarkForRetry(ctx, tx.TrackingID, repository.StatusRetryPending, newRetryCount, &nextTime, err.Error())
			s.repo.SaveRetryAudit(ctx, tx.TrackingID, newRetryCount, "TECHNICAL_FAILURE", err.Error())
			continue
		}

		// 4. Success Path
		respBytes, _ := json.Marshal(providerResp)
		s.repo.UpdateProviderDetails(ctx, tx.TrackingID, providerResp.RequestID, repository.StatusAccepted, string(respBytes))
		s.repo.SaveRetryAudit(ctx, tx.TrackingID, newRetryCount, "SUCCESS", "Retry successful")
	}
}