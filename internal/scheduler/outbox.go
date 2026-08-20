package scheduler

import (
	"context"
	"encoding/json"
	"log"

	"merchant-onboarding-service/internal/messaging"
	"merchant-onboarding-service/internal/repository"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type OutboxService struct {
	repo     *repository.MerchantRepository
	cron     *cron.Cron
	kafkaPub *messaging.KafkaPublisher
}

func NewOutboxService(lc fx.Lifecycle, repo *repository.MerchantRepository, kafkaPub *messaging.KafkaPublisher) *OutboxService {
	s := &OutboxService{
		repo:     repo,
		cron:     cron.New(),
		kafkaPub: kafkaPub,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Run every 10 seconds to publish events quickly
			_, err := s.cron.AddFunc("@every 10s", s.publishEvents)
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

func (s *OutboxService) publishEvents() {
	ctx := context.Background()
	events, err := s.repo.FindPendingOutboxEvents(ctx)
	if err != nil {
		log.Printf("Error fetching outbox events: %v", err)
		return
	}

	for _, event := range events {
		// 1. Create Enterprise Kafka Payload
		type KafkaMessage struct {
			EventID   string          `json:"eventId"`
			EventType string          `json:"eventType"`
			Payload   json.RawMessage `json:"payload"`
		}

		msg := KafkaMessage{
			EventID:   event.EventID,
			EventType: event.EventType,
			Payload:   json.RawMessage(event.Payload),
		}
		msgBytes, _ := json.Marshal(msg)

		// 2. Publish to Kafka (Using AggregateID as the Partition Key)
		err := s.kafkaPub.Publish(event.AggregateID, string(msgBytes))
		
		if err != nil {
			log.Printf("Kafka Publish Failed for Event %s: %v. Will retry later.", event.EventID, err)
			continue // Skip updating the DB status; it remains PENDING.
		}

		// 3. Update Database Status on Success
		err = s.repo.UpdateOutboxStatus(ctx, event.EventID, "PUBLISHED")
		if err != nil {
			log.Printf("Failed to mark event %s as published in DB: %v", event.EventID, err)
		}
	}
}