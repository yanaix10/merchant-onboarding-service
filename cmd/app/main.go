package main

import (
	"context"
	"net/http"

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/config"
	"merchant-onboarding-service/internal/handlers"
	"merchant-onboarding-service/internal/messaging"
	"merchant-onboarding-service/internal/repository"
	"merchant-onboarding-service/internal/scheduler"
	"merchant-onboarding-service/internal/statemachine"
	"merchant-onboarding-service/internal/saga"
	"merchant-onboarding-service/internal/ledger"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
)

func NewGinEngine() *gin.Engine {
	r := gin.Default()
	return r
}

func RegisterRoutes(r *gin.Engine, h *handlers.MerchantHandler, w *handlers.WebhookHandler) {
	r.POST("/api/v1/merchant/onboard", h.Onboard)
	r.GET("/api/v1/transactions/:trackingId", h.GetStatus)
	r.POST("/api/v1/webhooks/provider", w.HandleProviderCallback)
}

func StartHTTPServer(lc fx.Lifecycle, r *gin.Engine) {
	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					panic(err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}

func main() {
	fx.New(
		fx.Provide(
			NewGinEngine,
			config.NewDatabasePool,
			messaging.NewKafkaPublisher, 

			ledger.NewLedgerRepository,
			ledger.NewLedgerService,


			messaging.NewKafkaConsumer,
			client.NewProviderClient,
			client.NewBankClient,

			repository.NewIdempotencyRepository,
			repository.NewMerchantRepository,
			repository.NewSagaRepository,
			repository.NewReconciliationRepository,
			repository.NewSettlementRepository,

			statemachine.NewStateMachine,
			saga.NewOnboardingSagaOrchestrator,
			handlers.NewMerchantHandler,
			handlers.NewWebhookHandler,
			scheduler.NewPollingService,
			scheduler.NewRetryService,
			scheduler.NewOutboxService,
			scheduler.NewReconciliationService,
			scheduler.NewSettlementEngine,
		),
		fx.Invoke(
			RegisterRoutes,
			StartHTTPServer,
			func(*scheduler.PollingService) {},
			func(*scheduler.RetryService) {},
			func(*scheduler.OutboxService) {},
			func(*messaging.KafkaConsumer) {},
			func(*scheduler.ReconciliationService) {},
			func(*scheduler.SettlementEngine) {},
		),
	).Run()
}