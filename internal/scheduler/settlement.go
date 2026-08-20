package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"	

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/ledger"
	"merchant-onboarding-service/internal/repository"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type SettlementEngine struct {
	settlementRepo *repository.SettlementRepository
	ledgerService  *ledger.LedgerService
	bankClient     *client.BankClient
	cron           *cron.Cron
}

func NewSettlementEngine(
	lc fx.Lifecycle,
	sRepo *repository.SettlementRepository,
	ls *ledger.LedgerService,
	bank *client.BankClient,
) *SettlementEngine {
	engine := &SettlementEngine{
		settlementRepo: sRepo,
		ledgerService:  ls,
		bankClient:     bank,
		cron:           cron.New(),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// In production, this is usually "0 1 * * *" (1 AM Daily for T+1)
			// Using every minute for testing.
			_, err := engine.cron.AddFunc("@every 1m", engine.ProcessDailySettlement)
			engine.cron.Start()
			return err
		},
		OnStop: func(ctx context.Context) error {
			engine.cron.Stop()
			return nil
		},
	})

	return engine
}

func (e *SettlementEngine) ProcessDailySettlement() {
	ctx := context.Background()
	log.Println("[SETTLEMENT ENGINE] Starting settlement batch run...")

	// 1. Find all merchants with a positive payable balance (Source of truth: Ledger)
	balances, err := e.settlementRepo.FindEligibleBalances(ctx)
	if err != nil {
		log.Printf("[SETTLEMENT ENGINE] Failed to fetch balances: %v", err)
		return
	}

	for _, bal := range balances {
		e.processMerchantSettlement(ctx, bal)
	}
	log.Println("[SETTLEMENT ENGINE] Batch run completed.")
}

func (e *SettlementEngine) processMerchantSettlement(ctx context.Context, bal repository.MerchantPayableBalance) {
	merchantID := strings.Replace(bal.AccountID, "ACC-PAYABLE-", "", 1)
	batchID := "SET-" + time.Now().Format("20060102") + "-" + uuid.New().String()[:8]

	// 2. Settlement Calculation Rules (Example: 2% Fee, 18% GST on Fee, 10% Reserve Holdback)
	gross := bal.Amount
	fee := (gross * 2) / 100
	tax := (fee * 18) / 100
	reserve := (gross * 10) / 100
	net := gross - fee - tax - reserve

	log.Printf("[SETTLEMENT ENGINE] Processing %s | Gross: %d, Fee: %d, Tax: %d, Reserve: %d, Net: %d", merchantID, gross, fee, tax, reserve, net)

	// 3. Create Settlement Batch Record
	if err := e.settlementRepo.CreateBatch(ctx, batchID, merchantID, gross, fee, tax, reserve, net); err != nil {
		log.Printf("Failed to create batch for %s: %v", merchantID, err)
		return
	}

	// 4. Generate Double-Entry Journal (Zeroing out the Payable account)
	entries := []ledger.LedgerEntry{
		{AccountID: bal.AccountID, EntryType: ledger.Debit, Amount: gross},           // Clear Platform Liability
		{AccountID: "ACC-REVENUE-FEES", EntryType: ledger.Credit, Amount: fee},        // Gateway Revenue
		{AccountID: "ACC-TAX-PAYABLE", EntryType: ledger.Credit, Amount: tax},         // Government Liability
		{AccountID: "ACC-RESERVE-HOLDBACK", EntryType: ledger.Credit, Amount: reserve},// Risk Holdback
		{AccountID: "ACC-BANK-SETTLEMENT", EntryType: ledger.Credit, Amount: net},     // Actual cash out the door
	}

	if err := e.ledgerService.Post(ctx, batchID, "DAILY_SETTLEMENT", entries); err != nil {
		log.Printf("FATAL: Ledger post failed for %s. Halting settlement. Err: %v", batchID, err)
		e.settlementRepo.UpdateBatchStatus(ctx, batchID, repository.SettlementFailed)
		e.settlementRepo.LogException(ctx, batchID, fmt.Sprintf("Ledger Failure: %v", err))
		return
	}

	// 5. Initiate Real Bank Transfer (Using BatchID as Idempotency Key)
	req := client.BankTransferRequest{
		MerchantAccountID: merchantID,
		Amount:            net,
		Currency:          "INR",
	}

	if err := e.bankClient.Transfer(ctx, batchID, req); err != nil {
		// Rule #1: Payment Success != Settlement Success
		log.Printf("Bank transfer failed for %s: %v. Moving to Retry Queue.", batchID, err)
		e.settlementRepo.UpdateBatchStatus(ctx, batchID, repository.SettlementRetryPending)
		e.settlementRepo.LogException(ctx, batchID, fmt.Sprintf("Bank API Failure: %v", err))
		return
	}

	// 6. Mark Completed
	e.settlementRepo.UpdateBatchStatus(ctx, batchID, repository.SettlementCompleted)
	log.Printf("[SETTLEMENT ENGINE] Successfully settled batch %s for merchant %s", batchID, merchantID)
}