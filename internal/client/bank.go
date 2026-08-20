package client

import (
	"context"
	"errors"
	"log"
	"time"
)

type BankTransferRequest struct {
	MerchantAccountID string
	Amount            int64
	Currency          string
}

type BankClient struct {
	// In reality, this would contain HTTP clients, auth tokens, etc.
}

func NewBankClient() *BankClient {
	return &BankClient{}
}

// Transfer simulates a payout to a merchant's real-world bank account
func (c *BankClient) Transfer(ctx context.Context, idempotencyKey string, req BankTransferRequest) error {
	log.Printf("[BANK API] Initiating Transfer: %d paise to %s (IdempotencyKey: %s)", req.Amount, req.MerchantAccountID, idempotencyKey)

	// Simulate Network Latency
	time.Sleep(500 * time.Millisecond)

	// Simulate a random bank failure for robustness testing (e.g., if amount ends in 99)
	if req.Amount%100 == 99 {
		return errors.New("bank network timeout - transfer status unknown")
	}

	log.Printf("[BANK API] Transfer Successful: %s", idempotencyKey)
	return nil
}