package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var (
	ErrUnbalancedJournal = errors.New("ledger violation: total debits do not equal total credits")
	ErrNegativeAmount    = errors.New("ledger violation: amounts must be positive")
)

type LedgerService struct {
	repo *LedgerRepository
}

func NewLedgerService(repo *LedgerRepository) *LedgerService {
	return &LedgerService{repo: repo}
}

// Post ensures mathematical integrity before allowing the database to be touched
func (s *LedgerService) Post(ctx context.Context, trackingID string, txType string, entries []LedgerEntry) error {
	var totalDebit, totalCredit int64

	for _, e := range entries {
		if e.Amount <= 0 {
			return fmt.Errorf("%w: account %s", ErrNegativeAmount, e.AccountID)
		}
		switch e.EntryType {
case Debit:
			totalDebit += e.Amount
		case Credit:
			totalCredit += e.Amount
		}
	}

	// Mathematical FinTech Guardrail
	if totalDebit != totalCredit {
		return fmt.Errorf("%w (Debits: %d, Credits: %d)", ErrUnbalancedJournal, totalDebit, totalCredit)
	}

	journal := &Journal{
		JournalID:       "JRN-" + uuid.New().String(),
		TrackingID:      trackingID,
		TransactionType: txType,
		Entries:         entries,
	}

	return s.repo.PostJournal(ctx, journal)
}