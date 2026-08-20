package ledger

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LedgerRepository struct {
	db *pgxpool.Pool
}

func NewLedgerRepository(db *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{db: db}
}

// CreateAccount sets up a new ledger account
func (r *LedgerRepository) CreateAccount(ctx context.Context, acc LedgerAccount) error {
	query := `INSERT INTO ledger_account (account_id, account_name, account_type, currency) VALUES ($1, $2, $3, $4)`
	_, err := r.db.Exec(ctx, query, acc.AccountID, acc.AccountName, acc.AccountType, acc.Currency)
	return err
}

// PostJournal atomically saves the journal, immutable entries, and updates the balance snapshot
func (r *LedgerRepository) PostJournal(ctx context.Context, j *Journal) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Insert Journal
	qJournal := `INSERT INTO journal_entry (journal_id, tracking_id, transaction_type) VALUES ($1, $2, $3)`
	if _, err := tx.Exec(ctx, qJournal, j.JournalID, j.TrackingID, j.TransactionType); err != nil {
		return fmt.Errorf("failed to insert journal: %w", err)
	}

	// 2. Insert Entries & Update Balances
	qEntry := `INSERT INTO ledger_entry (entry_id, journal_id, account_id, entry_type, amount) VALUES ($1, $2, $3, $4, $5)`
	
	// Fast-read snapshot upsert
	qBalance := `
		INSERT INTO account_balance (account_id, current_balance) 
		VALUES ($1, $2)
		ON CONFLICT (account_id) DO UPDATE 
		SET current_balance = account_balance.current_balance + EXCLUDED.current_balance, updated_at = CURRENT_TIMESTAMP
	`

	for _, entry := range j.Entries {
		entryID := "ENT-" + uuid.New().String()

		// Insert Immutable Ledger Entry
		if _, err := tx.Exec(ctx, qEntry, entryID, j.JournalID, entry.AccountID, entry.EntryType, entry.Amount); err != nil {
			return fmt.Errorf("failed to insert entry for account %s: %w", entry.AccountID, err)
		}

		// Calculate balance impact (For simplicity here: Credits add to balance, Debits subtract)
		var impact int64 = entry.Amount
		if entry.EntryType == Debit {
			impact = -entry.Amount
		}

		// Update Derived Balance
		if _, err := tx.Exec(ctx, qBalance, entry.AccountID, impact); err != nil {
			return fmt.Errorf("failed to update balance snapshot for %s: %w", entry.AccountID, err)
		}
	}

	return tx.Commit(ctx)
}