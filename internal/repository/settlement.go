package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SettlementStatus string

const (
	SettlementCreated      SettlementStatus = "CREATED"
	SettlementProcessing   SettlementStatus = "PROCESSING"
	SettlementCompleted    SettlementStatus = "COMPLETED"
	SettlementFailed       SettlementStatus = "FAILED"
	SettlementRetryPending SettlementStatus = "RETRY_PENDING"
)

type MerchantPayableBalance struct {
	AccountID string
	Amount    int64
}

type SettlementRepository struct {
	db *pgxpool.Pool
}

func NewSettlementRepository(db *pgxpool.Pool) *SettlementRepository {
	return &SettlementRepository{db: db}
}

// FindEligibleBalances queries the derived Ledger Balance table for positive payables
func (r *SettlementRepository) FindEligibleBalances(ctx context.Context) ([]MerchantPayableBalance, error) {
	// We only settle accounts prefixed with ACC-PAYABLE- that have money owed to them
	query := `
		SELECT account_id, current_balance 
		FROM account_balance 
		WHERE account_id LIKE 'ACC-PAYABLE-%' AND current_balance > 0
	`
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []MerchantPayableBalance
	for rows.Next() {
		var b MerchantPayableBalance
		if err := rows.Scan(&b.AccountID, &b.Amount); err == nil {
			balances = append(balances, b)
		}
	}
	return balances, nil
}

func (r *SettlementRepository) CreateBatch(
	ctx context.Context, batchID, merchantID string,
	gross, fee, tax, reserve, net int64,
) error {
	query := `
		INSERT INTO settlement_batch (batch_id, merchant_id, gross_amount, fee_amount, tax_amount, reserve_amount, net_amount, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.db.Exec(ctx, query, batchID, merchantID, gross, fee, tax, reserve, net, SettlementProcessing)
	return err
}

func (r *SettlementRepository) UpdateBatchStatus(ctx context.Context, batchID string, status SettlementStatus) error {
	query := `UPDATE settlement_batch SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE batch_id = $2`
	_, err := r.db.Exec(ctx, query, status, batchID)
	return err
}

func (r *SettlementRepository) LogException(ctx context.Context, batchID, reason string) error {
	query := `INSERT INTO settlement_exception (batch_id, reason) VALUES ($1, $2)`
	_, err := r.db.Exec(ctx, query, batchID, reason)
	return err
}