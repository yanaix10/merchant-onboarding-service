package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ReconStatus string

const (
	ReconMatched       ReconStatus = "MATCHED"
	ReconMismatched    ReconStatus = "MISMATCHED"
	ReconAutoCorrected ReconStatus = "AUTO_CORRECTED"
	ReconManualReview  ReconStatus = "MANUAL_REVIEW"
)

type ReconciliationRepository struct {
	db *pgxpool.Pool
}

func NewReconciliationRepository(db *pgxpool.Pool) *ReconciliationRepository {
	return &ReconciliationRepository{db: db}
}

// FindStuckTransactions gets transactions that have been PROCESSING/ACCEPTED for over 30 mins
func (r *ReconciliationRepository) FindStuckTransactions(ctx context.Context) ([]MerchantEntity, error) {
	query := `
		SELECT tracking_id, external_request_id, status 
		FROM transactions 
		WHERE status IN ('ACCEPTED', 'PROCESSING') 
		  AND updated_at < CURRENT_TIMESTAMP - INTERVAL '30 minutes'
	`
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MerchantEntity
	for rows.Next() {
		var m MerchantEntity
		if err := rows.Scan(&m.TrackingID, &m.ExternalRequestID, &m.Status); err == nil {
			results = append(results, m)
		}
	}
	return results, nil
}

func (r *ReconciliationRepository) SaveResult(ctx context.Context, trackingID string, internal, external string, result ReconStatus, remarks string) error {
	query := `
		INSERT INTO reconciliation_result (tracking_id, internal_status, external_status, result, remarks)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.Exec(ctx, query, trackingID, internal, external, result, remarks)
	return err
}

func (r *ReconciliationRepository) SaveException(ctx context.Context, trackingID, issueType string) error {
	query := `
		INSERT INTO reconciliation_exception (tracking_id, issue_type)
		VALUES ($1, $2)
	`
	_, err := r.db.Exec(ctx, query, trackingID, issueType)
	return err
}