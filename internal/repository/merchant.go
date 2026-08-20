package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionStatus string

const (
	StatusReceived     TransactionStatus = "RECEIVED"
	StatusAccepted     TransactionStatus = "ACCEPTED"
	StatusProcessing   TransactionStatus = "PROCESSING"
	StatusCompleted    TransactionStatus = "COMPLETED"
	StatusFailed       TransactionStatus = "FAILED"
	StatusTimeout      TransactionStatus = "TIMEOUT"
	StatusRetrying     TransactionStatus = "RETRYING"
	StatusRetryPending TransactionStatus = "RETRY_PENDING"
)

type MerchantEntity struct {
	ID                int64
	TrackingID        string
	ExternalRequestID *string
	Status            TransactionStatus
	RequestPayload    string
	ResponsePayload   *string
	RetryCount        int
	CreatedAt         time.Time
	UpdatedAt         time.Time
	LastPolledAt      *time.Time
	NextPollAt        *time.Time
	CompletedAt       *time.Time
	NextRetryAt       *time.Time
	LastFailureReason *string
}

type OutboxEntity struct {
	EventID     string
	EventType   string
	AggregateID string
	Payload     string
}

type MerchantRepository struct {
	db *pgxpool.Pool
}

func NewMerchantRepository(db *pgxpool.Pool) *MerchantRepository {
	return &MerchantRepository{db: db}
}

// ------------------------------------------------------------------------
// Core Transaction Methods & State Machine + Outbox Updates
// ------------------------------------------------------------------------

func (r *MerchantRepository) Save(ctx context.Context, m *MerchantEntity, outboxEventID, eventType, payload string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Insert Transaction
	queryTx := `
        INSERT INTO transactions (tracking_id, status, request_payload, retry_count, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6)
    `
	if _, err := tx.Exec(ctx, queryTx, m.TrackingID, m.Status, m.RequestPayload, m.RetryCount, m.CreatedAt, m.UpdatedAt); err != nil {
		return err
	}

	// Insert Outbox Event
	queryOutbox := `
        INSERT INTO outbox_event (event_id, event_type, aggregate_id, payload, status)
        VALUES ($1, $2, $3, $4, 'PENDING')
    `
	if _, err := tx.Exec(ctx, queryOutbox, outboxEventID, eventType, m.TrackingID, payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *MerchantRepository) UpdateStateAndAudit(
	ctx context.Context,
	trackingID string,
	oldStatus TransactionStatus,
	newStatus TransactionStatus,
	reason string,
	outboxEventID string,
	eventType string,
	payload string,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Update Status
	queryUpdate := `UPDATE transactions SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE tracking_id = $2`
	if _, err := tx.Exec(ctx, queryUpdate, newStatus, trackingID); err != nil {
		return err
	}

	// Insert Audit
	queryAudit := `INSERT INTO transaction_state_audit (tracking_id, old_status, new_status, reason) VALUES ($1, $2, $3, $4)`
	if _, err := tx.Exec(ctx, queryAudit, trackingID, oldStatus, newStatus, reason); err != nil {
		return err
	}

	// Insert Outbox Event
	queryOutbox := `
        INSERT INTO outbox_event (event_id, event_type, aggregate_id, payload, status)
        VALUES ($1, $2, $3, $4, 'PENDING')
    `
	if _, err := tx.Exec(ctx, queryOutbox, outboxEventID, eventType, trackingID, payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *MerchantRepository) FindByTrackingID(ctx context.Context, trackingID string) (*MerchantEntity, error) {
	query := `
        SELECT tracking_id, external_request_id, status, request_payload, response_payload, retry_count, created_at, updated_at 
        FROM transactions 
        WHERE tracking_id = $1
    `

	var m MerchantEntity
	err := r.db.QueryRow(ctx, query, trackingID).Scan(
		&m.TrackingID,
		&m.ExternalRequestID,
		&m.Status,
		&m.RequestPayload,
		&m.ResponsePayload,
		&m.RetryCount,
		&m.CreatedAt,
		&m.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (r *MerchantRepository) UpdateProviderDetails(ctx context.Context, trackingID, extID string, status TransactionStatus, respPayload string) error {
	query := `
        UPDATE transactions 
        SET external_request_id = $1, status = $2, response_payload = $3, updated_at = CURRENT_TIMESTAMP
        WHERE tracking_id = $4
    `
	_, err := r.db.Exec(ctx, query, extID, status, respPayload, trackingID)
	return err
}

// ------------------------------------------------------------------------
// Polling Engine Methods
// ------------------------------------------------------------------------

func (r *MerchantRepository) FindEligibleForPolling(ctx context.Context) ([]MerchantEntity, error) {
	query := `
        SELECT tracking_id, external_request_id, status, created_at 
        FROM transactions 
        WHERE status IN ('ACCEPTED', 'PROCESSING') 
          AND (next_poll_at <= CURRENT_TIMESTAMP OR next_poll_at IS NULL)
        FOR UPDATE SKIP LOCKED
    `

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MerchantEntity
	for rows.Next() {
		var m MerchantEntity
		if err := rows.Scan(&m.TrackingID, &m.ExternalRequestID, &m.Status, &m.CreatedAt); err == nil {
			results = append(results, m)
		}
	}
	return results, nil
}

func (r *MerchantRepository) UpdatePollingStatus(ctx context.Context, trackingID string, status TransactionStatus, nextPoll *time.Time, completedAt *time.Time) error {
	query := `
        UPDATE transactions 
        SET status = $1, last_polled_at = CURRENT_TIMESTAMP, next_poll_at = $2, completed_at = $3, updated_at = CURRENT_TIMESTAMP
        WHERE tracking_id = $4
    `
	_, err := r.db.Exec(ctx, query, status, nextPoll, completedAt, trackingID)
	return err
}

// ------------------------------------------------------------------------
// Webhook Framework Methods
// ------------------------------------------------------------------------

func (r *MerchantRepository) FindByExternalRequestID(ctx context.Context, extID string) (*MerchantEntity, error) {
	query := `
        SELECT tracking_id, external_request_id, status 
        FROM transactions 
        WHERE external_request_id = $1
    `

	var m MerchantEntity
	err := r.db.QueryRow(ctx, query, extID).Scan(
		&m.TrackingID,
		&m.ExternalRequestID,
		&m.Status,
	)

	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (r *MerchantRepository) SaveWebhookAudit(ctx context.Context, eventID string, payload string) error {
	query := `INSERT INTO webhook_audit (event_id, payload) VALUES ($1, $2)`
	_, err := r.db.Exec(ctx, query, eventID, payload)
	return err
}

func (r *MerchantRepository) UpdateWebhookStatus(ctx context.Context, trackingID string, status TransactionStatus, completedAt *time.Time) error {
	query := `
        UPDATE transactions 
        SET status = $1, completed_at = $2, updated_at = CURRENT_TIMESTAMP
        WHERE tracking_id = $3
    `
	_, err := r.db.Exec(ctx, query, status, completedAt, trackingID)
	return err
}

// ------------------------------------------------------------------------
// Retry Scheduler Methods
// ------------------------------------------------------------------------

func (r *MerchantRepository) FindPendingRetries(ctx context.Context) ([]MerchantEntity, error) {
	query := `
        SELECT tracking_id, request_payload, retry_count 
        FROM transactions 
        WHERE status = 'RETRY_PENDING' 
          AND next_retry_at <= CURRENT_TIMESTAMP
        FOR UPDATE SKIP LOCKED
    `
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MerchantEntity
	for rows.Next() {
		var m MerchantEntity
		if err := rows.Scan(&m.TrackingID, &m.RequestPayload, &m.RetryCount); err == nil {
			results = append(results, m)
		}
	}
	return results, nil
}

func (r *MerchantRepository) MarkForRetry(ctx context.Context, trackingID string, status TransactionStatus, newRetryCount int, nextRetry *time.Time, reason string) error {
	query := `
        UPDATE transactions 
        SET status = $1, retry_count = $2, next_retry_at = $3, last_failure_reason = $4, updated_at = CURRENT_TIMESTAMP
        WHERE tracking_id = $5
    `
	_, err := r.db.Exec(ctx, query, status, newRetryCount, nextRetry, reason, trackingID)
	return err
}

func (r *MerchantRepository) SaveRetryAudit(ctx context.Context, trackingID string, attempt int, result, reason string) error {
	query := `
        INSERT INTO retry_audit (tracking_id, attempt_number, result, failure_reason)
        VALUES ($1, $2, $3, $4)
    `
	_, err := r.db.Exec(ctx, query, trackingID, attempt, result, reason)
	return err
}

// ------------------------------------------------------------------------
// Outbox Publisher Methods
// ------------------------------------------------------------------------

func (r *MerchantRepository) FindPendingOutboxEvents(ctx context.Context) ([]OutboxEntity, error) {
	query := `
        SELECT event_id, event_type, aggregate_id, payload 
        FROM outbox_event 
        WHERE status = 'PENDING' 
        ORDER BY created_at ASC 
        LIMIT 100
        FOR UPDATE SKIP LOCKED
    `
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []OutboxEntity
	for rows.Next() {
		var e OutboxEntity
		if err := rows.Scan(&e.EventID, &e.EventType, &e.AggregateID, &e.Payload); err == nil {
			results = append(results, e)
		}
	}
	return results, nil
}

func (r *MerchantRepository) UpdateOutboxStatus(ctx context.Context, eventID string, status string) error {
	query := `
        UPDATE outbox_event 
        SET status = $1, published_at = CURRENT_TIMESTAMP 
        WHERE event_id = $2
    `
	_, err := r.db.Exec(ctx, query, status, eventID)
	return err
}