package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SagaStatus string

const (
	SagaStarted      SagaStatus = "STARTED"
	SagaInProgress   SagaStatus = "IN_PROGRESS"
	SagaCompleted    SagaStatus = "COMPLETED"
	SagaCompensating SagaStatus = "COMPENSATING"
	SagaCompensated  SagaStatus = "COMPENSATED"
	SagaFailed       SagaStatus = "FAILED"
)

type SagaAction string

const (
	ActionForward    SagaAction = "FORWARD"
	ActionCompensate SagaAction = "COMPENSATE"
)

type SagaEntity struct {
	SagaID      string
	TrackingID  string
	CurrentStep string
	Status      SagaStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type SagaRepository struct {
	db *pgxpool.Pool
}

func NewSagaRepository(db *pgxpool.Pool) *SagaRepository {
	return &SagaRepository{db: db}
}

// CreateSaga initializes the saga state atomically
func (r *SagaRepository) CreateSaga(ctx context.Context, sagaID, trackingID, initialStep string) error {
	query := `
		INSERT INTO saga_instance (saga_id, tracking_id, current_step, status)
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.db.Exec(ctx, query, sagaID, trackingID, initialStep, SagaStarted)
	return err
}

// AdvanceSaga updates the step, audits the action, and publishes an Outbox command atomically
func (r *SagaRepository) AdvanceSaga(
	ctx context.Context,
	sagaID string,
	newStatus SagaStatus,
	stepName string,
	action SagaAction,
	actionStatus string,
	outboxEventID string,
	commandType string,
	commandPayload string,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Update Saga State
	updateQuery := `UPDATE saga_instance SET current_step = $1, status = $2, updated_at = CURRENT_TIMESTAMP WHERE saga_id = $3`
	if _, err := tx.Exec(ctx, updateQuery, stepName, newStatus, sagaID); err != nil {
		return err
	}

	// 2. Insert Audit Log
	auditQuery := `INSERT INTO saga_audit (saga_id, step_name, action, status) VALUES ($1, $2, $3, $4)`
	if _, err := tx.Exec(ctx, auditQuery, sagaID, stepName, action, actionStatus); err != nil {
		return err
	}

	// 3. Insert Outbox Command (To communicate with other microservices via Kafka)
	if outboxEventID != "" {
		outboxQuery := `INSERT INTO outbox_event (event_id, event_type, aggregate_id, payload, status) VALUES ($1, $2, $3, $4, 'PENDING')`
		if _, err := tx.Exec(ctx, outboxQuery, outboxEventID, commandType, sagaID, commandPayload); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *SagaRepository) FindByTrackingID(ctx context.Context, trackingID string) (*SagaEntity, error) {
	query := `SELECT saga_id, tracking_id, current_step, status FROM saga_instance WHERE tracking_id = $1`
	var s SagaEntity
	err := r.db.QueryRow(ctx, query, trackingID).Scan(&s.SagaID, &s.TrackingID, &s.CurrentStep, &s.Status)
	if err != nil {
		return nil, err
	}
	return &s, nil
}