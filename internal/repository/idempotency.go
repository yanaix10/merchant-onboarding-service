package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type IdempotencyEntity struct {
	ID              int64
	IdempotencyKey  string
	TrackingID      string
	RequestHash     string
	ResponsePayload string
	CreatedAt       time.Time
}

type IdempotencyRepository struct {
	db *pgxpool.Pool
}

func NewIdempotencyRepository(db *pgxpool.Pool) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

func (r *IdempotencyRepository) FindByKey(ctx context.Context, key string) (*IdempotencyEntity, error) {
	query := `
		SELECT idempotency_key, tracking_id, request_hash, response_payload, created_at 
		FROM idempotency_record 
		WHERE idempotency_key = $1
	`
	var entity IdempotencyEntity
	err := r.db.QueryRow(ctx, query, key).Scan(
		&entity.IdempotencyKey,
		&entity.TrackingID,
		&entity.RequestHash,
		&entity.ResponsePayload,
		&entity.CreatedAt,
	)

	if err != nil {
		return nil, err // Will return pgx.ErrNoRows if not found
	}
	return &entity, nil
}

func (r *IdempotencyRepository) Save(ctx context.Context, e *IdempotencyEntity) error {
	query := `
		INSERT INTO idempotency_record (idempotency_key, tracking_id, request_hash, response_payload) 
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.db.Exec(ctx, query, e.IdempotencyKey, e.TrackingID, e.RequestHash, e.ResponsePayload)
	return err // Will return unique constraint error if a race condition occurs
}