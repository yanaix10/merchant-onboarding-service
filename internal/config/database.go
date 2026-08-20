package config

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func NewDatabasePool(lc fx.Lifecycle) *pgxpool.Pool {
	connStr := "postgres://postgres:password@localhost:5432/onboarding?sslmode=disable"
	
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("Unable to parse database URL: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			err := pool.Ping(ctx)
			if err != nil {
				return err
			}
			return idxInitTables(ctx, pool)
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})

	return pool
}

func idxInitTables(ctx context.Context, pool *pgxpool.Pool) error {
	// 1. Transactions Table
	query1 := `
	CREATE TABLE IF NOT EXISTS transactions (
		id BIGSERIAL PRIMARY KEY,
		tracking_id VARCHAR(50) UNIQUE NOT NULL,
		external_request_id VARCHAR(100),
		status VARCHAR(30) NOT NULL,
		request_payload TEXT,
		response_payload TEXT,
		retry_count INTEGER DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		last_polled_at TIMESTAMP,
		next_poll_at TIMESTAMP,
		completed_at TIMESTAMP,
		next_retry_at TIMESTAMP,
		last_failure_reason VARCHAR(500)
	);`

	// 2. Webhook Audit Table
	query2 := `
	CREATE TABLE IF NOT EXISTS webhook_audit (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(100) UNIQUE NOT NULL,
		payload TEXT,
		received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	// 3. New Idempotency Record Table
	query3 := `
	CREATE TABLE IF NOT EXISTS idempotency_record (
		id BIGSERIAL PRIMARY KEY,
		idempotency_key VARCHAR(100) UNIQUE NOT NULL,
		tracking_id VARCHAR(100),
		request_hash VARCHAR(255),
		response_payload TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	query4 := `
	CREATE TABLE IF NOT EXISTS retry_audit (
		id BIGSERIAL PRIMARY KEY,
		tracking_id VARCHAR(100),
		attempt_number INTEGER,
		attempt_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		result VARCHAR(50),
		failure_reason VARCHAR(500)
	);`
	query5 := `
	CREATE TABLE IF NOT EXISTS transaction_state_audit (
		id BIGSERIAL PRIMARY KEY,
		tracking_id VARCHAR(100),
		old_status VARCHAR(50),
		new_status VARCHAR(50),
		transition_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		reason VARCHAR(500)
	);`
	query6 := `
	CREATE TABLE IF NOT EXISTS outbox_event (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(100) UNIQUE NOT NULL,
		event_type VARCHAR(100) NOT NULL,
		aggregate_id VARCHAR(100) NOT NULL,
		payload JSONB NOT NULL,
		status VARCHAR(30) NOT NULL,
		event_version INTEGER DEFAULT 1,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		published_at TIMESTAMP
	);`

	query7 := `
	CREATE TABLE IF NOT EXISTS saga_instance (
		saga_id VARCHAR(100) PRIMARY KEY,
		tracking_id VARCHAR(100) UNIQUE NOT NULL,
		current_step VARCHAR(100) NOT NULL,
		status VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	query8 := `
	CREATE TABLE IF NOT EXISTS saga_audit (
		id BIGSERIAL PRIMARY KEY,
		saga_id VARCHAR(100) NOT NULL,
		step_name VARCHAR(100) NOT NULL,
		action VARCHAR(50) NOT NULL,
		status VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	query9 := `
	CREATE TABLE IF NOT EXISTS processing_ledger (
		event_id VARCHAR(100) PRIMARY KEY,
		business_key VARCHAR(100) NOT NULL,
		status VARCHAR(50) NOT NULL,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	query10 := `
	CREATE TABLE IF NOT EXISTS reconciliation_result (
		id BIGSERIAL PRIMARY KEY,
		tracking_id VARCHAR(100) NOT NULL,
		internal_status VARCHAR(50),
		external_status VARCHAR(50),
		result VARCHAR(50) NOT NULL,
		remarks VARCHAR(500),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	query11 := `
	CREATE TABLE IF NOT EXISTS reconciliation_exception (
		id BIGSERIAL PRIMARY KEY,
		tracking_id VARCHAR(100) NOT NULL,
		issue_type VARCHAR(100) NOT NULL,
		status VARCHAR(50) DEFAULT 'PENDING_REVIEW',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	query12 := `
	CREATE TABLE IF NOT EXISTS ledger_account (
		account_id VARCHAR(100) PRIMARY KEY,
		account_name VARCHAR(100) NOT NULL,
		account_type VARCHAR(50) NOT NULL,
		currency VARCHAR(10) NOT NULL,
		status VARCHAR(20) DEFAULT 'ACTIVE',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	// 13. Journal Entry Table (The Business Event)
	query13 := `
	CREATE TABLE IF NOT EXISTS journal_entry (
		journal_id VARCHAR(100) PRIMARY KEY,
		tracking_id VARCHAR(100) NOT NULL,
		transaction_type VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	// 14. Ledger Entry Table (Immutable movements)
	query14 := `
	CREATE TABLE IF NOT EXISTS ledger_entry (
		entry_id VARCHAR(100) PRIMARY KEY,
		journal_id VARCHAR(100) REFERENCES journal_entry(journal_id),
		account_id VARCHAR(100) REFERENCES ledger_account(account_id),
		entry_type VARCHAR(20) NOT NULL,
		amount BIGINT NOT NULL, -- Stored in lowest denomination (paise/cents)
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	// 15. Account Balance Table (Derived Snapshot for fast reads)
	query15 := `
	CREATE TABLE IF NOT EXISTS account_balance (
		account_id VARCHAR(100) PRIMARY KEY REFERENCES ledger_account(account_id),
		current_balance BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	query16 := `
	CREATE TABLE IF NOT EXISTS settlement_batch (
		batch_id VARCHAR(100) PRIMARY KEY,
		merchant_id VARCHAR(100) NOT NULL,
		gross_amount BIGINT NOT NULL,
		fee_amount BIGINT NOT NULL,
		tax_amount BIGINT NOT NULL,
		reserve_amount BIGINT NOT NULL,
		net_amount BIGINT NOT NULL,
		status VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	// 17. Settlement Exception Table
	query17 := `
	CREATE TABLE IF NOT EXISTS settlement_exception (
		exception_id BIGSERIAL PRIMARY KEY,
		batch_id VARCHAR(100) NOT NULL,
		reason VARCHAR(500) NOT NULL,
		status VARCHAR(50) DEFAULT 'PENDING_REVIEW',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	queries := []string{query1, query2, query3, query4 , query5,query6 , query7 , query8,query9 , query10 , query11 ,
		 query12,query13,query14,query15 , query16 , query17}
	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil

}