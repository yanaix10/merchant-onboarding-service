package messaging

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"merchant-onboarding-service/internal/ledger"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	db       *pgxpool.Pool
	ledger   *ledger.LedgerService
}

func NewKafkaConsumer(lc fx.Lifecycle, db *pgxpool.Pool ,ls *ledger.LedgerService) *KafkaConsumer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "fintech-notification-group", // Consumer Group
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	kc := &KafkaConsumer{
		consumer: c,
		db:       db,
		ledger:   ls,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Run the consumer in the background
			go kc.startConsuming()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return kc.consumer.Close()
		},
	})

	return kc
}

func (kc *KafkaConsumer) startConsuming() {
	kc.consumer.SubscribeTopics([]string{"transaction-events"}, nil)
	log.Println("Kafka Consumer started, waiting for events...")

	for {
		msg, err := kc.consumer.ReadMessage(-1)
		if err == nil {
			kc.processMessage(msg)
		} else {
			log.Printf("Kafka consumer error: %v", err)
		}
	}
}

func (kc *KafkaConsumer) processMessage(msg *kafka.Message) {
	type KafkaMessage struct {
		EventID   string          `json:"eventId"`
		EventType string          `json:"eventType"`
		Payload   json.RawMessage `json:"payload"`
	}

	var parsedMsg KafkaMessage
	if err := json.Unmarshal(msg.Value, &parsedMsg); err != nil {
		log.Printf("Failed to parse message, sending to DLQ: %v", err)
		// In a production system, you would publish this raw message to a Dead Letter Queue (DLQ) here.
		return
	}

	ctx := context.Background()

	// ---------------------------------------------------------
	// ENTERPRISE IDEMPOTENCY: The Processing Ledger Pattern
	// ---------------------------------------------------------
	
	// Step 1: Start a Database Transaction
	tx, err := kc.db.Begin(ctx)
	if err != nil {
		log.Printf("DB connection error: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Step 2: Attempt to insert into the processing_ledger
	// If the event_id already exists, Postgres returns 0 affected rows.
	tag, err := tx.Exec(ctx, `
		INSERT INTO processing_ledger (event_id, business_key, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (event_id) DO NOTHING
	`, parsedMsg.EventID, string(msg.Key), "PROCESSED")

	if err != nil {
		log.Printf("Error checking ledger: %v", err)
		return
	}

	// Step 3: Duplicate Detection Check
	if tag.RowsAffected() == 0 {
		log.Printf("DUPLICATE DETECTED: Event %s was already processed. Skipping safely.", parsedMsg.EventID)
		return // We exit early. Kafka can redeliver 100 times, and it will never execute the business logic twice.
	}

	// Step 4: Execute Business Logic
	// (e.g., Sending an email, triggering the next Saga step, updating read models)
	log.Printf("Processing Business Logic for Event: %s (Type: %s, Aggregate: %s)", parsedMsg.EventID, parsedMsg.EventType, string(msg.Key))

	if parsedMsg.EventType == "TransactionStatusChangedEvent" {
		// Mock logic: deduct a 5000 paise (50.00 INR) onboarding fee
		entries := []ledger.LedgerEntry{
			{AccountID: "ACC-MERCHANT-WALLET", EntryType: ledger.Debit, Amount: 5000},
			{AccountID: "ACC-ONBOARDING-REVENUE", EntryType: ledger.Credit, Amount: 4500},
			{AccountID: "ACC-TAX-PAYABLE", EntryType: ledger.Credit, Amount: 500},
		}

		err = kc.ledger.Post(ctx, string(msg.Key), "ONBOARDING_FEE", entries)
		if err != nil {
			log.Printf("FATAL LEDGER ERROR: %v", err)
			tx.Rollback(ctx) // Rollback the processing ledger if accounting fails
			return
		}
		log.Printf("Successfully posted double-entry journal for %s", string(msg.Key))
	}

	// Step 5: Commit the Transaction (Ledger record + Business updates saved atomically)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	log.Printf("Successfully processed and recorded event: %s", parsedMsg.EventID)
}