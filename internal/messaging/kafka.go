package messaging

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
)

type KafkaPublisher struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaPublisher(lc fx.Lifecycle) *KafkaPublisher {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"acks":              "all", // Maximum durability: wait for all replicas
		"retries":           3,
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	publisher := &KafkaPublisher{
		producer: p,
		topic:    "transaction-events", // Single domain topic
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			// Flush any pending messages before shutdown
			publisher.producer.Flush(15 * 1000)
			publisher.producer.Close()
			return nil
		},
	})

	return publisher
}

// Publish sends the message to Kafka using the AggregateID to guarantee ordering
func (p *KafkaPublisher) Publish(aggregateID string, payload string) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(aggregateID), // CRITICAL: Ensures ordering per transaction
		Value:          []byte(payload),
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	// Wait for delivery confirmation
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}