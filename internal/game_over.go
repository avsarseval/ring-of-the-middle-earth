package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type GameOverEvent struct {
	EventType string `json:"eventType"`
	GameID    string `json:"gameId"`
	Winner    string `json:"winner"`
	Cause     string `json:"cause"`
	Turn      int    `json:"turn"`
	Timestamp int64  `json:"timestamp"`
}

func gameOverKafkaKey(gameID string, winner string, cause string) string {
	return fmt.Sprintf("%s|%s|%s", gameID, winner, cause)
}

// PublishGameOverTransactionally publishes GameOver inside a Kafka transaction.
// This is used for K6 exactly-once demo.
func PublishGameOverTransactionally(
	producer *kafka.Producer,
	gameID string,
	winner string,
	cause string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	event := GameOverEvent{
		EventType: "GameOver",
		GameID:    gameID,
		Winner:    winner,
		Cause:     cause,
		Turn:      GetCurrentTurn(),
		Timestamp: time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("game over marshal error: %w", err)
	}

	key := []byte(gameOverKafkaKey(gameID, winner, cause))

	if err := producer.BeginTransaction(); err != nil {
		return fmt.Errorf("begin transaction error: %w", err)
	}

	topic := TopicBroadcast

	deliveryChan := make(chan kafka.Event, 1)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: payload,
	}, deliveryChan)

	if err != nil {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("produce game over error: %w", err)
	}

	deliveryEvent := <-deliveryChan
	close(deliveryChan)

	msg, ok := deliveryEvent.(*kafka.Message)
	if !ok {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("unexpected delivery event: %v", deliveryEvent)
	}

	if msg.TopicPartition.Error != nil {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("delivery error: %w", msg.TopicPartition.Error)
	}

	if err := producer.CommitTransaction(ctx); err != nil {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("commit transaction error: %w", err)
	}

	fmt.Printf("🏁 Transactional GameOver committed | key=%s winner=%s cause=%s\n",
		string(key),
		winner,
		cause,
	)

	return nil
}

// Demo helper: starts a transaction, produces GameOver, then aborts it.
// Consumers with read_committed should not see this record.
func PublishGameOverAndAbortForDemo(
	producer *kafka.Producer,
	gameID string,
	winner string,
	cause string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	event := GameOverEvent{
		EventType: "GameOver",
		GameID:    gameID,
		Winner:    winner,
		Cause:     cause,
		Turn:      GetCurrentTurn(),
		Timestamp: time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("game over marshal error: %w", err)
	}

	key := []byte(gameOverKafkaKey(gameID, winner, cause))
	topic := TopicBroadcast

	if err := producer.BeginTransaction(); err != nil {
		return fmt.Errorf("begin transaction error: %w", err)
	}

	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: payload,
	}, deliveryChan)

	if err != nil {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("produce aborted demo game over error: %w", err)
	}

	deliveryEvent := <-deliveryChan
	close(deliveryChan)

	msg, ok := deliveryEvent.(*kafka.Message)
	if !ok {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("unexpected delivery event: %v", deliveryEvent)
	}

	if msg.TopicPartition.Error != nil {
		_ = producer.AbortTransaction(ctx)
		return fmt.Errorf("delivery error: %w", msg.TopicPartition.Error)
	}

	if err := producer.AbortTransaction(ctx); err != nil {
		return fmt.Errorf("abort transaction error: %w", err)
	}

	fmt.Printf("🧪 Aborted GameOver transaction | key=%s\n", string(key))
	return nil
}
