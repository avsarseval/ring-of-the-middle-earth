package internal

import (
	"fmt"
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// InitProducer Kafka'ya mesaj gönderecek yapıyı kurar.
func InitProducer(broker string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return nil, fmt.Errorf("producer oluşturulamadı: %w", err)
	}
	return p, nil
}
func InitTransactionalProducer(broker string, transactionalID string) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"transactional.id":  transactionalID,
		"acks":              "all",
		"enable.idempotence":  true,

	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := producer.InitTransactions(ctx); err != nil {
		producer.Close()
		return nil, err
	}

	fmt.Printf("✅ Transactional Kafka Producer hazırlandı | transactional.id=%s\n", transactionalID)
	return producer, nil
}

// ProduceMessage verilen topic'e Kafka mesajı yazar.
func ProduceMessage(producer *kafka.Producer, topic string, payload []byte) error {
	if producer == nil {
		return fmt.Errorf("producer hazır değil")
	}

	deliveryCh := make(chan kafka.Event, 1)
	defer close(deliveryCh)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, deliveryCh)
	if err != nil {
		return fmt.Errorf("mesaj Kafka'ya gönderilemedi: %w", err)
	}

	e := <-deliveryCh
	msg, ok := e.(*kafka.Message)
	if !ok {
		return fmt.Errorf("beklenmeyen Kafka delivery event'i: %v", e)
	}
	if msg.TopicPartition.Error != nil {
		return fmt.Errorf("Kafka delivery hatası: %w", msg.TopicPartition.Error)
	}

	return nil
}


// ProduceMessageWithKey sends a Kafka message with an explicit string key.
// REQUIRED for log-compacted topics (game.session).
// Log compaction rejects null-key messages at the broker level with
// "Broker: Broker failed to validate record". The key identifies which
// record to keep as the "latest" during compaction.
func ProduceMessageWithKey(producer *kafka.Producer, topic string, key string, payload []byte) error {
	if producer == nil {
		return fmt.Errorf("producer hazır değil")
	}

	deliveryCh := make(chan kafka.Event, 1)
	defer close(deliveryCh)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key), // FIX: non-null key required for compact topics
		Value:          payload,
	}, deliveryCh)
	if err != nil {
		return fmt.Errorf("mesaj Kafka'ya gönderilemedi: %w", err)
	}

	e := <-deliveryCh
	msg, ok := e.(*kafka.Message)
	if !ok {
		return fmt.Errorf("beklenmeyen Kafka delivery event'i: %v", e)
	}
	if msg.TopicPartition.Error != nil {
		return fmt.Errorf("Kafka delivery hatası: %w", msg.TopicPartition.Error)
	}

	return nil
}


// RecoverStateFromSession reads the game.session log-compacted topic from the beginning
// using a TEMPORARY, DEDICATED consumer that is NOT part of the main consumer group.
//
// Why a separate consumer:
//   - The main consumer group (game-engine-group-final) uses cooperative rebalancing and
//     should not be joined until recovery is complete.
//   - Using c.Assign() instead of c.Subscribe() bypasses group coordination entirely —
//     this consumer is invisible to the broker's group protocol.
//
// Strategy:
//   1. Assign partition 0 of game.session at OffsetBeginning.
//   2. Poll until we reach the high-water mark (no more messages to read).
//   3. Keep only the last message with key == "game-state" (compaction guarantee:
//      only one such key exists, but we read until EOF to get the absolute latest).
//   4. Return the raw JSON value for the caller to deserialize into WorldState.
//
// Returns (nil, nil) when game.session is empty (fresh game — no recovery needed).
func RecoverStateFromSession(broker string) ([]byte, error) {
	fmt.Println("🔍 KTable Bootstrap: reading game.session for state recovery...")

	// Dedicated consumer — unique group ID so it never interferes with the main group
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           fmt.Sprintf("recovery-bootstrap-%d", timeNowMs()),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // read-only; no offset commits needed
	})
	if err != nil {
		return nil, fmt.Errorf("recovery consumer init failed: %w", err)
	}
	defer c.Close()

	topic := "game.session"
	partition := int32(0)

	// Assign directly — no group join, no rebalance
	if err := c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.OffsetBeginning,
	}}); err != nil {
		return nil, fmt.Errorf("recovery consumer assign failed: %w", err)
	}

	// Get the high-water mark so we know when we have read all committed messages
	_, highWaterMark, err := c.QueryWatermarkOffsets(topic, partition, 5000)
	if err != nil {
		// Topic may not exist yet on a brand-new cluster — treat as empty
		fmt.Printf("⚠️ game.session watermark query failed (new cluster?): %v\n", err)
		return nil, nil
	}
	if highWaterMark == 0 {
		fmt.Println("ℹ️ game.session is empty — starting fresh (turn 1)")
		return nil, nil
	}

	fmt.Printf("📖 game.session high-water mark = %d; reading all records...\n", highWaterMark)

	var latestPayload []byte
	readCount := 0

	for {
		msg, err := c.ReadMessage(3 * time.Second)
		if err != nil {
			// Timeout = no more messages to read
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				break
			}
			fmt.Printf("⚠️ recovery read error: %v\n", err)
			break
		}

		readCount++
		// Only process the "game-state" key (our compaction key)
		if string(msg.Key) == "game-state" {
			latestPayload = msg.Value
			fmt.Printf("   ✓ Found game-state record at offset %d\n", msg.TopicPartition.Offset)
		}

		// Stop once we have consumed up to the high-water mark
		if int64(msg.TopicPartition.Offset)+1 >= highWaterMark {
			break
		}
	}

	fmt.Printf("📖 Recovery read %d records from game.session\n", readCount)

	if latestPayload == nil {
		fmt.Println("ℹ️ No game-state record found — starting fresh")
		return nil, nil
	}

	return latestPayload, nil
}

func timeNowMs() int64 {
	return time.Now().UnixMilli()
}

// InitConsumer Kafka'dan mesaj okuyacak yapıyı kurar.
func InitConsumer(broker, groupID string) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("consumer oluşturulamadı: %w", err)
	}
	return c, nil
}

// StartKafkaConsumer Kafka'daki mesajları dinler ve eventCh'e aktarır.
func StartKafkaConsumer(broker string, groupID string, topics []string, eventCh chan<- Event) {
	c, err := InitConsumer(broker, groupID)
	if err != nil {
		fmt.Printf("❌ Kafka Consumer Hatası: %v\n", err)
		return
	}
	defer c.Close()
	if err := c.SubscribeTopics(topics, func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			fmt.Printf("🔁 Consumer group rebalance: partitions assigned: %v\n", e.Partitions)
			return c.Assign(e.Partitions)

		case kafka.RevokedPartitions:
			fmt.Printf("🔁 Consumer group rebalance: partitions revoked: %v\n", e.Partitions)
			return c.Unassign()

		default:
			fmt.Printf("🔁 Consumer group rebalance event: %v\n", e)
			return nil
		}
	}); err != nil {
		fmt.Printf("❌ Topic aboneliği başarısız: %v\n", err)
		return
	}

	fmt.Printf("🎧 Kafka Dinleyici Başlatıldı: %v topicleri dinleniyor...\n", topics)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("⚠️ Kafka Okuma Hatası: %v\n", err)
			continue
		}

		eventCh <- Event{
			Topic:   *msg.TopicPartition.Topic,
			Payload: msg.Value,
		}
	}
}