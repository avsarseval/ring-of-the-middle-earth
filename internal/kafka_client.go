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