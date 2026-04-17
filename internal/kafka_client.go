package internal

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// InitProducer Kafka'ya mesaj (event) gönderecek yapıyı kurar
func InitProducer(broker string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return nil, fmt.Errorf("Producer oluşturulamadı: %w", err)
	}
	return p, nil
}

// InitConsumer Kafka'dan mesaj (order) okuyacak yapıyı kurar
func InitConsumer(broker, groupID string) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("Consumer oluşturulamadı: %w", err)
	}
	return c, nil
}
