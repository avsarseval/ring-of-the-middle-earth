package internal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type BroadcastWorldStateSnapshot struct {
	EventType string          `json:"eventType"`
	Turn      int             `json:"turn"`
	State     WorldStateCache `json:"state"`
	Timestamp int64           `json:"timestamp"`
}

// PublishWorldStateSnapshot turn sonunda WorldStateSnapshot event'ini game.broadcast topic'ine yazar.
func PublishWorldStateSnapshot(producer *kafka.Producer) {
	worldStateMu.RLock()
	stateCopy := WorldState
	worldStateMu.RUnlock()

	snapshot := BroadcastWorldStateSnapshot{
		EventType: "WorldStateSnapshot",
		Turn:      GetCurrentTurn(),
		State:     stateCopy,
		Timestamp: time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Printf("❌ WorldStateSnapshot JSON oluşturulamadı: %v\n", err)
		return
	}

	if err := ProduceMessage(producer, TopicBroadcast, payload); err != nil {
		fmt.Printf("❌ WorldStateSnapshot Kafka'ya yazılamadı: %v\n", err)
		return
	}

	fmt.Printf("📡 WorldStateSnapshot game.broadcast topic'ine yazıldı | turn=%d\n", GetCurrentTurn())
}