package internal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// BroadcastWorldStateSnapshot is the full world state broadcast payload.
type BroadcastWorldStateSnapshot struct {
	EventType string          `json:"eventType"`
	Turn      int             `json:"turn"`
	State     WorldStateCache `json:"state"`
	Timestamp int64           `json:"timestamp"`
}

// BuildWorldStateSnapshot creates a snapshot struct without publishing it.
// Used by HTTP endpoints and the coordinator's broadcastWorldState helper.
func BuildWorldStateSnapshot() BroadcastWorldStateSnapshot {
	worldStateMu.RLock()
	stateCopy := copyWorldState(WorldState)
	worldStateMu.RUnlock()

	return BroadcastWorldStateSnapshot{
		EventType: "WorldStateSnapshot",
		Turn:      GetCurrentTurn(),
		State:     stateCopy,
		Timestamp: time.Now().UnixMilli(),
	}
}

// PublishWorldStateSnapshot marshals the snapshot and writes it to game.broadcast.
// Called at the end of each turn by RunFullTurnProcessing.
func PublishWorldStateSnapshot(producer *kafka.Producer) {
	snapshot := BuildWorldStateSnapshot()

	payload, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Printf("❌ WorldStateSnapshot marshal error: %v\n", err)
		return
	}

	if err := ProduceMessage(producer, TopicBroadcast, payload); err != nil {
		fmt.Printf("❌ WorldStateSnapshot write error: %v\n", err)
		return
	}

	fmt.Printf("📡 WorldStateSnapshot → game.broadcast (turn=%d)\n", GetCurrentTurn())
}
