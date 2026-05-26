package internal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const HiddenUntilTurn = 3

type RingDetectionEvent struct {
	EventType string `json:"eventType"`
	RegionID  string `json:"regionId"`
	Turn      int    `json:"turn"`
	Timestamp int64  `json:"timestamp"`
}

// RunDetection turn sonunda Ring Bearer detection kontrolünü çalıştırır.
func RunDetection(producer *kafka.Producer) {
	turn := GetCurrentTurn()

	if turn <= HiddenUntilTurn {
		fmt.Printf("🕶️ Detection kapalı. Hidden start aktif: turn=%d\n", turn)
		return
	}

	worldStateMu.RLock()

	ringBearerRegion := WorldState.LightView.RingBearerRegion
	if ringBearerRegion == "" {
		if rb, ok := WorldState.Units[RingBearerID]; ok {
			ringBearerRegion = rb.Region
		}
	}

	units := make([]UnitSnapshot, 0, len(WorldState.Units))
	for _, unit := range WorldState.Units {
		units = append(units, unit)
	}

	worldStateMu.RUnlock()

	if ringBearerRegion == "" {
		fmt.Println("⚠️ Detection yapılamadı: Ring Bearer region bilinmiyor")
		return
	}

	sauronAmplifier := isSauronAmplifierActive()
	detected := false

	for _, unit := range units {
		// FIX B1: config-driven check — no class name string in logic.
		// Q&A Q1: "show where Nazgul detection range is applied — no 'witch-king' string"
		config, ok := getUnitConfig(unit.ID)
		if !ok || config.DetectionRange == 0 || unit.Status != "ACTIVE" {
			continue
		}

		effectiveRange := config.DetectionRange
		if sauronAmplifier {
			effectiveRange++
		}

		distance := graphDistance(unit.Region, ringBearerRegion)

		fmt.Printf(
			"🔎 Detection kontrolü | nazgul=%s region=%s rb=%s distance=%d range=%d\n",
			unit.ID,
			unit.Region,
			ringBearerRegion,
			distance,
			effectiveRange,
		)

		if distance >= 0 && distance <= effectiveRange {
			detected = true
			break
		}
	}

	if !detected {
		fmt.Println("✅ Detection sonucu: Ring Bearer bulunamadı")
		return
	}

	event := RingDetectionEvent{
		EventType: "RingBearerDetected",
		RegionID:  ringBearerRegion,
		Turn:      turn,
		Timestamp: time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("❌ Detection event JSON oluşturulamadı: %v\n", err)
		return
	}

	if err := ProduceMessage(producer, TopicRingDetection, payload); err != nil {
		fmt.Printf("❌ Detection event Kafka'ya yazılamadı: %v\n", err)
		return
	}

	updateDarkDetectionView(ringBearerRegion, turn)

	fmt.Printf("🚨 Ring Bearer detected! region=%s turn=%d → %s\n",
		ringBearerRegion,
		turn,
		TopicRingDetection,
	)
}

// isSauronAmplifierActive Sauron'un pasif etkisini config-driven'a yakın şekilde kontrol eder.
// Shadow tarafında, Maia class'ında, ACTIVE ve Mordor'da olan unit varsa amplifier aktif sayılır.
// Bu prototipte Sauron zaten Mordor'da başlayan tek Shadow Maia olarak kabul edilir.
func isSauronAmplifierActive() bool {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	// FIX B1 / Q&A Q5: config-driven amplifier check.
	// Sauron is the only unit with AbilityEffect=="SAURON_AMPLIFIER".
	// No unit ID or class name string appears here.
	for unitID, unit := range WorldState.Units {
		cfg, ok := WorldState.UnitConfigs[unitID]
		if ok && cfg.AbilityEffect == "SAURON_AMPLIFIER" &&
			unit.Region == "mordor" &&
			unit.Status == "ACTIVE" {
			return true
		}
	}

	return false
}

func updateDarkDetectionView(regionID string, turn int) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	WorldState.DarkView.LastDetectedRegion = regionID
	WorldState.DarkView.LastDetectedTurn = turn
	WorldState.DarkView.RingBearerRegion = "" // yine de gerçek region direkt view'a yazılmıyor
	WorldState.UpdatedAt = time.Now().UnixMilli()
}