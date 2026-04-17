package internal

import (
	"encoding/json"
	"fmt"
)

// Event, Kafka'dan gelen veya SSE'ye gidecek temel mesaj yapısı
type Event struct {
	Topic   string
	Payload []byte
}

// Dünya durumunu temsil eden struct (İleride detaylandıracağız)
type WorldStateSnapshot struct {
	Turn      int                 `json:"turn"`
	Regions   map[string]string   `json:"regions"`
	Units     map[string]string   `json:"units"`
	Timestamp int64               `json:"timestamp"`
}

// EventRouter, projenin kalbidir. Bilgi asimetrisini burada zorluyoruz.
func EventRouter(
	eventCh <-chan Event,             // Kafka'dan okunan ham olaylar (cap=100)
	lightSideSSECh chan<- Event,      // Işık tarafı tarayıcısına gidecekler
	darkSideSSECh chan<- Event,       // Karanlık taraf tarayıcısına gidecekler
	cacheUpdateCh chan<- Event,       // CacheManager'a gidecek state güncellemeleri
	engineCh chan<- Event,            // TurnProcessor'a (Oyun motoruna) gidecek doğrulanmış emirler
) {
	fmt.Println("🔄 EventRouter goroutine'i başlatıldı, olaylar bekleniyor...")

	for event := range eventCh {
		switch event.Topic {
		case "game.ring.position":
			// Sadece Işık Tarafına gider
			lightSideSSECh <- event

		case "game.ring.detection":
			// Sadece Karanlık Tarafına gider
			darkSideSSECh <- event

		case "game.broadcast":
			// Işık Tarafı her şeyi görür
			lightSideSSECh <- event

			// Karanlık Taraf için yüzüğün konumunu sansürlemeliyiz!
			darkSideSSECh <- stripRingBearer(event)

		case "game.events.unit", "game.events.region", "game.events.path":
			// Ortak olaylar her iki tarafa da gider
			lightSideSSECh <- event
			darkSideSSECh <- event

		case "game.orders.validated":
			// Doğrulanmış emirler oyun motoruna (TurnProcessor) gider
			engineCh <- event

		default:
			// Diğer olaylar cache güncellemelerine yönlendirilebilir
			cacheUpdateCh <- event
		}
	}
}

// stripRingBearer, Karanlık Taraf'a giden mesajdan yüzüğün konumunu siler
func stripRingBearer(event Event) Event {
	var snapshot WorldStateSnapshot
	if err := json.Unmarshal(event.Payload, &snapshot); err == nil {
		// Eğer units map'i içinde yüzük taşıyıcısı varsa konumunu gizle
		if _, exists := snapshot.Units["ring-bearer"]; exists {
			snapshot.Units["ring-bearer"] = "" // Yüzük gizlendi!
		}
		newPayload, _ := json.Marshal(snapshot)
		return Event{Topic: event.Topic, Payload: newPayload}
	}
	return event // Çözülemezse orijinalini döndür (hata toleransı)
}
