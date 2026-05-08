package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"ring-of-the-middle-earth/internal"
)

func main() {
	fmt.Println("🌟 Yüzüklerin Efendisi: Oyun Motoru Başlatılıyor... 🌟")

	// JSON Konfigürasyonlarını Yükle
	if err := internal.LoadAllConfigs("../config/map.conf", "../config/units.conf"); err != nil {
		log.Fatalf("❌ Ayarlar yüklenemedi: %v", err)
	}

	// WorldStateCache'i başlat
	internal.InitWorldStateFromConfig()

	// Haritadaki ilk 5 yolu ekrana bas ki isimleri görelim
	fmt.Println("📍 Haritadaki Bazı Bağlantılar:")
	for i, path := range internal.LoadedMap.Paths {
		if i < 5 {
			fmt.Printf("Yol: %s -> %s\n", path.From, path.To)
		}
	}

	kafkaBroker := "localhost:9092"

	// Kafka Producer başlat
	producer, err := internal.InitProducer(kafkaBroker)
	if err != nil {
		log.Fatalf("❌ Kafka producer başlatılamadı: %v", err)
	}
	defer producer.Close()

	eventCh := make(chan internal.Event, 100)
	lightSideSSECh := make(chan internal.Event, 100)
	darkSideSSECh := make(chan internal.Event, 100)
	cacheUpdateCh := make(chan internal.Event, 100)
	engineCh := make(chan internal.Event, 100)

	go internal.CacheManager(cacheUpdateCh)

	go internal.EventRouter(
		eventCh,
		lightSideSSECh,
		darkSideSSECh,
		cacheUpdateCh,
		engineCh,
		producer,
	)

	// Kafka'dan raw, validated ve dlq topiclerini dinle
	go internal.StartKafkaConsumer(
	kafkaBroker,
	"game-engine-group-v7",
	[]string{
		internal.TopicOrdersRaw,
		internal.TopicOrdersValidated,
		internal.TopicDLQ,
		internal.TopicBroadcast,
		internal.TopicRingDetection,
		internal.TopicRingPosition,
		"game.events.unit",
		"game.events.region",
		"game.events.path",
	},
	eventCh,
)

	// Işık tarafı SSE bağlantısı
	http.HandleFunc("/events/light", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("🌐 Işık Tarafı canlı yayına bağlandı!")

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		for msg := range lightSideSSECh {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Topic, string(msg.Payload))

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	})

	// Karanlık taraf SSE bağlantısı
	http.HandleFunc("/events/dark", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("🌑 Karanlık Taraf canlı yayına bağlandı!")

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		for msg := range darkSideSSECh {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Topic, string(msg.Payload))

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	})

	// Oyun state endpoint'i
	http.HandleFunc("/game/state", func(w http.ResponseWriter, r *http.Request) {
		side := r.URL.Query().Get("side")
		if side == "" {
			side = "light"
		}

		payload, err := internal.GetPublicWorldStateJSON(side)
		if err != nil {
			http.Error(w, "State oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	})
	// Light Side route analysis endpoint'i
	http.HandleFunc("/analysis/routes", func(w http.ResponseWriter, r *http.Request) {
		playerID := r.URL.Query().Get("playerId")
		if playerID == "" {
			playerID = "light"
		}

		if playerID != "light" && playerID != "free" && playerID != "FREE_PEOPLES" {
			http.Error(w, "Bu endpoint sadece Light Side için kullanılabilir", http.StatusForbidden)
			return
		}

		payload, err := internal.GetRouteAnalysisJSON()
		if err != nil {
			http.Error(w, "Route analysis oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	})
	// Dark Side interception analysis endpoint'i
	http.HandleFunc("/analysis/intercept", func(w http.ResponseWriter, r *http.Request) {
		playerID := r.URL.Query().Get("playerId")
		if playerID == "" {
			playerID = "shadow"
		}

		if playerID != "shadow" && playerID != "dark" && playerID != "SHADOW" {
			http.Error(w, "Bu endpoint sadece Dark Side için kullanılabilir", http.StatusForbidden)
			return
		}

		payload, err := internal.GetInterceptAnalysisJSON()
		if err != nil {
			http.Error(w, "Intercept analysis oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	})
	http.HandleFunc("/turn/end", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Sadece POST desteklenir", http.StatusMethodNotAllowed)
			return
		}

		newTurn := internal.AdvanceTurn()

		// Turn ilerledikten sonra detection kontrolü çalışır.
		internal.RunDetection(producer)

		// Her turn sonunda WorldStateSnapshot broadcast edilir.
		internal.PublishWorldStateSnapshot(producer)

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"message":"Turn advanced","turn":%d}`, newTurn)
	})

	// Oyuncu emri alma endpoint'i
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Body okunamadı", http.StatusBadRequest)
			return
		}

		fmt.Printf("\n🕹️ Dışarıdan Yeni Emir Geldi: %s\n", string(body))

		if err := internal.ProduceMessage(producer, internal.TopicOrdersRaw, body); err != nil {
			fmt.Printf("❌ Kafka'ya order yazılamadı: %v\n", err)
			http.Error(w, "Kafka'ya order yazılamadı", http.StatusInternalServerError)
			return
		}

		fmt.Println("✅ Emir Kafka'ya başarıyla yazıldı: game.orders.raw")

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "⚔️ Emir Kafka'ya iletildi!")
	})

	fmt.Println("🚀 Tüm sistemler devrede! Sunucu 8080 portunda dinliyor...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("❌ Sunucu çöktü: %v", err)
	}
}