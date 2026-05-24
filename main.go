package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
_ 	"net/http/pprof"
	"os"

	"ring-of-the-middle-earth/internal"
)

func main() {
	fmt.Println("🌟 Yüzüklerin Efendisi: Oyun Motoru Başlatılıyor... 🌟")

	// JSON Konfigürasyonlarını Yükle
	mapConfigPath := os.Getenv("MAP_CONFIG_PATH")
	if mapConfigPath == "" {
		mapConfigPath = "../config/map.conf"
	}

	unitsConfigPath := os.Getenv("UNITS_CONFIG_PATH")
	if unitsConfigPath == "" {
		unitsConfigPath = "../config/units.conf"
	}

	if err := internal.LoadAllConfigs(mapConfigPath, unitsConfigPath); err != nil {
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

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	
	// Kafka Producer başlat
	producer, err := internal.InitProducer(kafkaBroker)
	if err != nil {
		log.Fatalf("❌ Kafka producer başlatılamadı: %v", err)
	}
	defer producer.Close()

	instanceID := os.Getenv("HOSTNAME")
	if instanceID == "" {
		instanceID = "local"
	}

	transactionalID := fmt.Sprintf("game-over-transactional-producer-%s", instanceID)

	transactionalProducer, err := internal.InitTransactionalProducer(
		kafkaBroker,
		transactionalID,
	)
	if err != nil {
		log.Fatalf("❌ Transactional producer başlatılamadı: %v", err)
	}
	defer transactionalProducer.Close()

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
	consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
	if consumerGroup == "" {
		consumerGroup = "game-engine-group-final"
	}
	go internal.StartKafkaConsumer(
	kafkaBroker,
	consumerGroup,
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

		// Generic SSE endpoint'i: /events?side=light veya /events?side=shadow
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		side := r.URL.Query().Get("side")
		if side == "" {
			side = "light"
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		var selectedCh <-chan internal.Event

		if side == "shadow" || side == "dark" {
			fmt.Println("🌑 Karanlık Taraf /events üzerinden canlı yayına bağlandı!")
			selectedCh = darkSideSSECh
		} else {
			fmt.Println("🌐 Işık Tarafı /events üzerinden canlı yayına bağlandı!")
			selectedCh = lightSideSSECh
		}

		for msg := range selectedCh {
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
		// Health check endpoint'i
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Sadece GET desteklenir", http.StatusMethodNotAllowed)
			return
		}

		payload, err := internal.ToJSON(internal.GetHealthResponse())
		if err != nil {
			http.Error(w, "Health response oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	})
		// Game start endpoint'i
	http.HandleFunc("/game/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Sadece POST desteklenir", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Body okunamadı", http.StatusBadRequest)
			return
		}

		req := internal.GameStartRequest{Mode: "HVH"}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, "Geçersiz JSON", http.StatusBadRequest)
				return
			}
		}

		resp := internal.StartGameFromRequest(req)

		payload, err := internal.ToJSON(resp)
		if err != nil {
			http.Error(w, "Game start response oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	})
	// Available orders endpoint'i
	http.HandleFunc("/orders/available", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Sadece GET desteklenir", http.StatusMethodNotAllowed)
			return
		}

		playerID := r.URL.Query().Get("playerId")
		unitID := r.URL.Query().Get("unitId")

		resp := internal.GetOrdersAvailable(playerID, unitID)

		payload, err := internal.ToJSON(resp)
		if err != nil {
			http.Error(w, "Available orders response oluşturulamadı", http.StatusInternalServerError)
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

	// K6 Demo: transactional GameOver publish
	http.HandleFunc("/demo/game-over-transaction", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Sadece POST desteklenir", http.StatusMethodNotAllowed)
			return
		}

		gameID := r.URL.Query().Get("gameId")
		if gameID == "" {
			gameID = "demo-game-1"
		}

		if err := internal.PublishGameOverAndAbortForDemo(
			transactionalProducer,
			gameID,
			"SHADOW",
			"ABORTED_ENGINE_CRASH",
		); err != nil {
			http.Error(w, fmt.Sprintf("Abort demo failed: %v", err), http.StatusInternalServerError)
			return
		}

		if err := internal.PublishGameOverTransactionally(
			transactionalProducer,
			gameID,
			"SHADOW",
			"ENGINE_CRASH_COMMITTED",
		); err != nil {
			http.Error(w, fmt.Sprintf("Commit demo failed: %v", err), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"message": "Transactional GameOver demo executed",
			"gameId": gameID,
			"abortedTransaction": "ABORTED_ENGINE_CRASH should not be visible to read_committed consumers",
			"committedTransaction": "ENGINE_CRASH_COMMITTED should be visible exactly once",
		}

		payload, err := internal.ToJSON(resp)
		if err != nil {
			http.Error(w, "Response oluşturulamadı", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
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
	// Static UI
	fs := http.FileServer(http.Dir("./ui"))
	http.Handle("/", fs)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("🚀 Tüm sistemler devrede! Sunucu %s portunda dinliyor...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("❌ Sunucu çöktü: %v", err)
	}
}