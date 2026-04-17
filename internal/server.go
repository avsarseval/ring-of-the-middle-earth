package internal

import (
	"fmt"
	"net/http"
)

// StartServer, HTTP API ve SSE bağlantılarını ayağa kaldırır
func StartServer(port string, lightSideSSECh <-chan Event, darkSideSSECh <-chan Event) {
	// 1. Işık Tarafı (Free Peoples) SSE Endpoint'i
	http.HandleFunc("/events/light", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		fmt.Println("📡 Işık Tarafı (Tarayıcı) sunucuya bağlandı!")
		
		for event := range lightSideSSECh {
			// SSE formatında veriyi tarayıcıya yaz
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Topic, event.Payload)
			w.(http.Flusher).Flush() // Veriyi anında yolla
		}
	})

	// 2. Karanlık Taraf (Shadow) SSE Endpoint'i
	http.HandleFunc("/events/dark", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		fmt.Println("📡 Karanlık Taraf (Tarayıcı) sunucuya bağlandı!")
		
		for event := range darkSideSSECh {
			// SSE formatında veriyi tarayıcıya yaz
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Topic, event.Payload)
			w.(http.Flusher).Flush() // Veriyi anında yolla
		}
	})

	// 3. Oyunculardan gelen emirleri (Order) alma endpoint'i
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Sadece POST metodu kabul edilir", http.StatusMethodNotAllowed)
			return
		}
		
		// İleride burada gelen emri okuyup Kafka'ya (game.orders.raw) göndereceğiz.
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "Emir alındı, doğrulama için Kafka'ya gönderiliyor...")
	})

	fmt.Printf("🌐 HTTP Sunucusu başlatıldı! (Port: %s)\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		fmt.Printf("❌ Sunucu hatası: %v\n", err)
	}
}
