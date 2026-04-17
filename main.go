package main

import (
	"fmt"
	"ring-of-the-middle-earth/internal"
)

func main() {
	fmt.Println("--- Yüzüklerin Efendisi: Oyun Motoru Başlatılıyor ---")

	// 1. Kanallar (Otoyollar)
	eventCh := make(chan internal.Event, 100)
	lightSSECh := make(chan internal.Event, 100)
	darkSSECh := make(chan internal.Event, 100)
	cacheUpdateCh := make(chan internal.Event, 100)
	engineCh := make(chan internal.Event, 100)

	// 2. Beyin ve Hafızayı Başlat
	go internal.EventRouter(eventCh, lightSSECh, darkSSECh, cacheUpdateCh, engineCh)
	go internal.CacheManager(cacheUpdateCh)

	// 3. Kafka Bağlantıları
	broker := "localhost:9092"
	groupID := "game-engine-group"

	producer, err := internal.InitProducer(broker)
	if err != nil {
		fmt.Printf("❌ Producer Hatası: %v\n", err)
	} else {
		defer producer.Close()
		fmt.Println("✅ Kafka Producer hazır!")
	}

	consumer, err := internal.InitConsumer(broker, groupID)
	if err != nil {
		fmt.Printf("❌ Consumer Hatası: %v\n", err)
	} else {
		defer consumer.Close()
		fmt.Println("✅ Kafka Consumer hazır!")

		// 3.5. Kafka Topic'lerine Abone Ol ve Dinlemeye Başla!
		consumer.SubscribeTopics([]string{"game.broadcast", "game.ring.position"}, nil)

		go func() {
			fmt.Println("🎧 Kafka dinleme döngüsü (Consumer Loop) başladı, tetikteyiz...")
			for {
				msg, err := consumer.ReadMessage(-1)
				if err == nil {
					topic := *msg.TopicPartition.Topic
					fmt.Printf("📥 Kafka'dan YENİ OLAY yakalandı! Topic: %s\n", topic)
					
					// Gelen mesajı Olay Yönlendiriciye (Router) gönder
					eventCh <- internal.Event{
						Topic:   topic,
						Payload: msg.Value,
					}
				}
			}
		}()
	}

	// 4. Sunucuyu Başlat
	internal.StartServer(":8080", lightSSECh, darkSSECh)
}
