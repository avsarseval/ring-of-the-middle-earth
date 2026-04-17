package internal

import "fmt"

// Oyundaki birimlerin o anki durumu
type UnitSnapshot struct {
	ID       string `json:"id"`
	Region   string `json:"region"`
	Strength int    `json:"strength"`
	Status   string `json:"status"`
}

// Bölgelerin o anki durumu
type RegionState struct {
	ID           string `json:"id"`
	ControlledBy string `json:"controlledBy"`
	ThreatLevel  int    `json:"threatLevel"`
	Fortified    bool   `json:"fortified"`
}

// Yolların o anki durumu
type PathState struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	SurveillanceLevel int    `json:"surveillanceLevel"`
}

// Işık Tarafı'nın Görebildikleri
type LightSideView struct {
	RingBearerRegion string   `json:"ringBearerRegion"`
	AssignedRoute    []string `json:"assignedRoute"`
	RouteIdx         int      `json:"routeIdx"`
}

// Karanlık Taraf'ın Görebildikleri
type DarkSideView struct {
	RingBearerRegion   string `json:"ringBearerRegion"` // HER ZAMAN BOŞ ("") KALMALI[cite: 1, 31]!
	LastDetectedRegion string `json:"lastDetectedRegion"`
	LastDetectedTurn   int    `json:"lastDetectedTurn"`
}

// WorldStateCache: Oyunun hafızası 
type WorldStateCache struct {
	Turn        int
	Units       map[string]UnitSnapshot
	Regions     map[string]RegionState
	Paths       map[string]PathState
	UnitConfigs map[string]UnitConfig // Başlangıçtan sonra sadece okunur (read-only)
	LightView   LightSideView
	DarkView    DarkSideView
}

// CacheManager goroutine'i: Gelen güncellemeleri hafızaya yazar
func CacheManager(updateCh <-chan Event) {
	fmt.Println("🧠 CacheManager goroutine'i başlatıldı, state güncellemeleri dinleniyor...")

	// Başlangıç (Boş) State'i
	cache := WorldStateCache{
		Turn:    0,
		Units:   make(map[string]UnitSnapshot),
		Regions: make(map[string]RegionState),
		Paths:   make(map[string]PathState),
	}

	for event := range updateCh {
		// İleride buraya Kafka'dan gelen Event'leri JSON'dan çözüp 
		// cache objesini güncelleyen mantığı yazacağız.
		fmt.Printf("📦 Cache güncellendi. (Gelen Event: %s)\n", event.Topic)
		_ = cache // Go'nun 'kullanılmayan değişken' hatası vermemesi için
	}
}
