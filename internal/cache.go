package internal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Oyundaki birimlerin o anki durumu
type UnitSnapshot struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Class    string `json:"class"`
	Side     string `json:"side"`
	Region   string `json:"region"`
	Strength int    `json:"strength"`
	Status   string `json:"status"`
}

// Bölgelerin o anki durumu
type RegionState struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ControlledBy string `json:"controlledBy"`
	ThreatLevel  int    `json:"threatLevel"`
	Fortified    bool   `json:"fortified"`
}

// Yolların o anki durumu
type PathState struct {
	ID                string `json:"id"`
	From              string `json:"from"`
	To                string `json:"to"`
	Cost              int    `json:"cost"`
	Status            string `json:"status"`
	SurveillanceLevel int    `json:"surveillanceLevel"`
}

// Işık Tarafı'nın Görebildikleri
type LightSideView struct {
	RingBearerRegion string   `json:"ringBearerRegion"`
	AssignedRoute    []string `json:"assignedRoute"`
	RouteIdx          int      `json:"routeIdx"`
}

// Karanlık Taraf'ın Görebildikleri
type DarkSideView struct {
	RingBearerRegion   string `json:"ringBearerRegion"` // HER ZAMAN BOŞ KALMALI
	LastDetectedRegion string `json:"lastDetectedRegion"`
	LastDetectedTurn   int    `json:"lastDetectedTurn"`
}

// WorldStateCache: Oyunun hafızası
type WorldStateCache struct {
	Turn        int                     `json:"turn"`
	Units       map[string]UnitSnapshot `json:"units"`
	Regions     map[string]RegionState  `json:"regions"`
	Paths       map[string]PathState    `json:"paths"`
	UnitConfigs map[string]UnitConfig   `json:"-"`
	LightView   LightSideView           `json:"lightView"`
	DarkView    DarkSideView            `json:"darkView"`
	UpdatedAt   int64                   `json:"updatedAt"`
}

var (
	worldStateMu sync.RWMutex
	WorldState  WorldStateCache
)

// InitWorldStateFromConfig config dosyalarından başlangıç state'ini oluşturur.
func InitWorldStateFromConfig() {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	fmt.Println("🧠 WorldStateCache config üzerinden başlatılıyor...")

	WorldState = WorldStateCache{
		Turn:        1,
		Units:       make(map[string]UnitSnapshot),
		Regions:     make(map[string]RegionState),
		Paths:       make(map[string]PathState),
		UnitConfigs: make(map[string]UnitConfig),
		LightView: LightSideView{
			RingBearerRegion: "",
			AssignedRoute:    []string{},
			RouteIdx:          0,
		},
		DarkView: DarkSideView{
			RingBearerRegion:   "",
			LastDetectedRegion: "",
			LastDetectedTurn:   0,
		},
		UpdatedAt: time.Now().UnixMilli(),
	}

	// Unit configlerinden unit state oluştur.
	for _, unit := range LoadedUnits {
		WorldState.UnitConfigs[unit.ID] = unit

		region := unit.StartRegion
		if unit.ID == RingBearerID {
			WorldState.LightView.RingBearerRegion = region
			WorldState.DarkView.RingBearerRegion = "" // Dark side asla görmez.
		}

		WorldState.Units[unit.ID] = UnitSnapshot{
			ID:       unit.ID,
			Name:     unit.Name,
			Class:    unit.Class,
			Side:     unit.Side,
			Region:   region,
			Strength: unit.Strength,
			Status:   "ACTIVE",
		}

		// Eski prototip motoru da bozulmasın diye NodeOccupants dolduruyoruz.
		// Aynı region'da birden fazla unit varsa bu map sadece sonuncuyu tutar.
		// Finalde region -> []unitId yapısına geçmek daha doğru olur.
		NodeOccupants[region] = unit.ID
	}

	// Region configlerinden region state oluştur.
	for _, region := range LoadedMap.Regions {
		WorldState.Regions[region.ID] = RegionState{
			ID:           region.ID,
			Name:         region.Name,
			ControlledBy: region.StartControl,
			ThreatLevel:  region.StartThreat,
			Fortified:    false,
		}
	}

	// Path configlerinden path state oluştur.
	for _, path := range LoadedMap.Paths {
		WorldState.Paths[path.ID] = PathState{
			ID:                path.ID,
			From:              path.From,
			To:                path.To,
			Cost:              path.Cost,
			Status:            "OPEN",
			SurveillanceLevel: 0,
		}
	}

	fmt.Printf("✅ WorldState hazırlandı: units=%d regions=%d paths=%d turn=%d\n",
		len(WorldState.Units),
		len(WorldState.Regions),
		len(WorldState.Paths),
		WorldState.Turn,
	)
}

// GetWorldStateCopy dışarıya güvenli kopya verir.
func GetWorldStateCopy() WorldStateCache {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	copyState := WorldState
	return copyState
}

// GetPublicWorldStateJSON oyuncu tarafına göre state döndürür.
// side: "light" veya "shadow"
func GetPublicWorldStateJSON(side string) ([]byte, error) {
	worldStateMu.RLock()
	copyState := copyWorldState(WorldState)
	worldStateMu.RUnlock()

	if side == "shadow" || side == "dark" || side == "SHADOW" {
		// Dark Side, Ring Bearer'ın gerçek konumunu hiçbir view üzerinden görmemeli.
		copyState.LightView.RingBearerRegion = ""
		copyState.DarkView.RingBearerRegion = ""

		if rb, ok := copyState.Units[RingBearerID]; ok {
			rb.Region = ""
			copyState.Units[RingBearerID] = rb
		}
	}

	return json.MarshalIndent(copyState, "", "  ")
}

func copyWorldState(src WorldStateCache) WorldStateCache {
	dst := src

	dst.Units = make(map[string]UnitSnapshot, len(src.Units))
	for k, v := range src.Units {
		dst.Units[k] = v
	}

	dst.Regions = make(map[string]RegionState, len(src.Regions))
	for k, v := range src.Regions {
		dst.Regions[k] = v
	}

	dst.Paths = make(map[string]PathState, len(src.Paths))
	for k, v := range src.Paths {
		dst.Paths[k] = v
	}

	dst.UnitConfigs = make(map[string]UnitConfig, len(src.UnitConfigs))
	for k, v := range src.UnitConfigs {
		dst.UnitConfigs[k] = v
	}

	// Slice olduğu için ayrı kopyalıyoruz.
	dst.LightView.AssignedRoute = append([]string{}, src.LightView.AssignedRoute...)

	return dst
}

// UpdateUnitRegion unit hareket edince cache'i günceller.
func UpdateUnitRegion(unitID string, newRegion string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	unit, ok := WorldState.Units[unitID]
	if !ok {
		return
	}

	unit.Region = newRegion
	WorldState.Units[unitID] = unit

	if unitID == RingBearerID {
		WorldState.LightView.RingBearerRegion = newRegion
		WorldState.DarkView.RingBearerRegion = ""
	}

	WorldState.UpdatedAt = time.Now().UnixMilli()
}

// UpdatePathStatus cache içindeki path status'ünü günceller.
func UpdatePathStatus(pathID string, status string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	path, ok := WorldState.Paths[pathID]
	if !ok {
		return
	}

	path.Status = status
	WorldState.Paths[pathID] = path
	WorldState.UpdatedAt = time.Now().UnixMilli()
}

// CacheManager goroutine'i: Gelen güncellemeleri hafızaya yazar.
// Şimdilik log tutuyor; ileride Kafka event payloadlarını çözüp cache'e işleyecek.
func CacheManager(updateCh <-chan Event) {
	fmt.Println("🧠 CacheManager goroutine'i başlatıldı, state güncellemeleri dinleniyor...")

	for event := range updateCh {
		fmt.Printf("📦 Cache güncelleme eventi geldi. Topic: %s\n", event.Topic)
	}
}