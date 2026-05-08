package internal

import (
	"encoding/json"
	"fmt"
	"os"
)

// UnitConfig, oyundaki her birimin özelliklerini tutar.
type UnitConfig struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Class            string   `json:"class"`
	Side             string   `json:"side"`
	StartRegion      string   `json:"startRegion"`
	Strength         int      `json:"strength"`
	Leadership       bool     `json:"leadership"`
	LeadershipBonus  int      `json:"leadershipBonus"`
	Indestructible   bool     `json:"indestructible"`
	DetectionRange   int      `json:"detectionRange"`
	Respawns         bool     `json:"respawns"`
	RespawnTurns     int      `json:"respawnTurns"`
	Maia             bool     `json:"maia"`
	MaiaAbilityPaths []string `json:"maiaAbilityPaths"`
	IgnoresFortress  bool     `json:"ignoresFortress"`
	CanFortify       bool     `json:"canFortify"`
	Cooldown         int      `json:"cooldown"`
}

// RegionConfig, haritadaki bölgelerin özelliklerini tutar.
type RegionConfig struct {
	ID           string `json:"regionId"`
	Name         string `json:"name"`
	Terrain      string `json:"terrain"`
	SpecialRole  string `json:"specialRole"`
	StartControl string `json:"startControl"`
	StartThreat  int    `json:"startThreat"`
}

// PathConfig, yolların özelliklerini tutar.
type PathConfig struct {
	ID   string `json:"pathId"`
	From string `json:"from"`
	To   string `json:"to"`
	Cost int    `json:"cost"`
}

// MapConfig, tüm haritayı okumak için ana yapıdır.
type MapConfig struct {
	Regions []RegionConfig `json:"regions"`
	Paths   []PathConfig   `json:"paths"`
}

// --- YENİ EKLENEN KISIM: DOSYALARI OKUMA FONKSİYONLARI ---

// Global olarak her yerden ulaşabileceğimiz veriler:
var LoadedMap MapConfig
var LoadedUnits []UnitConfig

// LoadAllConfigs, JSON dosyalarını okuyup global değişkenlere atar
func LoadAllConfigs(mapPath string, unitsPath string) error {
	// 1. Haritayı Oku
	mapData, err := os.ReadFile(mapPath)
	if err != nil {
		return fmt.Errorf("harita dosyası okunamadı: %v", err)
	}
	if err := json.Unmarshal(mapData, &LoadedMap); err != nil {
		return fmt.Errorf("harita JSON'ı çözülemedi: %v", err)
	}

	// 2. Birimleri Oku
	unitsData, err := os.ReadFile(unitsPath)
	if err != nil {
		return fmt.Errorf("birimler dosyası okunamadı: %v", err)
	}
	if err := json.Unmarshal(unitsData, &LoadedUnits); err != nil {
		return fmt.Errorf("birimler JSON'ı çözülemedi: %v", err)
	}

	fmt.Printf("📂 Konfigürasyonlar Yüklendi! (Bölge: %d, Yol: %d, Birim: %d)\n", 
		len(LoadedMap.Regions), len(LoadedMap.Paths), len(LoadedUnits))
	
	return nil
}