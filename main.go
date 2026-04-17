package main

import (
	"encoding/json"
	"fmt"
	"os"
	"ring-of-the-middle-earth/internal"
)

func main() {
	fmt.Println("--- Yüzüklerin Efendisi: Oyun Motoru Başlatılıyor ---")

	// 1. Birimleri (Karakterleri) Yükle
	unitsData, err := os.ReadFile("../config/units.conf")
	if err != nil {
		fmt.Println("HATA: units.conf okunamadı:", err)
		return
	}

	var units []internal.UnitConfig
	if err := json.Unmarshal(unitsData, &units); err != nil {
		fmt.Println("HATA: units.conf JSON formatı çözülemedi:", err)
		return
	}
	fmt.Printf("✅ %d adet birim başarıyla yüklendi! (İlk birim: %s)\n", len(units), units[0].Name)

	// 2. Haritayı Yükle
	mapData, err := os.ReadFile("../config/map.conf")
	if err != nil {
		fmt.Println("HATA: map.conf okunamadı:", err)
		return
	}

	var gameMap internal.MapConfig
	if err := json.Unmarshal(mapData, &gameMap); err != nil {
		fmt.Println("HATA: map.conf JSON formatı çözülemedi:", err)
		return
	}
	fmt.Printf("✅ Harita başarıyla yüklendi: %d bölge, %d yol bulundu!\n", len(gameMap.Regions), len(gameMap.Paths))
}
