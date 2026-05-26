package internal

import (
	"os"
	"testing"
)

func setupTestWorld(t *testing.T) {
	t.Helper()

	mapPath, unitsPath := resolveTestConfigPaths(t)

	if err := LoadAllConfigs(mapPath, unitsPath); err != nil {
		t.Fatalf("config yüklenemedi: %v", err)
	}

	// Testler her çalıştığında temiz state ile başlasın.
	NodeOccupants = map[string]string{}

	pathStateMu.Lock()
	PathStatus = make(map[string]string)
	pathStateMu.Unlock()

	cooldownMu.Lock()
	UnitCooldown = make(map[string]int)
	cooldownMu.Unlock()

	orderTrackerMu.Lock()
	ordersByTurn = make(map[int]map[string]bool)
	orderTrackerMu.Unlock()

	currentTurnMu.Lock()
	CurrentTurn = 1
	currentTurnMu.Unlock()

	InitWorldStateFromConfig()
}

func resolveTestConfigPaths(t *testing.T) (string, string) {
	t.Helper()

	candidates := []struct {
		mapPath   string
		unitsPath string
	}{
		// go test ./... internal package içinden çalışınca doğru yol
		{"../../config/map.conf", "../../config/units.conf"},

		// option-b kökünden manuel çalıştırma ihtimaline karşı
		{"../config/map.conf", "../config/units.conf"},

		// proje kökünden çalıştırma ihtimaline karşı
		{"config/map.conf", "config/units.conf"},
	}

	for _, c := range candidates {
		if fileExists(c.mapPath) && fileExists(c.unitsPath) {
			return c.mapPath, c.unitsPath
		}
	}

	t.Fatal("config dosyaları bulunamadı: map.conf ve units.conf için path kontrol et")
	return "", ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
