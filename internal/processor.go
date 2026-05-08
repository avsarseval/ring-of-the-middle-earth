package internal

import (
	"errors"
	"fmt"
)

const (
	RingBearerID = "ring-bearer"
	MountDoomID  = "mount-doom"
)

// NodeOccupants regionId -> unitId şeklinde anlık basit oyun durumunu tutar.
// Bu şimdilik prototip state; ileride region -> []unitId yapısına geçmek daha doğru olur.
var NodeOccupants = map[string]string{}

func getUnitConfig(unitID string) (UnitConfig, bool) {
	for _, u := range LoadedUnits {
		if u.ID == unitID || u.Name == unitID {
			return u, true
		}
	}
	return UnitConfig{}, false
}

func getUnitStrength(unitID string) int {
	unit, ok := getUnitConfig(unitID)
	if !ok {
		return 0
	}
	return unit.Strength
}

func getUnitCurrentRegion(unitID string) (string, bool) {
	for regionID, occupantID := range NodeOccupants {
		if occupantID == unitID {
			return regionID, true
		}
	}

	unit, ok := getUnitConfig(unitID)
	if !ok {
		return "", false
	}

	return unit.StartRegion, true
}

func getPathByID(pathID string) (PathConfig, bool) {
	for _, path := range LoadedMap.Paths {
		if path.ID == pathID {
			return path, true
		}
	}
	return PathConfig{}, false
}

func areAdjacent(sourceNode string, targetNode string) bool {
	for _, path := range LoadedMap.Paths {
		if (path.From == sourceNode && path.To == targetNode) ||
			(path.From == targetNode && path.To == sourceNode) {
			return true
		}
	}
	return false
}

// ResolveMoveFromPath, ASSIGN_ROUTE order'ındaki ilk pathId'yi kullanarak bir sonraki hamleyi çıkarır.
func ResolveMoveFromPath(unitID string, pathID string) (source string, target string, err error) {
	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", "", fmt.Errorf("unit bulunamadı: %s", unitID)
	}

	path, ok := getPathByID(pathID)
	if !ok {
		return "", "", fmt.Errorf("path bulunamadı: %s", pathID)
	}

	switch currentRegion {
	case path.From:
		return path.From, path.To, nil
	case path.To:
		return path.To, path.From, nil
	default:
		return "", "", fmt.Errorf(
			"%s şu anda %s bölgesinde; %s path'inin endpoint'inde değil",
			unitID,
			currentRegion,
			pathID,
		)
	}
}

// ProcessTurn şimdilik tek hamleyi işler.
// Final mimaride bu fonksiyon 13 adımlı turn processor'a dönüşmeli.
func ProcessTurn(unitID string, sourceNode string, targetNode string) error {
	fmt.Printf("\n🛠️ Hakem Masası: Emir inceleniyor... (%s: %s -> %s)\n", unitID, sourceNode, targetNode)

	if _, ok := getUnitConfig(unitID); !ok {
		err := fmt.Errorf("❌ KURAL İHLALİ: unit config içinde bulunamadı: %s", unitID)
		fmt.Println(err)
		return err
	}

	if !areAdjacent(sourceNode, targetNode) {
		err := fmt.Errorf("❌ KURAL İHLALİ: %s ile %s arasında haritada bir yol yok", sourceNode, targetNode)
		fmt.Println(err)
		return err
	}

	occupant := NodeOccupants[targetNode]

	if occupant != "" && occupant != unitID {
		attackerConfig, attackerOk := getUnitConfig(unitID)
		defenderConfig, defenderOk := getUnitConfig(occupant)

		// Aynı taraf birlikleri aynı bölgeye girebilir; savaş yok.
		// Bu prototipte NodeOccupants tek unit tuttuğu için son gelen unit'i yazıyoruz.
		if attackerOk && defenderOk && attackerConfig.Side == defenderConfig.Side {
			fmt.Printf("🤝 Dost birlik aynı bölgede: %s ve %s. Savaş yok.\n", unitID, occupant)

			NodeOccupants[targetNode] = unitID
			NodeOccupants[sourceNode] = ""

			UpdateUnitRegion(unitID, targetNode)

			fmt.Println("🎯 İşlem tamamlandı!")
			return nil
		}

		attackerStrength := getUnitStrength(unitID)
		defenderStrength := getUnitStrength(occupant)

		fmt.Printf("⚔️  SAVAŞ: %s (Güç: %d) VS %s (Güç: %d)\n",
			unitID,
			attackerStrength,
			occupant,
			defenderStrength,
		)

		if attackerStrength > defenderStrength {
			fmt.Printf("🏆 KAZANAN: %s! %s haritadan silindi.\n", unitID, occupant)

			NodeOccupants[targetNode] = unitID
			NodeOccupants[sourceNode] = ""

			UpdateUnitRegion(unitID, targetNode)

			if occupant == RingBearerID {
				fmt.Println("💀 GÖREV BAŞARISIZ: Yüzük Taşıyıcısı yok edildi. KARANLIK TARAF KAZANDI! 🌑")
				return errors.New("GAME OVER: DARKNESS WINS")
			}
		} else if attackerStrength < defenderStrength {
			fmt.Printf("💀 KAYBETTİN: %s çok güçlü. %s yok edildi\n", occupant, unitID)

			NodeOccupants[sourceNode] = ""

			if unitID == RingBearerID {
				fmt.Println("💀 GÖREV BAŞARISIZ: Yüzük Taşıyıcısı yok edildi. KARANLIK TARAF KAZANDI! 🌑")
				return errors.New("GAME OVER: DARKNESS WINS")
			}

			return errors.New("birim yok edildi")
		} else {
			fmt.Println("🤝 BERABERE: İki taraf da eşit. Kendi bölgelerinde kalıyorlar.")
		}
	} else {
		NodeOccupants[targetNode] = unitID
		NodeOccupants[sourceNode] = ""

		UpdateUnitRegion(unitID, targetNode)
	}

	if unitID == RingBearerID && targetNode == MountDoomID {
		fmt.Println("🌋 GÖREV BAŞARILI: Yüzük Hüküm Dağı'na ulaştı ve yok edildi. IŞIK TARAFI KAZANDI! ☀️")
		return nil
	}

	fmt.Println("🎯 İşlem tamamlandı!")
	return nil
}