package internal

import (
	"errors"
	"fmt"
	"sync"
)

var (
	pathBlockersMu sync.Mutex
	pathBlockers   = make(map[string]string) // pathId -> blockingUnitId
)

func broadcastActionMessage(topic string, message string) Event {
	return Event{
		Topic:   topic,
		Payload: []byte(fmt.Sprintf(`{"message":"%s"}`, message)),
	}
}

func ApplyBlockPathOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)

	if pathID == "" {
		return "", errors.New("BLOCK_PATH için pathId boş")
	}

	path, ok := getPathByID(pathID)
	if !ok {
		return "", fmt.Errorf("path bulunamadı: %s", pathID)
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", fmt.Errorf("unit region bulunamadı: %s", unitID)
	}

	if currentRegion != path.From && currentRegion != path.To {
		return "", fmt.Errorf("%s, %s path endpointlerinden birinde değil", unitID, pathID)
	}

	setPathStatus(pathID, "BLOCKED")
	UpdatePathStatus(pathID, "BLOCKED")

	pathBlockersMu.Lock()
	pathBlockers[pathID] = unitID
	pathBlockersMu.Unlock()

	msg := fmt.Sprintf("%s, %s yolunu BLOCKED yaptı", unitID, pathID)
	fmt.Println("🧱", msg)

	return msg, nil
}

func ApplySearchPathOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)

	if pathID == "" {
		return "", errors.New("SEARCH_PATH için pathId boş")
	}

	path, ok := getPathByID(pathID)
	if !ok {
		return "", fmt.Errorf("path bulunamadı: %s", pathID)
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", fmt.Errorf("unit region bulunamadı: %s", unitID)
	}

	if currentRegion != path.From && currentRegion != path.To {
		return "", fmt.Errorf("%s, %s path endpointlerinden birinde değil", unitID, pathID)
	}

	// Prototip etki: searched path tehditli hale gelir.
	setPathStatus(pathID, "THREATENED")
	UpdatePathStatus(pathID, "THREATENED")

	msg := fmt.Sprintf("%s, %s yolunu SEARCH etti; path THREATENED oldu", unitID, pathID)
	fmt.Println("🔍", msg)

	return msg, nil
}

func ApplyFortifyRegionOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)

	regionID := order.RegionID
	if regionID == "" {
		regionID = order.TargetRegion
	}

	if regionID == "" {
		return "", errors.New("FORTIFY_REGION için regionId boş")
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", fmt.Errorf("unit region bulunamadı: %s", unitID)
	}

	if currentRegion != regionID {
		return "", fmt.Errorf("%s şu anda %s bölgesinde; %s fortify edemez", unitID, currentRegion, regionID)
	}

	worldStateMu.Lock()
	region, ok := WorldState.Regions[regionID]
	if !ok {
		worldStateMu.Unlock()
		return "", fmt.Errorf("region bulunamadı: %s", regionID)
	}

	region.Fortified = true
	WorldState.Regions[regionID] = region
	worldStateMu.Unlock()

	msg := fmt.Sprintf("%s, %s bölgesini fortified yaptı", unitID, regionID)
	fmt.Println("🛡️", msg)

	return msg, nil
}

func ApplyMaiaAbilityOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)

	unit, ok := getUnitConfig(unitID)
	if !ok {
		return "", fmt.Errorf("unit config bulunamadı: %s", unitID)
	}

	if unit.Class != "Maia" {
		return "", fmt.Errorf("%s Maia değil", unitID)
	}

	if getCooldown(unitID) > 0 {
		return "", fmt.Errorf("%s ability cooldown'da", unitID)
	}

	switch unit.AbilityEffect {
	case "TEMPORARILY_OPEN_PATH":
		if pathID == "" {
			return "", errors.New("TEMPORARILY_OPEN_PATH ability için pathId boş")
		}

		if !pathExists(pathID) {
			return "", fmt.Errorf("path bulunamadı: %s", pathID)
		}

		setPathStatus(pathID, "TEMPORARILY_OPEN")
		UpdatePathStatus(pathID, "TEMPORARILY_OPEN")
		setCooldown(unitID, 2)

		msg := fmt.Sprintf("%s, %s yolunu TEMPORARILY_OPEN yaptı", unitID, pathID)
		fmt.Println("🧙", msg)
		return msg, nil

	case "CORRUPT_PATH":
		if pathID == "" {
			return "", errors.New("CORRUPT_PATH ability için pathId boş")
		}

		worldStateMu.Lock()
		path, ok := WorldState.Paths[pathID]
		if !ok {
			worldStateMu.Unlock()
			return "", fmt.Errorf("path bulunamadı: %s", pathID)
		}

		path.SurveillanceLevel = 3
		path.Status = "THREATENED"
		WorldState.Paths[pathID] = path
		worldStateMu.Unlock()

		setPathStatus(pathID, "THREATENED")
		setCooldown(unitID, 2)

		msg := fmt.Sprintf("%s, %s yolunun surveillanceLevel değerini 3 yaptı", unitID, pathID)
		fmt.Println("🧙‍♂️", msg)
		return msg, nil

	case "SAURON_AMPLIFIER":
		setCooldown(unitID, 2)

		msg := fmt.Sprintf("%s passive amplifier aktif; Nazgul detection range +1", unitID)
		fmt.Println("👁️", msg)
		return msg, nil

	default:
		return "", fmt.Errorf("Maia abilityEffect tanımlı değil: unit=%s effect=%s", unitID, unit.AbilityEffect)
	}
}

func ApplyAttackRegionOrder(order OrderPayload) (string, error) {
	attackerID := normalizedUnitID(order)

	targetRegion := order.TargetRegion
	if targetRegion == "" {
		targetRegion = order.RegionID
	}

	if targetRegion == "" {
		return "", errors.New("ATTACK_REGION için targetRegion boş")
	}

	sourceRegion, ok := getUnitCurrentRegion(attackerID)
	if !ok {
		return "", fmt.Errorf("attacker region bulunamadı: %s", attackerID)
	}

	if !areAdjacent(sourceRegion, targetRegion) {
		return "", fmt.Errorf("%s -> %s adjacent değil", sourceRegion, targetRegion)
	}

	defenderID := NodeOccupants[targetRegion]
	if defenderID == "" {
		return "", fmt.Errorf("hedef bölgede defender yok: %s", targetRegion)
	}

	result := ResolveCombat(attackerID, defenderID, targetRegion)

	fmt.Printf("⚔️ Combat result: %+v\n", result)

	if result.WinnerID == attackerID {
		NodeOccupants[targetRegion] = attackerID
		NodeOccupants[sourceRegion] = ""
		UpdateUnitRegion(attackerID, targetRegion)
		RevertPathBlocksForMovingUnit(attackerID, targetRegion)

		worldStateMu.Lock()
		if defender, ok := WorldState.Units[defenderID]; ok {
			defender.Status = "DEFEATED"
			WorldState.Units[defenderID] = defender
		}
		worldStateMu.Unlock()

		return fmt.Sprintf("%s attacked %s and defeated %s", attackerID, targetRegion, defenderID), nil
	}

	if result.WinnerID == defenderID {
		NodeOccupants[sourceRegion] = ""

		worldStateMu.Lock()
		if attacker, ok := WorldState.Units[attackerID]; ok {
			attacker.Status = "DEFEATED"
			WorldState.Units[attackerID] = attacker
		}
		worldStateMu.Unlock()

		return fmt.Sprintf("%s attacked %s but was defeated by %s", attackerID, targetRegion, defenderID), nil
	}

	return fmt.Sprintf("%s attacked %s; combat ended in draw", attackerID, targetRegion), nil
}

type CombatResult struct {
	AttackerID       string `json:"attackerId"`
	DefenderID       string `json:"defenderId"`
	TargetRegion     string `json:"targetRegion"`
	AttackerPower    int    `json:"attackerPower"`
	DefenderPower    int    `json:"defenderPower"`
	WinnerID         string `json:"winnerId"`
	AttackerModifier int    `json:"attackerModifier"`
	DefenderModifier int    `json:"defenderModifier"`
	Reason           string `json:"reason"`
}

func ResolveCombat(attackerID string, defenderID string, targetRegion string) CombatResult {
	attackerPower := getUnitStrength(attackerID)
	defenderPower := getUnitStrength(defenderID)

	attackerMod := calculateCombatModifier(attackerID, targetRegion, true)
	defenderMod := calculateCombatModifier(defenderID, targetRegion, false)

	attackerTotal := attackerPower + attackerMod
	defenderTotal := defenderPower + defenderMod

	winner := ""
	reason := "draw"

	if attackerTotal > defenderTotal {
		winner = attackerID
		reason = "attacker_power_greater"
	} else if defenderTotal > attackerTotal {
		winner = defenderID
		reason = "defender_power_greater"
	}

	return CombatResult{
		AttackerID:       attackerID,
		DefenderID:       defenderID,
		TargetRegion:     targetRegion,
		AttackerPower:    attackerTotal,
		DefenderPower:    defenderTotal,
		WinnerID:         winner,
		AttackerModifier: attackerMod,
		DefenderModifier: defenderMod,
		Reason:           reason,
	}
}

func calculateCombatModifier(unitID string, regionID string, isAttacker bool) int {
	modifier := 0

	unit, ok := getUnitConfig(unitID)
	if !ok {
		return modifier
	}

	worldStateMu.RLock()
	region, regionOk := WorldState.Regions[regionID]
	worldStateMu.RUnlock()

	if regionOk {
		// Fortified region defender bonus.
		if !isAttacker && region.Fortified {
			modifier += 2
		}

		// Controlled region small defensive bonus.
		if !isAttacker && region.ControlledBy == unit.Side {
			modifier += 1
		}

		// UrukHaiLegion ignores fortress/fortify style defender advantage while attacking.
		if isAttacker && unit.Class == "UrukHaiLegion" {
			modifier += 1
		}
	}

	// Leadership bonus: Gandalf/Sauron same region gives +1 to same-side units.
	if hasLeaderInRegion(unit.Side, regionID) && unit.Class != "Maia" {
		modifier += 1
	}

	return modifier
}

func hasLeaderInRegion(side string, regionID string) bool {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	for _, unit := range WorldState.Units {
		if unit.Side == side &&
			unit.Region == regionID &&
			unit.Status == "ACTIVE" &&
			unit.Class == "Maia" {
			return true
		}
	}

	return false
}

func RevertPathBlocksForMovingUnit(unitID string, newRegion string) {
	pathBlockersMu.Lock()
	defer pathBlockersMu.Unlock()

	for pathID, blockerID := range pathBlockers {
		if blockerID != unitID {
			continue
		}

		path, ok := getPathByID(pathID)
		if !ok {
			continue
		}

		// Blocking unit artık path endpointlerinden birinde değilse block kalkar.
		if newRegion != path.From && newRegion != path.To {
			setPathStatus(pathID, "OPEN")
			UpdatePathStatus(pathID, "OPEN")
			delete(pathBlockers, pathID)

			fmt.Printf("♻️ Path block reverted: %s çünkü %s artık endpointte değil\n", pathID, unitID)
		}
	}
}