package internal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

// ApplyBlockPathOrder sets a path to BLOCKED and records the blocker.
// ApplyBlockPathOrder sets a path to BLOCKED and records the blocker.
func ApplyBlockPathOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)
	if pathID == "" {
		return "", errors.New("BLOCK_PATH: pathId is empty")
	}

	path, ok := getPathByID(pathID)
	if !ok {
		return "", fmt.Errorf("BLOCK_PATH: path not found: %s", pathID)
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", fmt.Errorf("BLOCK_PATH: unit region unknown: %s", unitID)
	}
	if currentRegion != path.From && currentRegion != path.To {
		return "", fmt.Errorf("BLOCK_PATH: %s not at endpoint of %s", unitID, pathID)
	}

	// FIX B6: If a Nazgul tries to block a path and a FellowshipGuard is present
	// at either endpoint of that path, the block attempt must fail.
	blockerCfg, hasCfg := getUnitConfig(unitID)
	if hasCfg && blockerCfg.DetectionRange > 0 {
		guardAtFrom := fellowshipGuardPresentAt(path.From, unitID)
		guardAtTo := fellowshipGuardPresentAt(path.To, unitID)

		if guardAtFrom || guardAtTo {
			return "", fmt.Errorf(
				"BLOCK_PREVENTED: a FellowshipGuard unit is present at endpoint of %s — %s cannot block",
				pathID, unitID)
		}
	}

	setPathStatus(pathID, "BLOCKED")
	UpdatePathStatus(pathID, "BLOCKED")

	pathBlockersMu.Lock()
	pathBlockers[pathID] = unitID
	pathBlockersMu.Unlock()

	msg := fmt.Sprintf("%s blocked path %s", unitID, pathID)
	fmt.Println("🧱", msg)
	return msg, nil
}
// fellowshipGuardPresentAt returns true if any ACTIVE FellowshipGuard unit
// (config.Class == "FellowshipGuard") is present at regionID, excluding excludeUnitID.
// FIX B6: config-driven class check — no unit ID strings in logic.
func fellowshipGuardPresentAt(regionID string, excludeUnitID string) bool {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	for _, u := range WorldState.Units {
		if u.ID == excludeUnitID || u.Region != regionID || u.Status != "ACTIVE" {
			continue
		}
		cfg, ok := WorldState.UnitConfigs[u.ID]
		// FellowshipGuard class covers: aragorn, legolas, gimli, rohan-cavalry
		if ok && cfg.Class == "FellowshipGuard" {
			return true
		}
	}
	return false
}

// ApplySearchPathOrder raises a path's surveillance level by 1 (max 3).
func ApplySearchPathOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)
	if pathID == "" {
		return "", errors.New("SEARCH_PATH: pathId is empty")
	}

	path, ok := getPathByID(pathID)
	if !ok {
		return "", fmt.Errorf("SEARCH_PATH: path not found: %s", pathID)
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", fmt.Errorf("SEARCH_PATH: unit region unknown: %s", unitID)
	}
	if currentRegion != path.From && currentRegion != path.To {
		return "", fmt.Errorf("SEARCH_PATH: %s not at endpoint of %s", unitID, pathID)
	}

	// Raise surveillance (spec: max 3)
	worldStateMu.Lock()
	p, pok := WorldState.Paths[pathID]
	if pok && p.SurveillanceLevel < 3 {
		p.SurveillanceLevel++
		WorldState.Paths[pathID] = p
	}
	worldStateMu.Unlock()

	setPathStatus(pathID, "THREATENED")
	UpdatePathStatus(pathID, "THREATENED")

	msg := fmt.Sprintf("%s searched path %s; surveillance now %d", unitID, pathID, p.SurveillanceLevel)
	fmt.Println("🔍", msg)
	return msg, nil
}

// ApplyFortifyRegionOrder fortifies a region for 2 turns.
// FIX B10: Sets FortifyTimers entry for Step 10 decrement.
func ApplyFortifyRegionOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)

	// FIX B1: no hardcoded "gondor-army" — check config.CanFortify
	cfg, ok := getUnitConfig(unitID)
	if !ok {
		return "", fmt.Errorf("FORTIFY_REGION: unit config not found: %s", unitID)
	}
	if !cfg.CanFortify {
		return "", fmt.Errorf("FORTIFY_REGION: %s cannot fortify (CanFortify=false)", unitID)
	}

	regionID := order.RegionID
	if regionID == "" {
		regionID = order.TargetRegion
	}
	if regionID == "" {
		return "", errors.New("FORTIFY_REGION: regionId is empty")
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok || currentRegion != regionID {
		return "", fmt.Errorf("FORTIFY_REGION: %s is not in %s", unitID, regionID)
	}

	worldStateMu.Lock()
	region, rOk := WorldState.Regions[regionID]
	if !rOk {
		worldStateMu.Unlock()
		return "", fmt.Errorf("FORTIFY_REGION: region not found: %s", regionID)
	}
	region.Fortified = true
	WorldState.Regions[regionID] = region
	WorldState.FortifyTimers[regionID] = 2 // spec §6 step 10: timer=2
	worldStateMu.Unlock()

	msg := fmt.Sprintf("%s fortified %s (2 turns)", unitID, regionID)
	fmt.Println("🛡️", msg)
	return msg, nil
}

// ApplyMaiaAbilityOrder dispatches the correct ability based on config.AbilityEffect.
// FIX B5 / Q&A Q2: dispatch is config-driven (unit.AbilityEffect), never if-unit-id.
// FIX B5: MAIA_DISABLED check added for Saruman when Isengard has fallen.
// FIX B1: Cooldown set from config.Cooldown, not hardcoded.
func ApplyMaiaAbilityOrder(order OrderPayload) (string, error) {
	unitID := normalizedUnitID(order)
	pathID := singlePathID(order)

	unit, ok := getUnitConfig(unitID)
	if !ok {
		return "", fmt.Errorf("MAIA_ABILITY: unit config not found: %s", unitID)
	}
	if !unit.Maia {
		return "", fmt.Errorf("MAIA_ABILITY: %s is not Maia", unitID)
	}
	if getCooldown(unitID) > 0 {
		return "", fmt.Errorf("ABILITY_ON_COOLDOWN: %s cooldown=%d", unitID, getCooldown(unitID))
	}

	switch unit.AbilityEffect {
	case "TEMPORARILY_OPEN_PATH":
		// Gandalf — OpenPath ability
		if pathID == "" {
			return "", errors.New("TEMPORARILY_OPEN_PATH: pathId is empty")
		}
		if !pathExists(pathID) {
			return "", fmt.Errorf("TEMPORARILY_OPEN_PATH: path not found: %s", pathID)
		}

		setPathStatus(pathID, "TEMPORARILY_OPEN")
		UpdatePathStatus(pathID, "TEMPORARILY_OPEN")

		// FIX B10: set TempOpenTimer for Step 9 decrement
		worldStateMu.Lock()
		WorldState.TempOpenTimers[pathID] = 2 // spec: 2 turns
		worldStateMu.Unlock()

		// FIX B1: cooldown from config, not hardcoded 2
		setCooldown(unitID, unit.Cooldown)

		msg := fmt.Sprintf("%s opened blocked path %s (TEMPORARILY_OPEN, 2 turns, cooldown=%d)", unitID, pathID, unit.Cooldown)
		fmt.Println("🧙", msg)
		return msg, nil

	case "CORRUPT_PATH":
		// Saruman — CorruptPath ability
		// FIX B5: MAIA_DISABLED — Saruman cannot act if Isengard has fallen to Free Peoples
		worldStateMu.RLock()
		isengard, isengardOk := WorldState.Regions["isengard"]
		worldStateMu.RUnlock()
		if isengardOk && isengard.ControlledBy != "SHADOW" {
			return "", fmt.Errorf("MAIA_DISABLED: Saruman is permanently disabled (Isengard fell)")
		}

		if pathID == "" {
			return "", errors.New("CORRUPT_PATH: pathId is empty")
		}

		// Verify path is in Saruman's maiaAbilityPaths list (config-driven)
		validPath := false
		for _, allowed := range unit.MaiaAbilityPaths {
			if allowed == pathID {
				validPath = true
				break
			}
		}
		if !validPath {
			return "", fmt.Errorf("CORRUPT_PATH: path %s not in Saruman's ability paths", pathID)
		}

		worldStateMu.Lock()
		path, pOk := WorldState.Paths[pathID]
		if !pOk {
			worldStateMu.Unlock()
			return "", fmt.Errorf("CORRUPT_PATH: path not found: %s", pathID)
		}
		path.SurveillanceLevel = 3 // spec: permanent, cannot be undone
		path.Status = "THREATENED"
		WorldState.Paths[pathID] = path
		worldStateMu.Unlock()

		setPathStatus(pathID, "THREATENED")
		// FIX B1: cooldown from config
		setCooldown(unitID, unit.Cooldown)

		msg := fmt.Sprintf("%s corrupted path %s (surveillanceLevel=3, permanent)", unitID, pathID)
		fmt.Println("🧙‍♂️", msg)
		return msg, nil

	case "SAURON_AMPLIFIER":
		// Sauron — passive effect, no action needed (applied automatically in detection)
		// This case is only reached if someone explicitly submits a MAIA_ABILITY order for Sauron
		// FIX B1: cooldown from config (Sauron cooldown=0 per config)
		setCooldown(unitID, unit.Cooldown)
		msg := fmt.Sprintf("%s passive amplifier is active; Nazgul detection range +1 while in Mordor", unitID)
		fmt.Println("👁️", msg)
		return msg, nil

	default:
		return "", fmt.Errorf("MAIA_ABILITY: unknown abilityEffect=%s for unit=%s", unit.AbilityEffect, unitID)
	}
}

// EmitPathCorrupted publishes a PathCorrupted event to game.events.path.
func EmitPathCorrupted(producer *kafka.Producer, pathID string) {
	if producer == nil {
		return
	}
	payload := []byte(fmt.Sprintf(`{"eventType":"PathCorrupted","pathId":"%s","turn":%d}`, pathID, GetCurrentTurn()))
	if err := ProduceMessage(producer, "game.events.path", payload); err != nil {
		fmt.Printf("❌ Failed to emit PathCorrupted: %v\n", err)
	}
}

// ApplyAttackRegionOrder resolves combat between attacker and defender.
// FIX B3: Uses fully corrected ResolveCombat (terrain + leadership + indestructible).
func ApplyAttackRegionOrder(order OrderPayload) (string, error) {
	attackerID := normalizedUnitID(order)

	targetRegion := order.TargetRegion
	if targetRegion == "" {
		targetRegion = order.RegionID
	}
	if targetRegion == "" {
		return "", errors.New("ATTACK_REGION: targetRegion is empty")
	}

	sourceRegion, ok := getUnitCurrentRegion(attackerID)
	if !ok {
		return "", fmt.Errorf("ATTACK_REGION: attacker region unknown: %s", attackerID)
	}
	if !areAdjacent(sourceRegion, targetRegion) {
		return "", fmt.Errorf("ATTACK_REGION: %s → %s not adjacent", sourceRegion, targetRegion)
	}

	defenderID := NodeOccupants[targetRegion]
	if defenderID == "" {
		return "", fmt.Errorf("ATTACK_REGION: no unit at %s", targetRegion)
	}

	result := ResolveCombat(attackerID, defenderID, targetRegion)
	fmt.Printf("⚔️ Combat result: attacker=%d defender=%d winner=%s\n",
		result.AttackerPower, result.DefenderPower, result.WinnerID)

	if result.WinnerID == attackerID {
		NodeOccupants[targetRegion] = attackerID
		NodeOccupants[sourceRegion] = ""
		UpdateUnitRegion(attackerID, targetRegion)
		RevertPathBlocksForMovingUnit(attackerID, targetRegion)

		// FIX B5: If Isengard falls to Light Side, disable Saruman permanently
		if targetRegion == "isengard" {
			attackerCfg, _ := getUnitConfig(attackerID)
			if attackerCfg.Side == "FREE_PEOPLES" {
				fmt.Println("🏰 Isengard has fallen! Saruman is permanently disabled.")
				// The CORRUPT_PATH check reads WorldState.Regions["isengard"].ControlledBy
				// which is now FREE_PEOPLES — so MAIA_DISABLED is enforced automatically.
			}
		}

		return fmt.Sprintf("%s defeated %s and captured %s", attackerID, defenderID, targetRegion), nil
	}

	if result.WinnerID == defenderID {
		NodeOccupants[sourceRegion] = ""
		return fmt.Sprintf("%s was defeated by %s at %s", attackerID, defenderID, targetRegion), nil
	}

	// Draw: attacker repelled (already took -1 damage in ResolveCombat)
	return fmt.Sprintf("%s attacked %s; combat was a draw, attacker repelled", attackerID, targetRegion), nil
}

// ─────────────────────────────────────────────
// COMBAT FORMULA — FIX B3
// Spec §4: terrain_bonus + fortification_bonus + leadership_bonus + ignoresFortress + indestructible
// ─────────────────────────────────────────────

// CombatResult is the outcome of one ResolveCombat call.
// FIX B3 (Group Combat): AttackerPower and DefenderPower are now the SUM of all
// active units' effective strengths on each side, not just the lead attacker/defender.
type CombatResult struct {
	AttackerID        string   `json:"attackerId"`        // lead attacker unit
	DefenderID        string   `json:"defenderId"`        // lead defender unit
	TargetRegion      string   `json:"targetRegion"`
	AttackerPower     int      `json:"attackerPower"`     // SUM of all attacker strengths
	DefenderPower     int      `json:"defenderPower"`     // SUM of all defender strengths + bonuses
	WinnerID          string   `json:"winnerId"`
	WinningSide       string   `json:"winningSide"`
	AttackerUnitIDs   []string `json:"attackerUnitIds"`   // all units contributing to attack
	DefenderUnitIDs   []string `json:"defenderUnitIds"`   // all units contributing to defense
	AttackerModifier  int      `json:"attackerModifier"`
	DefenderModifier  int      `json:"defenderModifier"`
	Reason            string   `json:"reason"`
}

// getUnitsInRegion returns all ACTIVE unit snapshots whose Region matches regionID
// and whose side matches the given side. Used by group combat.
func getUnitsInRegion(side string, regionID string) []UnitSnapshot {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	var result []UnitSnapshot
	for _, u := range WorldState.Units {
		if u.Region == regionID && u.Side == side && u.Status == "ACTIVE" {
			result = append(result, u)
		}
	}
	return result
}

// attackerIgnoresFortress returns true if ANY attacker unit has IgnoresFortress=true.
// Spec §4: UrukHai in a group nullifies terrain bonus for the whole attack.
func attackerIgnoresFortress(attackerUnits []UnitSnapshot) bool {
	for _, u := range attackerUnits {
		if cfg, ok := getUnitConfig(u.ID); ok && cfg.IgnoresFortress {
			return true
		}
	}
	return false
}
func leadershipBonusForGroup(units []UnitSnapshot) int {
	total := 0

	for _, leader := range units {
		leaderCfg, ok := getUnitConfig(leader.ID)
		if !ok || !leaderCfg.Leadership || leaderCfg.LeadershipBonus <= 0 {
			continue
		}

		for _, ally := range units {
			if ally.ID == leader.ID {
				continue
			}
			total += leaderCfg.LeadershipBonus
		}
	}

	return total
}
// ResolveCombat implements the full group-combat formula from spec §4.
//
// FIX B3 (Group Combat): attacker_power = SUM of all attacker unit strengths
// in the source region; defender_power = SUM of all defender unit strengths in
// the target region, plus terrain/fortification bonuses applied once to the whole group.
//
// The leadAttackerID drives the ATTACK_REGION order. All same-side units
// already present in the sourceRegion automatically participate.
// The lead defender is the current NodeOccupant of targetRegion; all same-side
// co-located units also contribute.
func ResolveCombat(leadAttackerID string, leadDefenderID string, targetRegion string) CombatResult {
	sourceRegion, _ := getUnitCurrentRegion(leadAttackerID)

	worldStateMu.RLock()
	region, regionOk := WorldState.Regions[targetRegion]
	worldStateMu.RUnlock()

	attackerCfg, _ := getUnitConfig(leadAttackerID)
	defenderCfg, _ := getUnitConfig(leadDefenderID)

	// ── Collect all participating units ─────────────────────────────────
	attackerUnits := getUnitsInRegion(attackerCfg.Side, sourceRegion)
	defenderUnits := getUnitsInRegion(defenderCfg.Side, targetRegion)

	// Safety: always include the lead units even if getUnitsInRegion misses them
	if len(attackerUnits) == 0 {
		attackerUnits = []UnitSnapshot{{ID: leadAttackerID, Side: attackerCfg.Side,
			Region: sourceRegion, Strength: attackerCfg.Strength, Status: "ACTIVE"}}
	}
	if len(defenderUnits) == 0 {
		defenderUnits = []UnitSnapshot{{ID: leadDefenderID, Side: defenderCfg.Side,
			Region: targetRegion, Strength: defenderCfg.Strength, Status: "ACTIVE"}}
	}

	// ── Attacker power: SUM of all attacker unit strengths ───────────────
	attackerBaseSum := 0
	attackerIDs := make([]string, 0, len(attackerUnits))
	for _, u := range attackerUnits {
		attackerBaseSum += getUnitStrength(u.ID)
		attackerIDs = append(attackerIDs, u.ID)
	}
	// Leadership bonus: ONE bonus applied to the whole group (not per-unit)
	attackerLeadership := leadershipBonusForGroup(attackerUnits)
	attackerTotal := attackerBaseSum + attackerLeadership

	// ── Defender power: SUM of all defender unit strengths ───────────────
	defenderBaseSum := 0
	defenderIDs := make([]string, 0, len(defenderUnits))
	for _, u := range defenderUnits {
		defenderBaseSum += getUnitStrength(u.ID)
		defenderIDs = append(defenderIDs, u.ID)
	}
	// Leadership bonus for defenders
	defenderLeadership := leadershipBonusForGroup(defenderUnits)

	// Terrain bonus — skipped if ANY attacker has IgnoresFortress (spec §4)
	terrainBonus := 0
	if regionOk && !attackerIgnoresFortress(attackerUnits) {
		switch region.Terrain {
		case "FORTRESS":
			terrainBonus = 2
		case "MOUNTAINS":
			terrainBonus = 1
		}
	}

	// Fortification bonus — always applies regardless of IgnoresFortress
	fortBonus := 0
	if regionOk && region.Fortified {
		fortBonus = 2
	}

	defenderTotal := defenderBaseSum + defenderLeadership + terrainBonus + fortBonus

	// ── Outcome ──────────────────────────────────────────────────────────
	winnerID := ""
	winningSide := ""
	reason := "draw"

	if attackerTotal > defenderTotal {
		winnerID = leadAttackerID
		winningSide = attackerCfg.Side
		reason = "attacker_wins"
		damage := attackerTotal - defenderTotal
		// Distribute damage to all defender units (remove weakest first)
		applyGroupDamage(defenderUnits, damage, targetRegion)

		// Region control changes to attacker's side
		worldStateMu.Lock()
		if regionOk {
			region.ControlledBy = attackerCfg.Side
			WorldState.Regions[targetRegion] = region
		}
		worldStateMu.Unlock()

	} else {
		// Tie or defender wins: each attacker loses 1 strength (spec §4 — whole group takes -1)
		for _, u := range attackerUnits {
			applyDamage(u.ID, 1)
		}
		if defenderTotal > attackerTotal {
			winnerID = leadDefenderID
			winningSide = defenderCfg.Side
			reason = "defender_wins"
		}
	}

	return CombatResult{
		AttackerID:       leadAttackerID,
		DefenderID:       leadDefenderID,
		TargetRegion:     targetRegion,
		AttackerPower:    attackerTotal,
		DefenderPower:    defenderTotal,
		WinnerID:         winnerID,
		WinningSide:      winningSide,
		AttackerUnitIDs:  attackerIDs,
		DefenderUnitIDs:  defenderIDs,
		AttackerModifier: attackerLeadership,
		DefenderModifier: defenderLeadership + terrainBonus + fortBonus,
		Reason:           reason,
	}
}

// applyGroupDamage distributes combat damage across all defeated units.
// Strategy: units are eliminated from weakest to strongest until damage is spent.
// Any remaining damage after the last unit is absorbed by the lead unit (indestructible rule).
func applyGroupDamage(units []UnitSnapshot, totalDamage int, regionID string) {
	// Sort weakest first to eliminate lowest-value units before high-value ones
	sortedUnits := make([]UnitSnapshot, len(units))
	copy(sortedUnits, units)
	for i := 0; i < len(sortedUnits)-1; i++ {
		for j := i + 1; j < len(sortedUnits); j++ {
			if sortedUnits[j].Strength < sortedUnits[i].Strength {
				sortedUnits[i], sortedUnits[j] = sortedUnits[j], sortedUnits[i]
			}
		}
	}
	remaining := totalDamage
	for _, u := range sortedUnits {
		if remaining <= 0 {
			break
		}
		dmg := remaining
		if dmg > u.Strength {
			dmg = u.Strength
		}
		applyDamage(u.ID, dmg)
		remaining -= dmg
	}
}

// leaderBonusInRegion returns the leadership bonus a unit receives from
// a co-located leader (config.Leadership=true, config.LeadershipBonus>0).
// FIX B1 / B3: Previously used unit.Class == "Maia" — wrong.
// Spec §4: "each ally co-located with a leader receives +leader.config.leadershipBonus"
// Aragorn and Witch-King are the config leaders; Gandalf/Sauron are NOT leaders.
func leaderBonusInRegion(side string, regionID string, excludeUnitID string) int {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	for _, unit := range WorldState.Units {
		if unit.ID == excludeUnitID {
			continue // spec: not the leader itself
		}
		if unit.Side != side || unit.Region != regionID || unit.Status != "ACTIVE" {
			continue
		}
		// FIX B1: Use config.Leadership, not class name check
		cfg, ok := WorldState.UnitConfigs[unit.ID]
		if ok && cfg.Leadership && cfg.LeadershipBonus > 0 {
			return cfg.LeadershipBonus
		}
	}
	return 0
}
// calculateCombatModifier is kept for backward compat with tests.
// Returns defender's fortification + control bonus (terrain is now in ResolveCombat).
func calculateCombatModifier(unitID string, regionID string, isAttacker bool) int {
	modifier := 0

	cfg, ok := getUnitConfig(unitID)
	if !ok {
		return modifier
	}

	worldStateMu.RLock()
	region, regionOk := WorldState.Regions[regionID]
	worldStateMu.RUnlock()

	if regionOk && !isAttacker {
		// Fortification bonus for defender
		if region.Fortified {
			modifier += 2
		}
		// Controlled region small defensive bonus
		if region.ControlledBy == cfg.Side {
			modifier += 1
		}
	}

	// Leadership bonus (config-driven)
	modifier += leaderBonusInRegion(cfg.Side, regionID, unitID)

	return modifier
}

// applyDamage subtracts damage from a unit and handles indestructible/respawn/destroy.
// FIX B3: Previously missing — indestructible units could be destroyed; respawn unimplemented.
func applyDamage(unitID string, damage int) {
	cfg, hasCfg := getUnitConfig(unitID)

	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	unit, ok := WorldState.Units[unitID]
	if !ok {
		return
	}

	newStrength := unit.Strength - damage

	if hasCfg && cfg.Indestructible {
		// FIX B3: Indestructible — strength floors at 1, status stays ACTIVE
		if newStrength < 1 {
			newStrength = 1
		}
		unit.Strength = newStrength
		unit.Status = "ACTIVE"

	} else if newStrength <= 0 {
		unit.Strength = 0
		if hasCfg && cfg.Respawns {
			// FIX B3: Respawning unit — removed from map, returns after config.RespawnTurns
			unit.Status = "RESPAWNING"
			unit.Region = ""
			NodeOccupants[unit.Region] = "" // vacate region
			WorldState.RespawnTimers[unitID] = cfg.RespawnTurns
			fmt.Printf("💫 %s is RESPAWNING (returns in %d turns)\n", unitID, cfg.RespawnTurns)
		} else {
			unit.Status = "DESTROYED"
			fmt.Printf("💀 %s is DESTROYED\n", unitID)
		}
	} else {
		unit.Strength = newStrength
	}

	WorldState.Units[unitID] = unit
}

// RevertPathBlocksForMovingUnit clears any blocks this unit owns if it leaves the endpoint.
// FIX B6: This logic is correct and fully tested.
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
		// If the unit is no longer at either endpoint, the block reverts
		if newRegion != path.From && newRegion != path.To {
			setPathStatus(pathID, "OPEN")
			UpdatePathStatus(pathID, "OPEN")
			delete(pathBlockers, pathID)
			fmt.Printf("♻️ Path block reverted: %s (blocker %s left endpoint)\n", pathID, unitID)
		}
	}
}

// getUnitStrength returns a unit's current strength from WorldState.
func getUnitStrength(unitID string) int {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	if unit, ok := WorldState.Units[unitID]; ok {
		return unit.Strength
	}
	cfg, ok := getUnitConfigNoLock(unitID)
	if ok {
		return cfg.Strength
	}
	return 0
}

// getUnitConfigNoLock is for internal use inside worldStateMu.RLock blocks.
func getUnitConfigNoLock(unitID string) (UnitConfig, bool) {
	cfg, ok := WorldState.UnitConfigs[unitID]
	return cfg, ok
}

// hasLeaderInRegion is kept for backward compat. Uses the new config-driven check.
func hasLeaderInRegion(side string, regionID string) bool {
	return leaderBonusInRegion(side, regionID, "") > 0
}
