package internal

import "testing"

// ─────────────────────────────────────────────────────────────────────────────
// ISOLATION STRATEGY
//
// setupTestWorld loads all 13 units into WorldState.Units at their config
// start regions. getUnitsInRegion sums EVERY active unit in a region, so ANY
// leader unit present (aragorn or witch-king, both leadershipBonus=1) will add
// +1 to the group total — even when the test doesn't intend it.
//
// The only correct fix: wipe WorldState.Units completely, then insert ONLY the
// exact actors needed for each test.
//
// Helper: wipeAndAddUnits replaces WorldState.Units with an empty map, then
// populates it with exactly the given snapshots. It also resets UnitCooldown
// and NodeOccupants so no other state bleeds between tests.
// ─────────────────────────────────────────────────────────────────────────────

func wipeAndAddUnits(t *testing.T, units ...UnitSnapshot) {
	t.Helper()
	worldStateMu.Lock()
	WorldState.Units = make(map[string]UnitSnapshot, len(units))
	for _, u := range units {
		WorldState.Units[u.ID] = u
	}
	worldStateMu.Unlock()

	// Clear occupant map so single-occupant fallback is also isolated
	NodeOccupants = make(map[string]string)

	cooldownMu.Lock()
	UnitCooldown = make(map[string]int)
	cooldownMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────────────
// UNIT ALIASES — use units with Leadership=false for exact-power tests
//
// Leadership=false units safe for 1v1 exact-power assertions:
//   FREE_PEOPLES str=5: gondor-army  (leadership=false, ignoresFortress=false)
//   SHADOW       str=5: sauron       (leadership=false, ignoresFortress=false, indestructible=true)
//   SHADOW       str=5: uruk-hai-legion (leadership=false, ignoresFortress=TRUE — used in Cases 3/4)
//
// Leadership=true units (aragorn, witch-king) are ONLY used in Case 5
// where the leadership bonus is the thing under test.
// ─────────────────────────────────────────────────────────────────────────────

// snap is a compact UnitSnapshot constructor.
func snap(id, side, region string, str int) UnitSnapshot {
	return UnitSnapshot{ID: id, Side: side, Region: region, Strength: str, Status: "ACTIVE"}
}

// ─────────────────────────────────────────────────────────────────────────────
// RUBRIC B3 — 6 REQUIRED COMBAT TEST CASES
// ─────────────────────────────────────────────────────────────────────────────

// Case 1: Attacker(5) vs Defender(5) at PLAINS → draw (5 vs 5, no bonuses)
// Uses gondor-army (FREE_PEOPLES, str=5, leadership=false) vs
//      sauron      (SHADOW,       str=5, leadership=false)
func TestCombatPlainsTerrain_Draw(t *testing.T) {
	setupTestWorld(t)
	wipeAndAddUnits(t,
		snap("gondor-army", "FREE_PEOPLES", "bree", 5),
		snap("sauron",      "SHADOW",       "bree", 5),
	)

	worldStateMu.Lock()
	if r, ok := WorldState.Regions["bree"]; ok {
		r.Terrain = "PLAINS"
		r.Fortified = false
		WorldState.Regions["bree"] = r
	}
	worldStateMu.Unlock()
	NodeOccupants["bree"] = "sauron"

	result := ResolveCombat("gondor-army", "sauron", "bree")

	if result.AttackerPower != 5 {
		t.Fatalf("Case 1: expected attacker power=5, got %d (check for stray leaders in WorldState)", result.AttackerPower)
	}
	if result.DefenderPower != 5 {
		t.Fatalf("Case 1: expected defender power=5, got %d", result.DefenderPower)
	}
	if result.WinnerID != "" {
		t.Fatalf("Case 1: expected draw (no winner), got winner=%s", result.WinnerID)
	}
	if result.Reason != "draw" {
		t.Fatalf("Case 1: expected reason=draw, got %s", result.Reason)
	}
}

// Case 2: Attacker(5) vs Defender(5, FORTRESS) → defender wins (5 vs 7)
// terrain_bonus = FORTRESS +2; attacker has ignoresFortress=false
func TestCombatFortressTerrain_DefenderWins(t *testing.T) {
	setupTestWorld(t)
	wipeAndAddUnits(t,
		snap("sauron",      "SHADOW",       "osgiliath",  5), // attacker — no leadership, no ignoresFortress
		snap("gondor-army", "FREE_PEOPLES", "minas-tirith", 5), // defender — no leadership
	)

	worldStateMu.Lock()
	if r, ok := WorldState.Regions["minas-tirith"]; ok {
		r.Terrain = "FORTRESS"
		r.Fortified = false
		r.ControlledBy = "FREE_PEOPLES"
		WorldState.Regions["minas-tirith"] = r
	}
	worldStateMu.Unlock()
	NodeOccupants["minas-tirith"] = "gondor-army"
	NodeOccupants["osgiliath"] = "sauron"

	result := ResolveCombat("sauron", "gondor-army", "minas-tirith")

	if result.AttackerPower != 5 {
		t.Fatalf("Case 2: expected attacker power=5, got %d", result.AttackerPower)
	}
	if result.DefenderPower != 7 {
		t.Fatalf("Case 2: expected defender power=7 (5 base + 2 FORTRESS), got %d", result.DefenderPower)
	}
	if result.WinnerID != "gondor-army" {
		t.Fatalf("Case 2: expected defender to win, got winner=%s", result.WinnerID)
	}
}

// Case 3: UrukHai(5, ignoresFortress=true) vs Defender(5, FORTRESS, NOT fortified) → draw
// ignoresFortress skips terrain_bonus → 5 vs 5
func TestCombatUrukHaiIgnoresFortressTerrain_Draw(t *testing.T) {
	setupTestWorld(t)

	cfg, ok := getUnitConfig("uruk-hai-legion")
	if !ok {
		t.Fatal("Case 3: uruk-hai-legion config not found")
	}
	if !cfg.IgnoresFortress {
		t.Skip("Case 3: uruk-hai-legion.IgnoresFortress=false in config")
	}

	wipeAndAddUnits(t,
		snap("uruk-hai-legion", "SHADOW",       "fords-of-isen", 5),
		snap("gondor-army",     "FREE_PEOPLES", "isengard",      5),
	)

	worldStateMu.Lock()
	if r, ok := WorldState.Regions["isengard"]; ok {
		r.Terrain = "FORTRESS"
		r.Fortified = false
		r.ControlledBy = "FREE_PEOPLES"
		WorldState.Regions["isengard"] = r
	}
	worldStateMu.Unlock()
	NodeOccupants["isengard"] = "gondor-army"
	NodeOccupants["fords-of-isen"] = "uruk-hai-legion"

	result := ResolveCombat("uruk-hai-legion", "gondor-army", "isengard")

	if result.AttackerPower != 5 {
		t.Fatalf("Case 3: expected attacker power=5, got %d", result.AttackerPower)
	}
	// ignoresFortress → terrain_bonus skipped → defender stays at 5
	if result.DefenderPower != 5 {
		t.Fatalf("Case 3: expected defender power=5 (FORTRESS terrain skipped), got %d", result.DefenderPower)
	}
	if result.WinnerID != "" {
		t.Fatalf("Case 3: expected draw, got winner=%s", result.WinnerID)
	}
}

// Case 4: UrukHai(5, ignoresFortress=true) vs Defender(5, FORTRESS + fortified) → defender wins (5 vs 7)
// ignoresFortress skips terrain_bonus (+2) but fortification_bonus (+2) ALWAYS applies → 5 vs 7
func TestCombatUrukHaiIgnoresFortress_FortificationStillApplies(t *testing.T) {
	setupTestWorld(t)

	cfg, ok := getUnitConfig("uruk-hai-legion")
	if !ok {
		t.Fatal("Case 4: uruk-hai-legion config not found")
	}
	if !cfg.IgnoresFortress {
		t.Skip("Case 4: uruk-hai-legion.IgnoresFortress=false in config")
	}

	wipeAndAddUnits(t,
		snap("uruk-hai-legion", "SHADOW",       "fords-of-isen", 5),
		snap("gondor-army",     "FREE_PEOPLES", "isengard",      5),
	)

	worldStateMu.Lock()
	if r, ok := WorldState.Regions["isengard"]; ok {
		r.Terrain = "FORTRESS"
		r.Fortified = true // fortification ON
		r.ControlledBy = "FREE_PEOPLES"
		WorldState.Regions["isengard"] = r
	}
	worldStateMu.Unlock()
	NodeOccupants["isengard"] = "gondor-army"
	NodeOccupants["fords-of-isen"] = "uruk-hai-legion"

	result := ResolveCombat("uruk-hai-legion", "gondor-army", "isengard")

	// terrain_bonus skipped (+0), fortification always applies (+2) → 5 vs 7
	if result.DefenderPower != 7 {
		t.Fatalf("Case 4: expected defender power=7 (5 + 0 terrain(skipped) + 2 fort), got %d", result.DefenderPower)
	}
	if result.AttackerPower != 5 {
		t.Fatalf("Case 4: expected attacker power=5, got %d", result.AttackerPower)
	}
	if result.WinnerID != "gondor-army" {
		t.Fatalf("Case 4: expected defender to win (7>5), got winner=%s", result.WinnerID)
	}
}

// Case 5: Leadership bonus applied correctly.

func TestCombatLeadershipBonusFromConfig(t *testing.T) {
	setupTestWorld(t)

	aragornCfg, ok := getUnitConfig("aragorn")
	if !ok {
		t.Fatal("Case 5: aragorn config not found")
	}
	if !aragornCfg.Leadership {
		t.Skip("Case 5: aragorn.Leadership=false in config")
	}

	wipeAndAddUnits(t,
		snap("aragorn",  "FREE_PEOPLES", "bree",       5), // leader
		snap("gimli",    "FREE_PEOPLES", "bree",       3), // ally gets +1
		snap("legolas",  "FREE_PEOPLES", "bree",       3), // ally gets +1
		snap("nazgul-2", "SHADOW",       "weathertop", 3), // defender
	)

	worldStateMu.Lock()
	if r, ok := WorldState.Regions["weathertop"]; ok {
		r.Terrain = "PLAINS"
		r.Fortified = false
		WorldState.Regions["weathertop"] = r
	}
	worldStateMu.Unlock()

	NodeOccupants["weathertop"] = "nazgul-2"
	NodeOccupants["bree"] = "aragorn"

	result := ResolveCombat("aragorn", "nazgul-2", "weathertop")

	// Rubric leadership rule:
	// Aragorn is the leader but does NOT receive his own bonus.
	// Gimli and Legolas are co-located allies, so each receives +1.
	// AttackerPower = Aragorn 5 + Gimli (3+1) + Legolas (3+1) = 13.
	expectedAttackerPower := 13
	if result.AttackerPower != expectedAttackerPower {
		t.Fatalf("Case 5: expected attacker power=%d (Aragorn 5 + Gimli 4 + Legolas 4), got %d",
			expectedAttackerPower, result.AttackerPower)
	}

	// Leadership modifier should be +2 total:
	// +1 for Gimli, +1 for Legolas.
	expectedModifier := aragornCfg.LeadershipBonus * 2
	if result.AttackerModifier != expectedModifier {
		t.Fatalf("Case 5: expected AttackerModifier=%d (+1 for Gimli, +1 for Legolas), got %d",
			expectedModifier, result.AttackerModifier)
	}
}
// Case 6: Indestructible unit — strength floors at 1, status stays ACTIVE after fatal damage.
func TestCombatIndestructibleStrengthFloorsAtOne(t *testing.T) {
	setupTestWorld(t)

	var indestructibleID string
	for _, u := range LoadedUnits {
		if u.Indestructible {
			indestructibleID = u.ID
			break
		}
	}
	if indestructibleID == "" {
		t.Skip("Case 6: no indestructible unit in units.conf")
	}

	// Only need the target unit in WorldState for applyDamage
	worldStateMu.Lock()
	WorldState.Units = make(map[string]UnitSnapshot)
	WorldState.Units[indestructibleID] = UnitSnapshot{
		ID: indestructibleID, Status: "ACTIVE", Strength: 1,
	}
	worldStateMu.Unlock()

	applyDamage(indestructibleID, 10)

	worldStateMu.RLock()
	result := WorldState.Units[indestructibleID]
	worldStateMu.RUnlock()

	if result.Strength < 1 {
		t.Fatalf("Case 6: indestructible %s strength=%d, want >=1", indestructibleID, result.Strength)
	}
	if result.Status != "ACTIVE" {
		t.Fatalf("Case 6: indestructible %s status=%s, want ACTIVE", indestructibleID, result.Status)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SUPPORTING TESTS
// ─────────────────────────────────────────────────────────────────────────────

func TestResolveCombatReturnsWinner(t *testing.T) {
	setupTestWorld(t)
	wipeAndAddUnits(t,
		snap("gondor-army",     "FREE_PEOPLES", "bree", 5),
		snap("uruk-hai-legion", "SHADOW",       "bree", 5),
	)
	NodeOccupants["bree"] = "uruk-hai-legion"

	result := ResolveCombat("gondor-army", "uruk-hai-legion", "bree")

	if result.AttackerID != "gondor-army" {
		t.Fatalf("expected attacker=gondor-army, got %s", result.AttackerID)
	}
	if result.DefenderID != "uruk-hai-legion" {
		t.Fatalf("expected defender=uruk-hai-legion, got %s", result.DefenderID)
	}
	if result.AttackerPower <= 0 {
		t.Fatalf("expected attacker power > 0, got %d", result.AttackerPower)
	}
	if result.DefenderPower <= 0 {
		t.Fatalf("expected defender power > 0, got %d", result.DefenderPower)
	}
}

func TestFortifiedRegionGivesDefenderBonus(t *testing.T) {
	setupTestWorld(t)
	wipeAndAddUnits(t,
		snap("gondor-army", "FREE_PEOPLES", "minas-tirith", 5),
	)

	worldStateMu.Lock()
	region := WorldState.Regions["minas-tirith"]
	region.Fortified = true
	WorldState.Regions["minas-tirith"] = region
	worldStateMu.Unlock()

	modifier := calculateCombatModifier("gondor-army", "minas-tirith", false)
	if modifier < 2 {
		t.Fatalf("expected fortified defender modifier >= 2, got %d", modifier)
	}
}

func TestMaiaAbilityGandalfTemporarilyOpensPath(t *testing.T) {
	setupTestWorld(t) // full config needed for Maia dispatch

	order := OrderPayload{
		OrderType: OrderMaiaAbility,
		PlayerID:  "light",
		UnitID:    "gandalf",
		Turn:      1,
		PathID:    "minas-tirith-to-osgiliath",
	}

	msg, err := ApplyMaiaAbilityOrder(order)
	if err != nil {
		t.Fatalf("expected Gandalf ability to succeed, got: %v", err)
	}
	if msg == "" {
		t.Fatal("expected non-empty action message")
	}

	path, ok := getPathState("minas-tirith-to-osgiliath")
	if !ok {
		t.Fatal("expected path state to exist")
	}
	if path.Status != "TEMPORARILY_OPEN" {
		t.Fatalf("expected TEMPORARILY_OPEN, got %s", path.Status)
	}
}

func TestMaiaAbilitySarumanThreatensPath(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderMaiaAbility,
		PlayerID:  "shadow",
		UnitID:    "saruman",
		Turn:      1,
		PathID:    "fords-of-isen-to-isengard",
	}

	msg, err := ApplyMaiaAbilityOrder(order)
	if err != nil {
		t.Fatalf("expected Saruman ability to succeed, got: %v", err)
	}
	if msg == "" {
		t.Fatal("expected non-empty action message")
	}

	path, ok := getPathState("fords-of-isen-to-isengard")
	if !ok {
		t.Fatal("expected path state to exist")
	}
	if path.Status != "THREATENED" {
		t.Fatalf("expected THREATENED, got %s", path.Status)
	}
	if path.SurveillanceLevel != 3 {
		t.Fatalf("expected surveillanceLevel=3, got %d", path.SurveillanceLevel)
	}
}

func TestMaiaDisabledAfterIsengardFalls(t *testing.T) {
	setupTestWorld(t)

	worldStateMu.Lock()
	r := WorldState.Regions["isengard"]
	r.ControlledBy = "FREE_PEOPLES"
	WorldState.Regions["isengard"] = r
	worldStateMu.Unlock()

	order := OrderPayload{
		OrderType: OrderMaiaAbility,
		PlayerID:  "shadow",
		UnitID:    "saruman",
		Turn:      1,
		PathID:    "fords-of-isen-to-isengard",
	}

	_, err := ApplyMaiaAbilityOrder(order)
	if err == nil {
		t.Fatal("expected MAIA_DISABLED error when Isengard has fallen, got nil")
	}
}

func TestRespawningUnitStatusAfterFatalDamage(t *testing.T) {
	setupTestWorld(t)

	var respawnID string
	for _, u := range LoadedUnits {
		if u.Respawns && !u.Indestructible {
			respawnID = u.ID
			break
		}
	}
	if respawnID == "" {
		t.Skip("no respawning unit in config")
	}

	worldStateMu.Lock()
	WorldState.Units = make(map[string]UnitSnapshot)
	WorldState.Units[respawnID] = UnitSnapshot{
		ID: respawnID, Status: "ACTIVE", Strength: 1,
		Region: "minas-morgul",
	}
	worldStateMu.Unlock()

	applyDamage(respawnID, 5)

	worldStateMu.RLock()
	result := WorldState.Units[respawnID]
	timer := WorldState.RespawnTimers[respawnID]
	worldStateMu.RUnlock()

	if result.Status != "RESPAWNING" {
		t.Fatalf("expected status RESPAWNING, got %s", result.Status)
	}
	if timer <= 0 {
		t.Fatalf("expected respawn timer > 0, got %d", timer)
	}
}
