package internal

import "testing"

func TestResolveCombatReturnsWinner(t *testing.T) {
	setupTestWorld(t)

	result := ResolveCombat("aragorn", "uruk-hai-legion", "bree")

	if result.AttackerID != "aragorn" {
		t.Fatalf("expected attacker aragorn, got %s", result.AttackerID)
	}

	if result.DefenderID != "uruk-hai-legion" {
		t.Fatalf("expected defender uruk-hai-legion, got %s", result.DefenderID)
	}

	if result.TargetRegion != "bree" {
		t.Fatalf("expected targetRegion bree, got %s", result.TargetRegion)
	}

	if result.AttackerPower <= 0 {
		t.Fatalf("expected attacker power > 0, got %d", result.AttackerPower)
	}

	if result.DefenderPower <= 0 {
		t.Fatalf("expected defender power > 0, got %d", result.DefenderPower)
	}

	if result.WinnerID == "" && result.Reason != "draw" {
		t.Fatalf("expected draw reason when no winner, got reason=%s", result.Reason)
	}
}

func TestFortifiedRegionGivesDefenderBonus(t *testing.T) {
	setupTestWorld(t)

	worldStateMu.Lock()
	region := WorldState.Regions["minas-tirith"]
	region.Fortified = true
	WorldState.Regions["minas-tirith"] = region
	worldStateMu.Unlock()

	modifier := calculateCombatModifier("gondor-army", "minas-tirith", false)

	if modifier < 2 {
		t.Fatalf("expected fortified defender modifier at least 2, got %d", modifier)
	}
}

func TestLeadershipBonusAppliedForNonMaiaSameRegion(t *testing.T) {
	setupTestWorld(t)

	// Gandalf and gondor-army start in minas-tirith in current config.
	modifier := calculateCombatModifier("gondor-army", "minas-tirith", false)

	if modifier < 1 {
		t.Fatalf("expected leadership/control modifier >= 1, got %d", modifier)
	}
}

func TestMaiaAbilityGandalfTemporarilyOpensPath(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderMaiaAbility,
		PlayerID:  "light",
		UnitID:    "gandalf",
		Turn:      1,
		PathID:    "minas-tirith-to-osgiliath",
	}

	msg, err := ApplyMaiaAbilityOrder(order)
	if err != nil {
		t.Fatalf("expected Gandalf ability to succeed, got error: %v", err)
	}

	if msg == "" {
		t.Fatal("expected action message")
	}

	path, ok := getPathState("minas-tirith-to-osgiliath")
	if !ok {
		t.Fatal("expected path state to exist")
	}

	if path.Status != "TEMPORARILY_OPEN" {
		t.Fatalf("expected path status TEMPORARILY_OPEN, got %s", path.Status)
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
		t.Fatalf("expected Saruman ability to succeed, got error: %v", err)
	}

	if msg == "" {
		t.Fatal("expected action message")
	}

	path, ok := getPathState("fords-of-isen-to-isengard")
	if !ok {
		t.Fatal("expected path state to exist")
	}

	if path.Status != "THREATENED" {
		t.Fatalf("expected path status THREATENED, got %s", path.Status)
	}

	if path.SurveillanceLevel != 3 {
		t.Fatalf("expected surveillanceLevel 3, got %d", path.SurveillanceLevel)
	}
}