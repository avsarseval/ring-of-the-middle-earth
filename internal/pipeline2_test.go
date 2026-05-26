package internal

import "testing"

func TestPipeline2InterceptAnalysisReturnsPlans(t *testing.T) {
	setupTestWorld(t)

	response := AnalyzeInterceptionForDarkSide()

	if response.PlayerID != "shadow" {
		t.Fatalf("expected playerId shadow, got %s", response.PlayerID)
	}

	if len(response.Plans) == 0 {
		t.Fatal("expected at least one intercept plan")
	}

	for _, plan := range response.Plans {
		if plan.UnitID == "" {
			t.Fatal("expected intercept plan unitId to be set")
		}

		if plan.TargetRegion == "" {
			t.Fatal("expected intercept plan targetRegion to be set")
		}

		if plan.TurnsToIntercept < 0 {
			t.Fatalf("turnsToIntercept should not be negative, got %d", plan.TurnsToIntercept)
		}
	}
}

func TestActiveNazgulUnitsExist(t *testing.T) {
	setupTestWorld(t)

	nazgul := GetActiveNazgulUnits()

	if len(nazgul) == 0 {
		t.Fatal("expected active Nazgul units")
	}

	for _, unit := range nazgul {
		// FIX B1: check config.DetectionRange > 0, not class name
		cfg, ok := getUnitConfig(unit.ID)
		if !ok || cfg.DetectionRange == 0 {
			t.Fatalf("expected detection-capable unit (DetectionRange>0), got unit %s", unit.ID)
		}

		if unit.Status != "ACTIVE" {
			t.Fatalf("expected active Nazgul, got status %s for unit %s", unit.Status, unit.ID)
		}
	}
}
// TestPipeline2PositiveInterceptWindowScoreAboveZero is rubric §35 pipeline2_test.go Case 1.
// "Positive intercept window → score > 0"
func TestPipeline2PositiveInterceptWindowScoreAboveZero(t *testing.T) {
	setupTestWorld(t)

	// witch-king at minas-morgul (1 hop from cirith-ungol)
	// Ring Bearer approaching cirith-ungol via ithilien-to-cirith-ungol (cost=2)
	// rbTurnsToReach=2, turnsToIntercept=1 → interceptWindow=2-1=1 > 0 → score > 0
	worldStateMu.Lock()
	wk := WorldState.Units["witch-king"]
	wk.Region = "minas-morgul"
	wk.Status = "ACTIVE"
	WorldState.Units["witch-king"] = wk
	worldStateMu.Unlock()

	job := InterceptJob{
		Unit: WorldState.Units["witch-king"],
		Route: RouteCandidate{
			Name:    "Test Route",
			PathIDs: []string{"ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		},
	}

	plans := computeInterceptPlans(job)

	foundPositive := false
	for _, p := range plans {
		if p.Score > 0 {
			foundPositive = true
			break
		}
	}

	if !foundPositive {
		t.Fatal("expected at least one plan with score > 0 when intercept window is positive")
	}
}

// TestPipeline2NegativeInterceptWindowScoreIsZero is rubric §35 pipeline2_test.go Case 2.
// "Negative intercept window → score = 0.0"
func TestPipeline2NegativeInterceptWindowScoreIsZero(t *testing.T) {
	setupTestWorld(t)

	// witch-king at the-shire (far from mount-doom); RB near mount-doom
	// turnsToIntercept >> rbTurnsToReach → interceptWindow < 0 → score = 0.0
	worldStateMu.Lock()
	wk := WorldState.Units["witch-king"]
	wk.Region = "the-shire"
	wk.Status = "ACTIVE"
	WorldState.Units["witch-king"] = wk
	worldStateMu.Unlock()

	job := InterceptJob{
		Unit: wk,
		Route: RouteCandidate{
			Name:    "Short Route",
			PathIDs: []string{"mordor-to-mount-doom"},
		},
	}

	plans := computeInterceptPlans(job)

	for _, p := range plans {
		if p.Score > 0 {
			t.Fatalf("expected score=0 when intercept window is negative, got score=%f for region=%s",
				p.Score, p.TargetRegion)
		}
	}
}
