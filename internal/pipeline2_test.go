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
		if unit.Class != "Nazgul" {
			t.Fatalf("expected Nazgul class, got %s for unit %s", unit.Class, unit.ID)
		}

		if unit.Status != "ACTIVE" {
			t.Fatalf("expected active Nazgul, got status %s for unit %s", unit.Status, unit.ID)
		}
	}
}