package internal

import "testing"

func TestPipeline1RouteAnalysisReturnsRecommendedRoute(t *testing.T) {
	setupTestWorld(t)

	response := AnalyzeRoutesForLightSide()

	if response.PlayerID != "light" {
		t.Fatalf("expected playerId light, got %s", response.PlayerID)
	}

	if response.UnitID != RingBearerID {
		t.Fatalf("expected unitId %s, got %s", RingBearerID, response.UnitID)
	}

	if len(response.Routes) == 0 {
		t.Fatal("expected at least one route candidate")
	}

	if response.Recommended.Name == "" {
		t.Fatal("expected recommended route to be set")
	}

	for i := 1; i < len(response.Routes); i++ {
		if response.Routes[i].RiskScore < response.Routes[i-1].RiskScore {
			t.Fatalf("routes are not sorted by riskScore ascending")
		}
	}
}

func TestCalculateRouteRiskReturnsNonNegativeScore(t *testing.T) {
	setupTestWorld(t)

	score, threatened, blocked := CalculateRouteRisk(RingBearerID, []string{"shire-to-bree"})

	if score < 0 {
		t.Fatalf("risk score should not be negative, got %d", score)
	}

	if threatened == nil {
		t.Fatal("threatenedPaths should not be nil")
	}

	if blocked == nil {
		t.Fatal("blockedPaths should not be nil")
	}
}
// TestPipeline1KnownThreatAndSurveillanceValues is rubric §35 pipeline1_test.go Case 1.
// "Route with known threat and surveillance values → correct riskScore computed"
func TestPipeline1KnownThreatAndSurveillanceValues(t *testing.T) {
	setupTestWorld(t)

	// Set up deterministic state: bree threatLevel=2, weathertop threatLevel=2
	// shire-to-bree surveillanceLevel=1
	worldStateMu.Lock()
	if r, ok := WorldState.Regions["bree"]; ok {
		r.ThreatLevel = 2
		WorldState.Regions["bree"] = r
	}
	if r, ok := WorldState.Regions["weathertop"]; ok {
		r.ThreatLevel = 2
		WorldState.Regions["weathertop"] = r
	}
	if p, ok := WorldState.Paths["shire-to-bree"]; ok {
		p.SurveillanceLevel = 1
		WorldState.Paths["shire-to-bree"] = p
	}
	if p, ok := WorldState.Paths["bree-to-weathertop"]; ok {
		p.SurveillanceLevel = 0
		WorldState.Paths["bree-to-weathertop"] = p
	}
	// Remove all units from route regions so nazgulProximityCount = 0
	for id, u := range WorldState.Units {
		cfg, ok := WorldState.UnitConfigs[id]
		if ok && cfg.DetectionRange > 0 {
			u.Region = "mordor" // park all Nazgul far away
			WorldState.Units[id] = u
		}
	}
	worldStateMu.Unlock()

	pathIDs := []string{"shire-to-bree", "bree-to-weathertop"}
	score, _, _ := CalculateRouteRisk(RingBearerID, pathIDs)

	// Expected:
	// regionThreat: bree=2 + weathertop=2 = 4
	// surveillance: shire-to-bree=1*3=3, bree-to-weathertop=0*3=0 → 3
	// threatened/blocked: 0
	// nazgulProximity: 0 (all Nazgul at mordor, far from route)
	// total = 4 + 3 = 7
	expected := 7
	if score != expected {
		t.Fatalf("expected riskScore=%d (threatLevel 4 + surveillance 3), got %d", expected, score)
	}
}

// TestPipeline1NazgulProximityAddsToScore is rubric §35 pipeline1_test.go Case 2.
// "Nazgul within 2 hops → proximity count adds correctly to score"
func TestPipeline1NazgulProximityAddsToScore(t *testing.T) {
	setupTestWorld(t)

	// Clear all threat/surveillance so only proximity contributes
	worldStateMu.Lock()
	for id, r := range WorldState.Regions {
		r.ThreatLevel = 0
		WorldState.Regions[id] = r
	}
	for id, p := range WorldState.Paths {
		p.SurveillanceLevel = 0
		p.Status = "OPEN"
		WorldState.Paths[id] = p
	}
	// Place witch-king at bree (1 hop from shire — within 2 hops of bree destination)
	wk := WorldState.Units["witch-king"]
	wk.Region = "bree"
	wk.Status = "ACTIVE"
	WorldState.Units["witch-king"] = wk
	// Move all other detection-capable units out
	for id, u := range WorldState.Units {
		if id == "witch-king" { continue }
		cfg, ok := WorldState.UnitConfigs[id]
		if ok && cfg.DetectionRange > 0 {
			u.Region = "mount-doom"
			WorldState.Units[id] = u
		}
	}
	worldStateMu.Unlock()

	// Route: shire-to-bree → destination is bree
	// witch-king is AT bree (distance=0 ≤ 2) → nazgulProximityCount=1 → +2
	scoreWithProximity, _, _ := CalculateRouteRisk(RingBearerID, []string{"shire-to-bree"})

	// Move witch-king far away and recompute
	worldStateMu.Lock()
	wk2 := WorldState.Units["witch-king"]
	wk2.Region = "mount-doom"
	WorldState.Units["witch-king"] = wk2
	worldStateMu.Unlock()

	scoreWithoutProximity, _, _ := CalculateRouteRisk(RingBearerID, []string{"shire-to-bree"})

	diff := scoreWithProximity - scoreWithoutProximity
	if diff != 2 {
		t.Fatalf("expected Nazgul within 2 hops to add exactly 2 to score, got diff=%d (with=%d without=%d)",
			diff, scoreWithProximity, scoreWithoutProximity)
	}
}
