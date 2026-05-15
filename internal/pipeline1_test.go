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