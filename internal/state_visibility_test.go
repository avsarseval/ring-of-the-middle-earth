package internal

import (
	"encoding/json"
	"testing"
)

func TestLightSideCanSeeRingBearerRegion(t *testing.T) {
	setupTestWorld(t)

	payload, err := GetPublicWorldStateJSON("light")
	if err != nil {
		t.Fatalf("state json oluşturulamadı: %v", err)
	}

	var state WorldStateCache
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatalf("state json parse edilemedi: %v", err)
	}

	ringBearer := state.Units[RingBearerID]

	if ringBearer.Region == "" {
		t.Fatal("light side should see ring-bearer region")
	}

	if state.LightView.RingBearerRegion == "" {
		t.Fatal("light side should see lightView.ringBearerRegion")
	}
}

func TestDarkSideCannotSeeRingBearerRegion(t *testing.T) {
	setupTestWorld(t)

	payload, err := GetPublicWorldStateJSON("shadow")
	if err != nil {
		t.Fatalf("state json oluşturulamadı: %v", err)
	}

	var state WorldStateCache
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatalf("state json parse edilemedi: %v", err)
	}

	ringBearer := state.Units[RingBearerID]

	if ringBearer.Region != "" {
		t.Fatalf("dark side should not see ring-bearer region, got %s", ringBearer.Region)
	}

	if state.LightView.RingBearerRegion != "" {
		t.Fatalf("dark side should not see lightView.ringBearerRegion, got %s", state.LightView.RingBearerRegion)
	}

	if state.DarkView.RingBearerRegion != "" {
		t.Fatalf("dark side should not see darkView.ringBearerRegion, got %s", state.DarkView.RingBearerRegion)
	}
}