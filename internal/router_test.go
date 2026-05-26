package internal

import (
	"encoding/json"
	"testing"
)

func TestStripRingBearerRemovesRegionFromBroadcastSnapshot(t *testing.T) {
	setupTestWorld(t)

	snapshot := BroadcastWorldStateSnapshot{
		EventType: "WorldStateSnapshot",
		Turn:      GetCurrentTurn(),
		State:     GetWorldStateCopy(),
		Timestamp: 123456,
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("snapshot marshal edilemedi: %v", err)
	}

	event := Event{
		Topic:   TopicBroadcast,
		Payload: payload,
	}

	stripped := stripRingBearer(event)

	var result map[string]interface{}
	if err := json.Unmarshal(stripped.Payload, &result); err != nil {
		t.Fatalf("stripped payload parse edilemedi: %v", err)
	}

	state, ok := result["state"].(map[string]interface{})
	if !ok {
		t.Fatal("state alanı bulunamadı")
	}

	units, ok := state["units"].(map[string]interface{})
	if !ok {
		t.Fatal("units alanı bulunamadı")
	}

	ringBearer, ok := units[RingBearerID].(map[string]interface{})
	if !ok {
		t.Fatal("ring-bearer bulunamadı")
	}

	if ringBearer["region"] != "" {
		t.Fatalf("dark payload içinde ring-bearer region boş olmalıydı, got=%v", ringBearer["region"])
	}

	lightView, ok := state["lightView"].(map[string]interface{})
	if !ok {
		t.Fatal("lightView alanı bulunamadı")
	}

	if lightView["ringBearerRegion"] != "" {
		t.Fatalf("dark payload içinde lightView.ringBearerRegion boş olmalıydı, got=%v", lightView["ringBearerRegion"])
	}

	darkView, ok := state["darkView"].(map[string]interface{})
	if !ok {
		t.Fatal("darkView alanı bulunamadı")
	}

	if darkView["ringBearerRegion"] != "" {
		t.Fatalf("dark payload içinde darkView.ringBearerRegion boş olmalıydı, got=%v", darkView["ringBearerRegion"])
	}
}

func TestStripRingBearerDoesNotMutateWorldState(t *testing.T) {
	setupTestWorld(t)

	before := WorldState.Units[RingBearerID].Region
	if before == "" {
		t.Fatal("test başlangıcında Ring Bearer region boş olmamalı")
	}

	snapshot := BroadcastWorldStateSnapshot{
		EventType: "WorldStateSnapshot",
		Turn:      GetCurrentTurn(),
		State:     GetWorldStateCopy(),
		Timestamp: 123456,
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("snapshot marshal edilemedi: %v", err)
	}

	event := Event{
		Topic:   TopicBroadcast,
		Payload: payload,
	}

	_ = stripRingBearer(event)

	after := WorldState.Units[RingBearerID].Region
	if after != before {
		t.Fatalf("stripRingBearer WorldState'i mutate etmemeli. before=%s after=%s", before, after)
	}
}
// TestDarkViewRingBearerRegionAlwaysEmpty is rubric §35 router_test.go Case 3.
// "cache.DarkView.RingBearerRegion is always '' after any cache update"
// Run with: go test -race -run TestDarkViewRingBearerRegionAlwaysEmpty
func TestDarkViewRingBearerRegionAlwaysEmpty(t *testing.T) {
	setupTestWorld(t)

	// Simulate a buggy write that sets the region (should never happen in prod)
	worldStateMu.Lock()
	WorldState.DarkView.RingBearerRegion = "mordor"
	worldStateMu.Unlock()

	// updateDarkDetectionView is called at end of RunDetection — it must reset the field
	updateDarkDetectionView("mordor", 1)

	worldStateMu.RLock()
	got := WorldState.DarkView.RingBearerRegion
	worldStateMu.RUnlock()

	if got != "" {
		t.Fatalf("DarkView.RingBearerRegion must always be '' after any cache update, got %q", got)
	}

	// Also verify GetPublicWorldStateJSON enforces the invariant for shadow side
	payload, err := GetPublicWorldStateJSON("shadow")
	if err != nil {
		t.Fatalf("GetPublicWorldStateJSON failed: %v", err)
	}
	var state WorldStateCache
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if state.DarkView.RingBearerRegion != "" {
		t.Fatalf("shadow JSON darkView.ringBearerRegion must be '', got %q", state.DarkView.RingBearerRegion)
	}
}
