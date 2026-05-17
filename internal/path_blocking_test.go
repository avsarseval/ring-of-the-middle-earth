package internal

import "testing"

func TestBlockPathSetsPathStatusBlocked(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "witch-king",
		Turn:      1,
		PathID:    "bree-to-rivendell",
	}

	msg, err := ApplyBlockPathOrder(order)
	if err != nil {
		t.Fatalf("expected block path order to succeed, got error: %v", err)
	}

	if msg == "" {
		t.Fatal("expected block path message")
	}

	path, ok := getPathState("bree-to-rivendell")
	if !ok {
		t.Fatal("expected path state to exist")
	}

	if path.Status != "BLOCKED" {
		t.Fatalf("expected path status BLOCKED, got %s", path.Status)
	}
}

func TestPathBlockStaysWhenBlockingUnitStillAtEndpoint(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "witch-king",
		Turn:      1,
		PathID:    "bree-to-rivendell",
	}

	if _, err := ApplyBlockPathOrder(order); err != nil {
		t.Fatalf("expected block path order to succeed, got error: %v", err)
	}

	// bree-to-rivendell endpointlerinden biri "rivendell".
	// Blocking unit hâlâ endpointteyse block kalkmamalı.
	RevertPathBlocksForMovingUnit("witch-king", "rivendell")

	path, ok := getPathState("bree-to-rivendell")
	if !ok {
		t.Fatal("expected path state to exist")
	}

	if path.Status != "BLOCKED" {
		t.Fatalf("expected path to remain BLOCKED while blocker is still endpoint, got %s", path.Status)
	}
}

func TestPathBlockRevertsWhenBlockingUnitLeavesEndpoint(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "witch-king",
		Turn:      1,
		PathID:    "bree-to-rivendell",
	}

	if _, err := ApplyBlockPathOrder(order); err != nil {
		t.Fatalf("expected block path order to succeed, got error: %v", err)
	}

	pathBefore, ok := getPathState("bree-to-rivendell")
	if !ok {
		t.Fatal("expected path state to exist before revert")
	}

	if pathBefore.Status != "BLOCKED" {
		t.Fatalf("expected path status BLOCKED before revert, got %s", pathBefore.Status)
	}

	// bree-to-rivendell endpointleri bree ve rivendell.
	// Unit moria gibi endpoint olmayan bir bölgeye geçince block kalkmalı.
	RevertPathBlocksForMovingUnit("witch-king", "moria")

	pathAfter, ok := getPathState("bree-to-rivendell")
	if !ok {
		t.Fatal("expected path state to exist after revert")
	}

	if pathAfter.Status != "OPEN" {
		t.Fatalf("expected path status OPEN after blocker leaves endpoint, got %s", pathAfter.Status)
	}
}