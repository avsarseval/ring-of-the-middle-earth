package internal

import "testing"

// All three tests use nazgul-2 at minas-morgul (default start region).
// minas-morgul has NO FellowshipGuard units, so the B6 guard does not trigger.
//
// Previous tests used witch-king at rivendell with path bree-to-rivendell.
// That broke after the B6 fix because legolas and gimli start at rivendell
// (both FellowshipGuard class), correctly triggering BLOCK_PREVENTED.
//
// Chosen path set:
//   Test 1 & 2: minas-morgul-to-mordor    (endpoints: minas-morgul, mordor)
//   Test 3:     minas-morgul-to-cirith-ungol (different path for revert test)
// Both endpoints of each path are free of FellowshipGuard units.

// TestBlockPathSetsPathStatusBlocked — B6 Case 1
// Nazgul at endpoint submits BLOCK_PATH → path becomes BLOCKED.
func TestBlockPathSetsPathStatusBlocked(t *testing.T) {
	setupTestWorld(t)

	// Place nazgul-2 at minas-morgul (already its default start region;
	// explicit set here for test clarity and independence)
	worldStateMu.Lock()
	u := WorldState.Units["nazgul-2"]
	u.Region = "minas-morgul"
	u.Status = "ACTIVE"
	WorldState.Units["nazgul-2"] = u
	worldStateMu.Unlock()

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "nazgul-2",
		Turn:      1,
		PathID:    "minas-morgul-to-mordor",
	}

	msg, err := ApplyBlockPathOrder(order)
	if err != nil {
		t.Fatalf("expected block path order to succeed (no FellowshipGuard at minas-morgul), got error: %v", err)
	}
	if msg == "" {
		t.Fatal("expected non-empty block confirmation message")
	}

	path, ok := getPathState("minas-morgul-to-mordor")
	if !ok {
		t.Fatal("expected path state to exist")
	}
	if path.Status != "BLOCKED" {
		t.Fatalf("expected path status BLOCKED, got %s", path.Status)
	}
}

// TestPathBlockStaysWhenBlockingUnitStillAtEndpoint — B6 Case 2
// After blocking, if the blocking unit stays at an endpoint, the block must persist.
func TestPathBlockStaysWhenBlockingUnitStillAtEndpoint(t *testing.T) {
	setupTestWorld(t)

	worldStateMu.Lock()
	u := WorldState.Units["nazgul-2"]
	u.Region = "minas-morgul"
	u.Status = "ACTIVE"
	WorldState.Units["nazgul-2"] = u
	worldStateMu.Unlock()

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "nazgul-2",
		Turn:      1,
		PathID:    "minas-morgul-to-mordor",
	}

	if _, err := ApplyBlockPathOrder(order); err != nil {
		t.Fatalf("block setup failed: %v", err)
	}

	// minas-morgul-to-mordor endpoints: minas-morgul and mordor.
	// Unit stays at minas-morgul → block must NOT revert.
	RevertPathBlocksForMovingUnit("nazgul-2", "minas-morgul")

	path, ok := getPathState("minas-morgul-to-mordor")
	if !ok {
		t.Fatal("expected path state to exist")
	}
	if path.Status != "BLOCKED" {
		t.Fatalf("expected path to remain BLOCKED while blocker is still at endpoint, got %s", path.Status)
	}
}

// TestPathBlockRevertsWhenBlockingUnitLeavesEndpoint — B6 Case 3
// When the blocking unit moves to a non-endpoint region, the block must revert to OPEN.
func TestPathBlockRevertsWhenBlockingUnitLeavesEndpoint(t *testing.T) {
	setupTestWorld(t)

	worldStateMu.Lock()
	u := WorldState.Units["nazgul-2"]
	u.Region = "minas-morgul"
	u.Status = "ACTIVE"
	WorldState.Units["nazgul-2"] = u
	worldStateMu.Unlock()

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "nazgul-2",
		Turn:      1,
		PathID:    "minas-morgul-to-mordor",
	}

	if _, err := ApplyBlockPathOrder(order); err != nil {
		t.Fatalf("block setup failed: %v", err)
	}

	pathBefore, ok := getPathState("minas-morgul-to-mordor")
	if !ok {
		t.Fatal("expected path state to exist before revert")
	}
	if pathBefore.Status != "BLOCKED" {
		t.Fatalf("expected BLOCKED before revert, got %s", pathBefore.Status)
	}

	// minas-morgul-to-mordor endpoints: minas-morgul and mordor.
	// Moving to osgiliath (not an endpoint) → block must revert to OPEN.
	RevertPathBlocksForMovingUnit("nazgul-2", "osgiliath")

	pathAfter, ok := getPathState("minas-morgul-to-mordor")
	if !ok {
		t.Fatal("expected path state to exist after revert")
	}
	if pathAfter.Status != "OPEN" {
		t.Fatalf("expected path status OPEN after blocker leaves endpoint, got %s", pathAfter.Status)
	}
}

// TestBlockPathFailsWhenFellowshipGuardPresent — B6 extra (documents the rule being enforced)
// Proves the B6 guard works: a Nazgul cannot block a path when a FellowshipGuard
// is present at the same endpoint region.
func TestBlockPathFailsWhenFellowshipGuardPresent(t *testing.T) {
	setupTestWorld(t)

	// Place witch-king at weathertop (no FellowshipGuard by default)
	// then move a FellowshipGuard (legolas) to weathertop to trigger the guard
	worldStateMu.Lock()
	wk := WorldState.Units["witch-king"]
	wk.Region = "weathertop"
	wk.Status = "ACTIVE"
	WorldState.Units["witch-king"] = wk

	leg := WorldState.Units["legolas"]
	leg.Region = "weathertop" // FellowshipGuard now co-located
	leg.Status = "ACTIVE"
	WorldState.Units["legolas"] = leg
	worldStateMu.Unlock()

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "shadow",
		UnitID:    "witch-king",
		Turn:      1,
		PathID:    "bree-to-weathertop", // weathertop is an endpoint
	}

	_, err := ApplyBlockPathOrder(order)
	if err == nil {
		t.Fatal("expected BLOCK_PREVENTED error when FellowshipGuard is at endpoint, got nil")
	}

	// Path must remain OPEN — the block was prevented
	path, ok := getPathState("bree-to-weathertop")
	if !ok {
		t.Fatal("expected path state to exist")
	}
	if path.Status == "BLOCKED" {
		t.Fatal("path must not be BLOCKED when block was prevented by FellowshipGuard")
	}
}
