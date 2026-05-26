package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	RingBearerID = "ring-bearer"
	MountDoomID  = "mount-doom"

	// Order type constants (FIX: added missing types)
	OrderDestroyRing      = "DESTROY_RING"
	OrderDeployNazgul     = "DEPLOY_NAZGUL"
	OrderReinforceRegion  = "REINFORCE_REGION"
)

// NodeOccupants is a fast region→unitId lookup.
// Note: single-occupant per region (prototype limitation — real impl uses []unitId).
var NodeOccupants = map[string]string{}


// ─────────────────────────────────────────────────────────────────────────────
// KTABLE STATE RECOVERY (B2 / Q&A Q7)
// Called synchronously at startup, BEFORE the main consumer group joins.
// ─────────────────────────────────────────────────────────────────────────────

// SessionSnapshot is the structure written to game.session by publishSessionState.
// Must match the fields serialised in publishSessionState exactly.
type SessionSnapshot struct {
	Turn      int                        `json:"turn"`
	Timestamp int64                      `json:"timestamp"`
	Units     map[string]UnitSnapshot    `json:"units"`
	Regions   map[string]RegionState     `json:"regions"`
	Paths     map[string]PathState       `json:"paths"`
}

// RestoreWorldStateFromSnapshot deserialises a game.session payload and
// overwrites the in-memory WorldState.
//
// Called synchronously during startup so the engine answers HTTP requests
// with the correct state immediately, without waiting for Kafka events to
// replay the full event history.
func RestoreWorldStateFromSnapshot(data []byte) error {
	var snap SessionSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("session snapshot unmarshal failed: %w", err)
	}

	if snap.Turn == 0 {
		// Defensive: a zero turn means a corrupt or pre-turn-1 snapshot
		return fmt.Errorf("session snapshot has turn=0 — ignoring")
	}

	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	// Restore turn counter
	currentTurnMu.Lock()
	CurrentTurn = snap.Turn
	currentTurnMu.Unlock()
	WorldState.Turn = snap.Turn

	// Restore units — preserve UnitConfigs (read-only, loaded from config files)
	if len(snap.Units) > 0 {
		for id, unit := range snap.Units {
			WorldState.Units[id] = unit
			// Rebuild NodeOccupants from restored unit positions
			if unit.Status == "ACTIVE" && unit.Region != "" {
				NodeOccupants[unit.Region] = id
			}
			// Restore Ring Bearer light-side view
			if id == RingBearerID {
				WorldState.LightView.RingBearerRegion = unit.Region
				WorldState.DarkView.RingBearerRegion = "" // invariant
			}
		}
	}

	// Restore regions
	if len(snap.Regions) > 0 {
		for id, region := range snap.Regions {
			WorldState.Regions[id] = region
		}
	}

	// Restore paths
	if len(snap.Paths) > 0 {
		for id, path := range snap.Paths {
			WorldState.Paths[id] = path
			// Sync PathStatus fast-lookup map
			if path.Status != "OPEN" {
				PathStatus[id] = path.Status
			}
		}
	}

	WorldState.UpdatedAt = time.Now().UnixMilli()

	fmt.Printf("✅ State recovered from game.session: turn=%d units=%d regions=%d paths=%d\n",
		snap.Turn, len(snap.Units), len(snap.Regions), len(snap.Paths))

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// PENDING ORDER BUFFER
// FIX B10: Orders are buffered here and processed together at turn end
// in the correct 13-step order (spec §6), not immediately on arrival.
// ─────────────────────────────────────────────────────────────────────────────

var (
	pendingOrdersMu sync.Mutex
	pendingOrders   []OrderPayload
	// Track which DESTROY_RING orders arrived this turn (for win condition Step 13)
	destroyRingThisTurn bool
)

// AddPendingOrder buffers a validated order for processing at turn end.
func AddPendingOrder(event Event) {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		fmt.Printf("⚠️ Could not buffer order: %v\n", err)
		return
	}
	pendingOrdersMu.Lock()
	pendingOrders = append(pendingOrders, order)
	if order.OrderType == OrderDestroyRing {
		destroyRingThisTurn = true
	}
	pendingOrdersMu.Unlock()
	fmt.Printf("📦 Order buffered: %s unit=%s\n", order.OrderType, order.UnitID)
}

func drainPendingOrders() []OrderPayload {
	pendingOrdersMu.Lock()
	defer pendingOrdersMu.Unlock()
	orders := pendingOrders
	pendingOrders = nil
	destroyRingThisTurn = false
	return orders
}

// ─────────────────────────────────────────────────────────────────────────────
// 13-STEP TURN PROCESSOR (spec §6)
// FIX B10: Full turn processing previously missing — ProcessTurn was only 1-step.
// ─────────────────────────────────────────────────────────────────────────────

// RunFullTurnProcessing executes all 13 steps of a turn.
// Called by the coordinator goroutine's time.After(60s) case.
func RunFullTurnProcessing(producer *kafka.Producer) {
	turn := GetCurrentTurn()
	fmt.Printf("\n⚙️ ====== Turn %d Processing Started ======\n", turn)

	// Step 1: Collect all validated orders for this turn
	orders := drainPendingOrders()
	fmt.Printf("📋 Step 1: %d orders collected\n", len(orders))

	// Step 2: Process AssignRoute and RedirectUnit
	fmt.Println("📋 Step 2: AssignRoute / RedirectUnit")
	for _, o := range orders {
		switch o.OrderType {
		case OrderAssignRoute:
			AssignUnitRoute(normalizedUnitID(o), o.PathIDs)
		case OrderRedirectUnit:
			RedirectUnitRoute(normalizedUnitID(o), o.NewPathIDs)
		}
	}

	// Step 3: Process BlockPath and SearchPath
	fmt.Println("📋 Step 3: BlockPath / SearchPath")
	for _, o := range orders {
		switch o.OrderType {
		case OrderBlockPath:
			if _, err := ApplyBlockPathOrder(o); err != nil {
				fmt.Printf("⚠️ BlockPath error: %v\n", err)
			}
		case OrderSearchPath:
			if _, err := ApplySearchPathOrder(o); err != nil {
				fmt.Printf("⚠️ SearchPath error: %v\n", err)
			}
		}
	}

	// Step 4: Process ReinforceRegion and DeployNazgul
	fmt.Println("📋 Step 4: ReinforceRegion / DeployNazgul (simplified)")
	for _, o := range orders {
		switch o.OrderType {
		case OrderReinforceRegion, OrderDeployNazgul:
			// Move unit to target region
			unitID := normalizedUnitID(o)
			targetRegion := o.TargetRegion
			if targetRegion != "" {
				srcRegion, _ := getUnitCurrentRegion(unitID)
				if areAdjacent(srcRegion, targetRegion) {
					NodeOccupants[targetRegion] = unitID
					NodeOccupants[srcRegion] = ""
					UpdateUnitRegion(unitID, targetRegion)
					fmt.Printf("🚶 %s reinforced to %s\n", unitID, targetRegion)
				}
			}
		}
	}

	// Step 5: Process FortifyRegion — sets fortified=true, timer=2
	fmt.Println("📋 Step 5: FortifyRegion")
	for _, o := range orders {
		if o.OrderType == OrderFortifyRegion {
			if _, err := ApplyFortifyRegionOrder(o); err != nil {
				fmt.Printf("⚠️ FortifyRegion error: %v\n", err)
			}
		}
	}

	// Step 6: Process MaiaAbility orders
	fmt.Println("📋 Step 6: MaiaAbility")
	for _, o := range orders {
		if o.OrderType == OrderMaiaAbility {
			if _, err := ApplyMaiaAbilityOrder(o); err != nil {
				fmt.Printf("⚠️ MaiaAbility error: %v\n", err)
			}
		}
	}

	// Step 7: Auto-advance all units with assigned routes
	fmt.Println("📋 Step 7: Auto-advance units")
	autoAdvanceAllUnits(producer)

	// Step 8: Process AttackRegion
	fmt.Println("📋 Step 8: AttackRegion")
	for _, o := range orders {
		if o.OrderType == OrderAttackRegion {
			if msg, err := ApplyAttackRegionOrder(o); err != nil {
				fmt.Printf("⚠️ AttackRegion error: %v\n", err)
			} else {
				fmt.Printf("⚔️ %s\n", msg)
			}
		}
	}

	// Step 9: Decrement TEMPORARILY_OPEN timers
	fmt.Println("📋 Step 9: Decrement TEMPORARILY_OPEN timers")
	decrementTempOpenTimers()

	// Step 10: Decrement fortification timers
	fmt.Println("📋 Step 10: Decrement fortification timers")
	decrementFortifyTimers()

	// Step 11: Decrement respawn and cooldown counters
	fmt.Println("📋 Step 11: Decrement respawn + cooldown")
	decrementRespawnAndCooldowns()

	// Step 12: Run detection (suppressed if turn <= hidden-until-turn)
	fmt.Println("📋 Step 12: Detection check")
	RunDetection(producer)

	// Step 13: Evaluate win/draw conditions; emit GameOver if done
	fmt.Println("📋 Step 13: Win condition evaluation")
	if winner, cause := evaluateWinConditions(orders); winner != "" {
		fmt.Printf("🎮 GAME OVER: winner=%s cause=%s\n", winner, cause)
		if producer != nil {
			PublishGameOverTransactionally(producer, "game-session-1", winner, cause)
		}
		PublishWorldStateSnapshot(producer)
		AdvanceTurn()
		return
	}

	// Draw check (40 turns)
	if turn >= 40 {
		fmt.Println("🤝 DRAW: 40 turns reached with no winner")
		if producer != nil {
			PublishGameOverTransactionally(producer, "game-session-1", "DRAW", "max_turns_reached")
		}
	}

	// Emit WorldStateSnapshot and advance to next turn
	PublishWorldStateSnapshot(producer)

	// Publish session state to game.session (log-compacted — for restart recovery)
	publishSessionState(producer, turn)

	newTurn := AdvanceTurn()
	fmt.Printf("✅ ====== Turn %d complete. Now on turn %d ======\n\n", turn, newTurn)
}

// autoAdvanceAllUnits moves every unit one step along its assigned route (Step 7).
// FIX B10: Previously unimplemented.
func autoAdvanceAllUnits(producer *kafka.Producer) {
	worldStateMu.RLock()
	// Snapshot routes to avoid holding lock during movement
	routes := make(map[string]UnitRoute, len(WorldState.UnitRoutes))
	for k, v := range WorldState.UnitRoutes {
		pathsCopy := make([]string, len(v.PathIDs))
		copy(pathsCopy, v.PathIDs)
		routes[k] = UnitRoute{PathIDs: pathsCopy, RouteIdx: v.RouteIdx}
	}
	worldStateMu.RUnlock()

	turn := GetCurrentTurn()

	for unitID, route := range routes {
		if route.RouteIdx >= len(route.PathIDs) {
			continue // Route already complete
		}

		// Check unit is still active
		worldStateMu.RLock()
		unit, uOk := WorldState.Units[unitID]
		worldStateMu.RUnlock()
		if !uOk || unit.Status != "ACTIVE" {
			continue
		}

		pathID := route.PathIDs[route.RouteIdx]
		pathStatus := getPathStatus(pathID)

		if pathStatus == "BLOCKED" {
			fmt.Printf("🚫 %s route blocked at %s — staying put\n", unitID, pathID)
			continue
		}

		path, ok := getPathByID(pathID)
		if !ok {
			continue
		}

		// Determine direction
		currentRegion, ok := getUnitCurrentRegion(unitID)
		if !ok {
			continue
		}
		var targetRegion string
		if currentRegion == path.From {
			targetRegion = path.To
		} else if currentRegion == path.To {
			targetRegion = path.From
		} else {
			continue // unit not at path endpoint — route invalid
		}

		// Move unit
		NodeOccupants[targetRegion] = unitID
		NodeOccupants[currentRegion] = ""
		UpdateUnitRegion(unitID, targetRegion)
		RevertPathBlocksForMovingUnit(unitID, targetRegion)
		AdvanceUnitRouteIdx(unitID)

		fmt.Printf("⬆️  Auto-advance: %s  %s → %s\n", unitID, currentRegion, targetRegion)

		// Ring Bearer specific events
		if unitID == RingBearerID {
			// Check surveillance on crossed path (exposed if surveillanceLevel >= 1 and turn > 3)
			worldStateMu.RLock()
			p, pOk := WorldState.Paths[pathID]
			worldStateMu.RUnlock()
			if pOk && p.SurveillanceLevel >= 1 && turn > HiddenUntilTurn {
				fmt.Printf("👁️ Ring Bearer crossed surveilled path %s — exposed this turn!\n", pathID)
				// Emit RingBearerSpotted to dark side (detection topic)
				if producer != nil {
					emitRingBearerSpotted(producer, pathID)
				}
			}

			// Emit RingBearerMoved to light side only
			if producer != nil {
				emitRingBearerMoved(producer, targetRegion)
			}
		}

		// Check route complete
		worldStateMu.RLock()
		updatedRoute, hasRoute := WorldState.UnitRoutes[unitID]
		worldStateMu.RUnlock()
		if hasRoute && updatedRoute.RouteIdx >= len(updatedRoute.PathIDs) {
			fmt.Printf("✅ Route complete: %s arrived at %s\n", unitID, targetRegion)
		}
	}
}

// decrementTempOpenTimers handles Step 9.
// When timer=0 and blocker still present → BLOCKED; timer=0 and no blocker → OPEN.
func decrementTempOpenTimers() {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	for pathID, timer := range WorldState.TempOpenTimers {
		timer--
		if timer <= 0 {
			delete(WorldState.TempOpenTimers, pathID)
			// Check if blocker is still at endpoint
			pathBlockersMu.Lock()
			_, hasBlocker := pathBlockers[pathID]
			pathBlockersMu.Unlock()
			if hasBlocker {
				WorldState.Paths[pathID] = PathState{
					ID:     pathID,
					Status: "BLOCKED",
				}
				setPathStatus(pathID, "BLOCKED")
				fmt.Printf("🔒 %s reverted from TEMPORARILY_OPEN → BLOCKED (blocker present)\n", pathID)
			} else {
				if p, ok := WorldState.Paths[pathID]; ok {
					p.Status = "OPEN"
					WorldState.Paths[pathID] = p
				}
				setPathStatus(pathID, "OPEN")
				fmt.Printf("🔓 %s reverted from TEMPORARILY_OPEN → OPEN (no blocker)\n", pathID)
			}
		} else {
			WorldState.TempOpenTimers[pathID] = timer
		}
	}
}

// decrementFortifyTimers handles Step 10.
// When timer=0 → fortification expires.
func decrementFortifyTimers() {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	for regionID, timer := range WorldState.FortifyTimers {
		timer--
		if timer <= 0 {
			delete(WorldState.FortifyTimers, regionID)
			if region, ok := WorldState.Regions[regionID]; ok {
				region.Fortified = false
				WorldState.Regions[regionID] = region
				fmt.Printf("🏳️ Fortification expired: %s\n", regionID)
			}
		} else {
			WorldState.FortifyTimers[regionID] = timer
		}
	}
}

// decrementRespawnAndCooldowns handles Step 11.
func decrementRespawnAndCooldowns() {
	worldStateMu.Lock()

	// Respawn timers
	for unitID, timer := range WorldState.RespawnTimers {
		timer--
		if timer <= 0 {
			delete(WorldState.RespawnTimers, unitID)
			// Return unit to its home region at full strength
			cfg, ok := WorldState.UnitConfigs[unitID]
			if ok {
				unit := WorldState.Units[unitID]
				unit.Strength = cfg.Strength
				unit.Status = "ACTIVE"
				unit.Region = cfg.StartRegion
				WorldState.Units[unitID] = unit
				NodeOccupants[cfg.StartRegion] = unitID
				fmt.Printf("🌟 %s respawned at %s with full strength=%d\n", unitID, cfg.StartRegion, cfg.Strength)
			}
		} else {
			WorldState.RespawnTimers[unitID] = timer
		}
	}
	worldStateMu.Unlock()

	// Cooldown counters (stored separately in UnitCooldown map)
	cooldownMu.Lock()
	for unitID, cd := range UnitCooldown {
		if cd > 0 {
			UnitCooldown[unitID] = cd - 1
		}
	}
	cooldownMu.Unlock()
}

// evaluateWinConditions checks spec §1.2 win conditions.
// FIX B10: Previously not called at turn end.
func evaluateWinConditions(orders []OrderPayload) (winner string, cause string) {
	pendingOrdersMu.Lock()
	destroyOrdered := destroyRingThisTurn
	pendingOrdersMu.Unlock()

	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	rbUnit, rbOk := WorldState.Units[RingBearerID]
	if !rbOk {
		return "", ""
	}

	rbRegion := WorldState.LightView.RingBearerRegion

	// ── Light Side win: Ring Bearer at mount-doom + DestroyRing submitted + no Dark Side there
	if rbRegion == MountDoomID && destroyOrdered {
		// Check no Shadow unit at mount-doom
		darkAtDoom := false
		for _, u := range WorldState.Units {
			if u.Side == "SHADOW" && u.Region == MountDoomID && u.Status == "ACTIVE" {
				darkAtDoom = true
				break
			}
		}
		if !darkAtDoom {
			return "FREE_PEOPLES", "ring_destroyed"
		}
	}

	// ── Dark Side win: Any Nazgul occupies same region as Ring Bearer AND exposed=true
	// exposed is evaluated in RunDetection; here we just check co-location
	for _, u := range WorldState.Units {
		if u.Side == "SHADOW" && u.Status == "ACTIVE" && u.Region == rbRegion && rbRegion != "" {
			cfg, ok := WorldState.UnitConfigs[u.ID]
			if ok && cfg.DetectionRange > 0 { // Nazgul-class unit
				// Check if RingBearer was detected this turn (exposure)
				if WorldState.DarkView.LastDetectedTurn == GetCurrentTurn() {
					return "SHADOW", "ring_bearer_captured"
				}
			}
		}
	}

	_ = rbUnit
	return "", ""
}

// publishSessionState writes to game.session (log-compacted) for restart recovery.
// FIX: Answers Q&A question 8 — on restart, consume game.session to get latest turn.
//
// ROOT CAUSE OF BUG: game.session is log-compacted (cleanup.policy=compact).
// Kafka brokers REJECT null-key messages on compact topics at the broker level —
// "Broker: Broker failed to validate record" — because compaction requires a key
// to identify which record is the canonical latest for each entity.
// FIX: Use ProduceMessageWithKey with key="game-state" (constant, single record per game).
func publishSessionState(producer *kafka.Producer, turn int) {
	if producer == nil {
		return
	}

	worldStateMu.RLock()
	snapshot := copyWorldState(WorldState)
	worldStateMu.RUnlock()

	session := map[string]interface{}{
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
		// Include full world state so a restarting instance can rebuild from this one record
		"units":   snapshot.Units,
		"regions": snapshot.Regions,
		"paths":   snapshot.Paths,
	}
	payload, err := json.Marshal(session)
	if err != nil {
		fmt.Printf("⚠️ game.session marshal error: %v\n", err)
		return
	}

	// Key = "game-state": log compaction keeps only the latest record for this key,
	// which is always the most recent world state. A restarting instance reads this
	// single compacted record to recover the current turn and unit/region/path states.
	if err := ProduceMessageWithKey(producer, TopicGameSession, "game-state", payload); err != nil {
		fmt.Printf("⚠️ Could not write to game.session: %v\n", err)
		return
	}
	fmt.Printf("💾 game.session updated (key=game-state, turn=%d)\n", turn)
}

// emitRingBearerMoved sends to game.ring.position (Light Side only).
func emitRingBearerMoved(producer *kafka.Producer, region string) {
	event := map[string]interface{}{
		"eventType": "RingBearerMoved",
		"trueRegion": region,
		"turn":      GetCurrentTurn(),
		"timestamp": time.Now().UnixMilli(),
	}
	payload, _ := json.Marshal(event)
	if err := ProduceMessage(producer, TopicRingPosition, payload); err != nil {
		fmt.Printf("⚠️ RingBearerMoved emit failed: %v\n", err)
	}
}

// emitRingBearerSpotted sends to game.ring.detection (Dark Side only).
func emitRingBearerSpotted(producer *kafka.Producer, pathID string) {
	event := map[string]interface{}{
		"eventType": "RingBearerSpotted",
		"pathId":    pathID,
		"turn":      GetCurrentTurn(),
		"timestamp": time.Now().UnixMilli(),
	}
	payload, _ := json.Marshal(event)
	if err := ProduceMessage(producer, TopicRingDetection, payload); err != nil {
		fmt.Printf("⚠️ RingBearerSpotted emit failed: %v\n", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LEGACY SINGLE-STEP MOVE (kept for backward compat with existing test paths)
// ─────────────────────────────────────────────────────────────────────────────

func ProcessTurn(unitID string, sourceNode string, targetNode string) error {
	fmt.Printf("\n🛠️ ProcessTurn: %s %s → %s\n", unitID, sourceNode, targetNode)

	if _, ok := getUnitConfig(unitID); !ok {
		return fmt.Errorf("unit not in config: %s", unitID)
	}
	if !areAdjacent(sourceNode, targetNode) {
		return fmt.Errorf("no path between %s and %s", sourceNode, targetNode)
	}

	occupant := NodeOccupants[targetNode]
	if occupant != "" && occupant != unitID {
		attackerCfg, aOk := getUnitConfig(unitID)
		defenderCfg, dOk := getUnitConfig(occupant)

		if aOk && dOk && attackerCfg.Side == defenderCfg.Side {
			// Friendly — move without combat
			NodeOccupants[targetNode] = unitID
			NodeOccupants[sourceNode] = ""
			UpdateUnitRegion(unitID, targetNode)
			RevertPathBlocksForMovingUnit(unitID, targetNode)
			return nil
		}

		result := ResolveCombat(unitID, occupant, targetNode)
		if result.WinnerID == unitID {
			NodeOccupants[targetNode] = unitID
			NodeOccupants[sourceNode] = ""
			UpdateUnitRegion(unitID, targetNode)
			RevertPathBlocksForMovingUnit(unitID, targetNode)
			if occupant == RingBearerID {
				return errors.New("GAME_OVER: SHADOW wins")
			}
		} else if result.WinnerID == occupant {
			NodeOccupants[sourceNode] = ""
			if unitID == RingBearerID {
				return errors.New("GAME_OVER: SHADOW wins")
			}
			return errors.New("unit defeated")
		}
		return nil
	}

	NodeOccupants[targetNode] = unitID
	NodeOccupants[sourceNode] = ""
	UpdateUnitRegion(unitID, targetNode)
	RevertPathBlocksForMovingUnit(unitID, targetNode)

	if unitID == RingBearerID && targetNode == MountDoomID {
		fmt.Println("🌋 Ring Bearer reached Mount Doom!")
	}
	return nil
}

// ResolveMoveFromPath resolves the next region to move to given a pathId.
func ResolveMoveFromPath(unitID string, pathID string) (source string, target string, err error) {
	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "", "", fmt.Errorf("unit not found: %s", unitID)
	}
	path, ok := getPathByID(pathID)
	if !ok {
		return "", "", fmt.Errorf("path not found: %s", pathID)
	}
	switch currentRegion {
	case path.From:
		return path.From, path.To, nil
	case path.To:
		return path.To, path.From, nil
	default:
		return "", "", fmt.Errorf("%s not at endpoint of %s", unitID, pathID)
	}
}

// getUnitConfig looks up unit config from the WorldState map (O(1)).
func getUnitConfig(unitID string) (UnitConfig, bool) {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	cfg, ok := WorldState.UnitConfigs[unitID]
	if ok {
		return cfg, true
	}
	// Fallback: linear search through LoadedUnits (covers test setup before InitWorldState)
	for _, u := range LoadedUnits {
		if u.ID == unitID || u.Name == unitID {
			return u, true
		}
	}
	return UnitConfig{}, false
}

func getUnitCurrentRegion(unitID string) (string, bool) {
	// Check WorldState first
	worldStateMu.RLock()
	unit, ok := WorldState.Units[unitID]
	worldStateMu.RUnlock()
	if ok && unit.Region != "" {
		return unit.Region, true
	}

	// Fall back to NodeOccupants
	for regionID, occupantID := range NodeOccupants {
		if occupantID == unitID {
			return regionID, true
		}
	}

	// Fall back to config start region
	cfg, cfgOk := getUnitConfig(unitID)
	if cfgOk {
		return cfg.StartRegion, true
	}
	return "", false
}

func getPathByID(pathID string) (PathConfig, bool) {
	for _, path := range LoadedMap.Paths {
		if path.ID == pathID {
			return path, true
		}
	}
	return PathConfig{}, false
}

func areAdjacent(src string, tgt string) bool {
	for _, path := range LoadedMap.Paths {
		if (path.From == src && path.To == tgt) || (path.From == tgt && path.To == src) {
			return true
		}
	}
	return false
}
