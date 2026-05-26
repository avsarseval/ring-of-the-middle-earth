package internal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	TopicOrdersRaw       = "game.orders.raw"
	TopicOrdersValidated = "game.orders.validated"
	TopicBroadcast       = "game.broadcast"
	TopicRingPosition    = "game.ring.position"
	TopicRingDetection   = "game.ring.detection"
	TopicDLQ             = "game.dlq"
	TopicGameSession     = "game.session" // FIX K1: was missing from consumer subscription

	OrderAssignRoute   = "ASSIGN_ROUTE"
	OrderRedirectUnit  = "REDIRECT_UNIT"
	OrderBlockPath     = "BLOCK_PATH"
	OrderSearchPath    = "SEARCH_PATH"
	OrderAttackRegion  = "ATTACK_REGION"
	OrderMaiaAbility   = "MAIA_ABILITY"
	OrderFortifyRegion = "FORTIFY_REGION"
	// FIX K4: missing order types now declared
	OrderDestroyRingConst = "DESTROY_RING"
)

var (
	currentTurnMu sync.RWMutex
	CurrentTurn   = 1
)

func GetCurrentTurn() int {
	currentTurnMu.RLock()
	defer currentTurnMu.RUnlock()
	return CurrentTurn
}

func AdvanceTurn() int {
	currentTurnMu.Lock()
	CurrentTurn++
	newTurn := CurrentTurn
	currentTurnMu.Unlock()

	// Clear duplicate-order tracker for completed turn
	orderTrackerMu.Lock()
	for turn := range ordersByTurn {
		if turn < newTurn {
			delete(ordersByTurn, turn)
		}
	}
	orderTrackerMu.Unlock()

	// Sync WorldState turn number
	worldStateMu.Lock()
	WorldState.Turn = newTurn
	WorldState.UpdatedAt = time.Now().UnixMilli()
	worldStateMu.Unlock()

	fmt.Printf("⏭️ Advanced to turn %d\n", newTurn)
	return newTurn
}

var (
	orderTrackerMu sync.Mutex
	ordersByTurn   = make(map[int]map[string]bool)
)

var (
	pathStateMu sync.Mutex
	PathStatus  = make(map[string]string) // pathId → status
)

var (
	cooldownMu   sync.Mutex
	UnitCooldown = make(map[string]int) // unitId → cooldown remaining
)

// Event is the internal message carrier (Kafka → coordinator → SSE clients).
type Event struct {
	Topic   string
	Payload []byte
}

// OrderPayload is the canonical order structure for all order types.
type OrderPayload struct {
	OrderType string `json:"orderType"`
	PlayerID  string `json:"playerId"`
	UnitID    string `json:"unitId"`
	Turn      int    `json:"turn"`

	PathIDs    []string `json:"pathIds"`
	NewPathIDs []string `json:"newPathIds"`

	PathID       string `json:"pathId"`
	TargetPathID string `json:"targetPathId"`
	TargetRegion string `json:"targetRegion"`
	RegionID     string `json:"regionId"`

	RouteRiskScore  *int     `json:"routeRiskScore,omitempty"`
	ThreatenedPaths []string `json:"threatenedPaths,omitempty"`
	BlockedPaths    []string `json:"blockedPaths,omitempty"`

	// Legacy proto fields
	LegacyUnitID string `json:"unit_id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
}

type WorldStateSnapshot struct {
	Turn      int               `json:"turn"`
	Regions   map[string]string `json:"regions"`
	Units     map[string]string `json:"units"`
	Timestamp int64             `json:"timestamp"`
}

type DLQEntry struct {
	OriginalTopic string `json:"originalTopic"`
	ErrorCode     string `json:"errorCode"`
	ErrorMessage  string `json:"errorMessage"`
	RawPayload    string `json:"rawPayload"`
	Timestamp     int64  `json:"timestamp"`
}

// EventRouter routes Kafka events to the right destinations.
// FIX B9: This is called from the coordinator goroutine's case 1.
// It no longer uses shared SSE channels — it returns routing info for the coordinator.
func EventRouter(
	eventCh <-chan Event,
	lightSideSSECh chan<- Event,
	darkSideSSECh chan<- Event,
	cacheUpdateCh chan<- Event,
	engineCh chan<- Event,
	producer *kafka.Producer,
) {
	fmt.Println("🔄 EventRouter goroutine started")

	for event := range eventCh {
		switch event.Topic {

		case TopicOrdersRaw:
			validatedEvent, ok := validateRawOrder(event, producer)
			if ok {
				enrichedEvent := enrichRouteRisk(validatedEvent)
				if err := ProduceMessage(producer, TopicOrdersValidated, enrichedEvent.Payload); err != nil {
					fmt.Printf("❌ Could not write to validated topic: %v\n", err)
					continue
				}
				fmt.Println("✅ Order validated and enriched → game.orders.validated")
			}

		case TopicOrdersValidated:
			// FIX B10: Buffer for 13-step turn processor instead of immediate apply
			AddPendingOrder(event)

		case TopicRingPosition:
			// FIX B7: Ring Bearer position → Light Side ONLY
			lightSideSSECh <- event

		case TopicRingDetection:
			// FIX B7: Detection events → Dark Side ONLY
			darkSideSSECh <- event

		case TopicBroadcast:
			lightSideSSECh <- event
			darkSideSSECh <- StripRingBearer(event) // FIX B7: strip position for dark side

		case "game.events.unit", "game.events.region", "game.events.path":
			lightSideSSECh <- event
			darkSideSSECh <- event

		case TopicDLQ:
			fmt.Printf("🧯 DLQ event: %s\n", string(event.Payload))

		default:
			cacheUpdateCh <- event
		}
	}
}

// ValidateRawOrder is the exported wrapper for the coordinator to call directly.
func ValidateRawOrder(event Event, producer *kafka.Producer) (Event, bool) {
	return validateRawOrder(event, producer)
}

// EnrichRouteRisk is the exported wrapper.
func EnrichRouteRisk(event Event) Event {
	return enrichRouteRisk(event)
}

func validateRawOrder(event Event, producer *kafka.Producer) (Event, bool) {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		return rejectOrder(producer, event, "INVALID_JSON", "order is not valid JSON")
	}

	fmt.Println("📩 Validating raw order...")

	unitID := normalizedUnitID(order)
	isLegacyMove := order.Source != "" && order.Target != ""

	validators := []func(OrderPayload, string, bool) (string, string){
		validateRequiredFields,
		validateWrongTurn,
		validateUnitOwnershipRule,
		validatePathRule,
		validatePathBlockedRule,
		validateUnitAdjacentRule,
		validateAttackTargetRule,
		validateCooldownRule,
		// FIX K4: MAIA_DISABLED added
		validateMaiaDisabledRule,
		// FIX K4: DESTROY_CONDITION_NOT_MET added
		validateDestroyConditionRule,
		validateDuplicateRule,
	}

	for _, validator := range validators {
		if code, message := validator(order, unitID, isLegacyMove); code != "" {
			return rejectOrder(producer, event, code, message)
		}
	}

	markUnitOrdered(order.Turn, unitID)
	fmt.Println("✅ Order validated")

	return Event{
		Topic:   TopicOrdersValidated,
		Payload: event.Payload,
	}, true
}

func rejectOrder(producer *kafka.Producer, event Event, errorCode string, message string) (Event, bool) {
	fmt.Printf("❌ Order rejected: %s — %s\n", errorCode, message)
	writeDLQ(producer, event, errorCode, message)
	return Event{}, false
}

func normalizedUnitID(order OrderPayload) string {
	if order.UnitID != "" {
		return order.UnitID
	}
	return order.LegacyUnitID
}

func validateRequiredFields(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if unitID == "" {
		return "NOT_YOUR_UNIT", "unitId is empty"
	}
	if order.OrderType == "" && !isLegacyMove {
		return "INVALID_ORDER_TYPE", "orderType is empty"
	}
	return "", ""
}

func validateWrongTurn(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove {
		return "", ""
	}
	if order.Turn != GetCurrentTurn() {
		return "WRONG_TURN", fmt.Sprintf("expected turn %d, got %d", GetCurrentTurn(), order.Turn)
	}
	return "", ""
}

func validateUnitOwnershipRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove && order.PlayerID == "" {
		return "", ""
	}
	ok, reason := validateUnitOwnership(order.PlayerID, unitID)
	if !ok {
		return "NOT_YOUR_UNIT", reason
	}
	return "", ""
}

func validatePathRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove {
		if !edgeExists(order.Source, order.Target) {
			return "INVALID_PATH", fmt.Sprintf("no path between %s and %s", order.Source, order.Target)
		}
		return "", ""
	}
	switch order.OrderType {
	case OrderAssignRoute:
		if len(order.PathIDs) == 0 {
			return "INVALID_PATH", "ASSIGN_ROUTE: pathIds is empty"
		}
		for _, pid := range order.PathIDs {
			if !pathExists(pid) {
				return "INVALID_PATH", fmt.Sprintf("path not found: %s", pid)
			}
		}
	case OrderRedirectUnit:
		if len(order.NewPathIDs) == 0 {
			return "INVALID_PATH", "REDIRECT_UNIT: newPathIds is empty"
		}
		for _, pid := range order.NewPathIDs {
			if !pathExists(pid) {
				return "INVALID_PATH", fmt.Sprintf("path not found: %s", pid)
			}
		}
	case OrderBlockPath, OrderSearchPath, OrderMaiaAbility:
		pid := singlePathID(order)
		if pid == "" {
			return "INVALID_PATH", fmt.Sprintf("%s: pathId is empty", order.OrderType)
		}
		if !pathExists(pid) {
			return "INVALID_PATH", fmt.Sprintf("path not found: %s", pid)
		}
	}
	return "", ""
}

func validatePathBlockedRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove {
		return "", ""
	}
	var pathIDs []string
	switch order.OrderType {
	case OrderAssignRoute:
		pathIDs = order.PathIDs
	case OrderRedirectUnit:
		pathIDs = order.NewPathIDs
	default:
		return "", ""
	}
	if len(pathIDs) > 0 && getPathStatus(pathIDs[0]) == "BLOCKED" {
		return "PATH_BLOCKED", fmt.Sprintf("first path is BLOCKED: %s", pathIDs[0])
	}
	return "", ""
}

func validateUnitAdjacentRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove {
		return "", ""
	}
	switch order.OrderType {
	case OrderBlockPath, OrderSearchPath, OrderMaiaAbility:
		pathID := singlePathID(order)
		if pathID == "" {
			return "", ""
		}
		path, ok := getPathByID(pathID)
		if !ok {
			return "INVALID_PATH", fmt.Sprintf("path not found: %s", pathID)
		}
		currentRegion, ok := getUnitCurrentRegion(unitID)
		if !ok {
			return "NOT_YOUR_UNIT", fmt.Sprintf("unit region unknown: %s", unitID)
		}
		if currentRegion != path.From && currentRegion != path.To {
			return "UNIT_NOT_ADJACENT", fmt.Sprintf(
				"%s is at %s; not at endpoint of %s (%s, %s)",
				unitID, currentRegion, pathID, path.From, path.To,
			)
		}
	}
	return "", ""
}

func validateAttackTargetRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove || order.OrderType != OrderAttackRegion {
		return "", ""
	}
	targetRegion := order.TargetRegion
	if targetRegion == "" {
		targetRegion = order.RegionID
	}
	if targetRegion == "" {
		return "INVALID_TARGET", "ATTACK_REGION: targetRegion is empty"
	}
	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "NOT_YOUR_UNIT", fmt.Sprintf("unit region unknown: %s", unitID)
	}
	if !areAdjacent(currentRegion, targetRegion) {
		return "INVALID_TARGET", fmt.Sprintf("%s → %s is not adjacent", currentRegion, targetRegion)
	}
	occupant := NodeOccupants[targetRegion]
	if occupant == "" {
		return "INVALID_TARGET", fmt.Sprintf("no enemy unit at %s", targetRegion)
	}
	attackerCfg, aOk := getUnitConfig(unitID)
	defenderCfg, dOk := getUnitConfig(occupant)
	if aOk && dOk && attackerCfg.Side == defenderCfg.Side {
		return "INVALID_TARGET", fmt.Sprintf("target unit %s is an ally", occupant)
	}
	return "", ""
}

func validateCooldownRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove || order.OrderType != OrderMaiaAbility {
		return "", ""
	}
	if getCooldown(unitID) > 0 {
		return "ABILITY_ON_COOLDOWN", fmt.Sprintf("%s cooldown=%d", unitID, getCooldown(unitID))
	}
	unitConfig, ok := getUnitConfig(unitID)
	if !ok {
		return "NOT_YOUR_UNIT", fmt.Sprintf("unit config not found: %s", unitID)
	}
	if !unitConfig.Maia {
		return "INVALID_TARGET", fmt.Sprintf("%s is not Maia", unitID)
	}
	return "", ""
}

// FIX K4: MAIA_DISABLED — Saruman cannot act after Isengard falls to Free Peoples.
// Config-driven: detects CORRUPT_PATH ability by AbilityEffect field, not unit name.
func validateMaiaDisabledRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove || order.OrderType != OrderMaiaAbility {
		return "", ""
	}
	cfg, ok := getUnitConfig(unitID)
	if !ok {
		return "", ""
	}
	// Only the CORRUPT_PATH Maia can be disabled (Saruman)
	if cfg.AbilityEffect != "CORRUPT_PATH" {
		return "", ""
	}
	worldStateMu.RLock()
	isengard, isengardOk := WorldState.Regions["isengard"]
	worldStateMu.RUnlock()
	if isengardOk && isengard.ControlledBy != "SHADOW" {
		return "MAIA_DISABLED", fmt.Sprintf("%s is permanently disabled (Isengard has fallen)", unitID)
	}
	return "", ""
}

// FIX K4: DESTROY_CONDITION_NOT_MET — Ring Bearer must be at mount-doom, no Dark Side unit present.
func validateDestroyConditionRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove || order.OrderType != OrderDestroyRingConst {
		return "", ""
	}
	rbRegion, ok := getUnitCurrentRegion(unitID)
	if !ok || rbRegion != MountDoomID {
		return "DESTROY_CONDITION_NOT_MET", "Ring Bearer is not at Mount Doom"
	}
	worldStateMu.RLock()
	for _, u := range WorldState.Units {
		if u.Side == "SHADOW" && u.Region == MountDoomID && u.Status == "ACTIVE" {
			worldStateMu.RUnlock()
			return "DESTROY_CONDITION_NOT_MET", "a Dark Side unit is present at Mount Doom"
		}
	}
	worldStateMu.RUnlock()
	return "", ""
}

func validateDuplicateRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	turn := order.Turn
	if isLegacyMove {
		turn = GetCurrentTurn()
	}
	if isDuplicateUnitOrder(turn, unitID) {
		return "DUPLICATE_UNIT_ORDER", fmt.Sprintf("second order for unit %s on turn %d", unitID, turn)
	}
	return "", ""
}

func singlePathID(order OrderPayload) string {
	if order.PathID != "" {
		return order.PathID
	}
	if order.TargetPathID != "" {
		return order.TargetPathID
	}
	if len(order.PathIDs) > 0 {
		return order.PathIDs[0]
	}
	if len(order.NewPathIDs) > 0 {
		return order.NewPathIDs[0]
	}
	return ""
}

func getPathStatus(pathID string) string {
	pathStateMu.Lock()
	defer pathStateMu.Unlock()
	if status := PathStatus[pathID]; status != "" {
		return status
	}
	return "OPEN"
}

func setPathStatus(pathID string, status string) {
	pathStateMu.Lock()
	defer pathStateMu.Unlock()
	PathStatus[pathID] = status
}

// SetPathStatusForTest is exported for use in test files.
func SetPathStatusForTest(pathID string, status string) {
	setPathStatus(pathID, status)
}

func getCooldown(unitID string) int {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	return UnitCooldown[unitID]
}

func setCooldown(unitID string, cooldown int) {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	UnitCooldown[unitID] = cooldown
}

func getUnitSide(unitID string) (string, bool) {
	unit, ok := getUnitConfig(unitID)
	if !ok {
		return "", false
	}
	return unit.Side, true
}

func expectedSideForPlayer(playerID string) (string, bool) {
	switch playerID {
	case "light", "free", "FREE_PEOPLES":
		return "FREE_PEOPLES", true
	case "dark", "shadow", "SHADOW":
		return "SHADOW", true
	default:
		return "", false
	}
}

func validateUnitOwnership(playerID string, unitID string) (bool, string) {
	expectedSide, ok := expectedSideForPlayer(playerID)
	if !ok {
		return false, fmt.Sprintf("invalid playerId: %s", playerID)
	}
	unitSide, found := getUnitSide(unitID)
	if !found {
		return false, fmt.Sprintf("unit not found: %s", unitID)
	}
	if unitSide != expectedSide {
		return false, fmt.Sprintf("unit %s (side=%s) does not belong to player %s (expected=%s)",
			unitID, unitSide, playerID, expectedSide)
	}
	return true, ""
}

func pathExists(pathID string) bool {
	_, ok := getPathByID(pathID)
	return ok
}

func edgeExists(source string, target string) bool {
	for _, path := range LoadedMap.Paths {
		if (path.From == source && path.To == target) || (path.From == target && path.To == source) {
			return true
		}
	}
	return false
}

func isDuplicateUnitOrder(turn int, unitID string) bool {
	orderTrackerMu.Lock()
	defer orderTrackerMu.Unlock()
	if ordersByTurn[turn] == nil {
		return false
	}
	return ordersByTurn[turn][unitID]
}

func markUnitOrdered(turn int, unitID string) {
	orderTrackerMu.Lock()
	defer orderTrackerMu.Unlock()
	if ordersByTurn[turn] == nil {
		ordersByTurn[turn] = make(map[string]bool)
	}
	ordersByTurn[turn][unitID] = true
}

func writeDLQ(producer *kafka.Producer, event Event, errorCode string, errorMessage string) {
	entry := DLQEntry{
		OriginalTopic: event.Topic,
		ErrorCode:     errorCode,
		ErrorMessage:  errorMessage,
		RawPayload:    string(event.Payload),
		Timestamp:     time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		fmt.Printf("❌ DLQ marshal error: %v\n", err)
		return
	}
	if err := ProduceMessage(producer, TopicDLQ, payload); err != nil {
		fmt.Printf("❌ DLQ write error: %v\n", err)
		return
	}
	fmt.Printf("🧯 DLQ: errorCode=%s\n", errorCode)
}

// processValidatedOrder is called when a validated order arrives via Kafka.
// FIX B10: Instead of applying immediately, now buffers for turn-end processing.
func processValidatedOrder(event Event, lightSideSSECh chan<- Event, darkSideSSECh chan<- Event) {
	// Buffer for 13-step turn processor
	AddPendingOrder(event)

	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		return
	}
	fmt.Printf("📦 Buffered validated order: %s unit=%s\n", order.OrderType, order.UnitID)
}

// StripRingBearer removes the Ring Bearer's true position from a broadcast event.
// FIX B7: Exported so the coordinator can call it when broadcasting to dark side clients.
func StripRingBearer(event Event) Event {
	return stripRingBearer(event)
}

func stripRingBearer(event Event) Event {
	var payload map[string]interface{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return event
	}

	// New format: { eventType, turn, state: { units, lightView, darkView } }
	if stateRaw, ok := payload["state"].(map[string]interface{}); ok {
		stripRingBearerFromStateMap(stateRaw)
		newPayload, err := json.Marshal(payload)
		if err == nil {
			return Event{Topic: event.Topic, Payload: newPayload}
		}
		return event
	}

	// Legacy format: { turn, units, regions, ... }
	stripRingBearerFromStateMap(payload)
	newPayload, err := json.Marshal(payload)
	if err != nil {
		return event
	}
	return Event{Topic: event.Topic, Payload: newPayload}
}

func stripRingBearerFromStateMap(state map[string]interface{}) {
	if unitsRaw, ok := state["units"].(map[string]interface{}); ok {
		if rbRaw, ok := unitsRaw[RingBearerID].(map[string]interface{}); ok {
			rbRaw["region"] = ""
			unitsRaw[RingBearerID] = rbRaw
		}
	}
	if lightViewRaw, ok := state["lightView"].(map[string]interface{}); ok {
		lightViewRaw["ringBearerRegion"] = ""
		state["lightView"] = lightViewRaw
	}
	if darkViewRaw, ok := state["darkView"].(map[string]interface{}); ok {
		darkViewRaw["ringBearerRegion"] = ""
		state["darkView"] = darkViewRaw
	}
}

func enrichRouteRisk(event Event) Event {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		return event
	}
	if order.OrderType != OrderAssignRoute && order.OrderType != OrderRedirectUnit {
		return event
	}
	pathIDs := order.PathIDs
	if order.OrderType == OrderRedirectUnit {
		pathIDs = order.NewPathIDs
	}
	if len(pathIDs) == 0 {
		return event
	}

	score, threatened, blocked := CalculateRouteRisk(normalizedUnitID(order), pathIDs)
	order.RouteRiskScore = &score
	order.ThreatenedPaths = threatened
	order.BlockedPaths = blocked

	payload, err := json.Marshal(order)
	if err != nil {
		return event
	}
	fmt.Printf("📊 RouteRisk enriched: unit=%s score=%d threatened=%v blocked=%v\n",
		normalizedUnitID(order), score, threatened, blocked)
	return Event{Topic: TopicOrdersValidated, Payload: payload}
}

func CalculateRouteRisk(unitID string, pathIDs []string) (int, []string, []string) {
	score := 0
	threatened := []string{}
	blocked := []string{}

	destinationRegions := resolveRouteDestinationRegions(unitID, pathIDs)
	for _, regionID := range destinationRegions {
		region, ok := getRegionState(regionID)
		if ok {
			score += region.ThreatLevel
		}
	}

	for _, pathID := range pathIDs {
		path, ok := getPathState(pathID)
		if !ok {
			continue
		}
		score += path.SurveillanceLevel * 3
		switch path.Status {
		case "THREATENED":
			score += 2
			threatened = append(threatened, pathID)
		case "BLOCKED":
			score += 5
			blocked = append(blocked, pathID)
		}
	}

	nazgulProx := countNazgulWithinTwoHops(destinationRegions)
	score += nazgulProx * 2

	return score, threatened, blocked
}

func resolveRouteDestinationRegions(unitID string, pathIDs []string) []string {
	destinations := []string{}
	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return destinations
	}
	for _, pathID := range pathIDs {
		path, ok := getPathByID(pathID)
		if !ok {
			continue
		}
		if currentRegion == path.From {
			destinations = append(destinations, path.To)
			currentRegion = path.To
		} else if currentRegion == path.To {
			destinations = append(destinations, path.From)
			currentRegion = path.From
		} else {
			destinations = append(destinations, path.To)
			currentRegion = path.To
		}
	}
	return destinations
}

func getRegionState(regionID string) (RegionState, bool) {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	region, ok := WorldState.Regions[regionID]
	return region, ok
}

func getPathState(pathID string) (PathState, bool) {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	path, ok := WorldState.Paths[pathID]
	if !ok {
		return PathState{}, false
	}
	// Override with live PathStatus map (for test-injected statuses)
	if status := PathStatus[pathID]; status != "" {
		path.Status = status
	}
	return path, true
}

func countNazgulWithinTwoHops(routeRegions []string) int {
	count := 0
	worldStateMu.RLock()
	units := make([]UnitSnapshot, 0, len(WorldState.Units))
	for _, u := range WorldState.Units {
		units = append(units, u)
	}
	worldStateMu.RUnlock()

	for _, unit := range units {
		// FIX B1: check config.DetectionRange > 0, not class == "Nazgul"
		cfg, ok := getUnitConfig(unit.ID)
		if !ok || cfg.DetectionRange == 0 || unit.Status != "ACTIVE" {
			continue
		}
		for _, routeRegion := range routeRegions {
			if d := graphDistance(unit.Region, routeRegion); d >= 0 && d <= 2 {
				count++
				break
			}
		}
	}
	return count
}

func graphDistance(start string, target string) int {
	if start == target {
		return 0
	}
	visited := map[string]bool{start: true}
	queue := []struct {
		region string
		dist   int
	}{{start, 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nb := range neighborsOf(cur.region) {
			if visited[nb] {
				continue
			}
			if nb == target {
				return cur.dist + 1
			}
			visited[nb] = true
			queue = append(queue, struct {
				region string
				dist   int
			}{nb, cur.dist + 1})
		}
	}
	return -1
}

func neighborsOf(regionID string) []string {
	var nbs []string
	for _, path := range LoadedMap.Paths {
		if path.From == regionID {
			nbs = append(nbs, path.To)
		} else if path.To == regionID {
			nbs = append(nbs, path.From)
		}
	}
	return nbs
}
