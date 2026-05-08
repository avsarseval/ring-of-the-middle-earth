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

	OrderAssignRoute   = "ASSIGN_ROUTE"
	OrderRedirectUnit  = "REDIRECT_UNIT"
	OrderBlockPath     = "BLOCK_PATH"
	OrderSearchPath    = "SEARCH_PATH"
	OrderAttackRegion  = "ATTACK_REGION"
	OrderMaiaAbility   = "MAIA_ABILITY"
	OrderFortifyRegion = "FORTIFY_REGION"
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

	// Duplicate order tracker yeni turn için temizlenir.
	orderTrackerMu.Lock()
	for turn := range ordersByTurn {
		if turn < newTurn {
			delete(ordersByTurn, turn)
		}
	}
	orderTrackerMu.Unlock()

	// WorldStateCache turn bilgisini de güncelle.
	worldStateMu.Lock()
	WorldState.Turn = newTurn
	WorldState.UpdatedAt = time.Now().UnixMilli()
	worldStateMu.Unlock()

	fmt.Printf("⏭️ Turn ilerledi! Yeni turn: %d\n", newTurn)

	return newTurn
}
// Aynı turn içinde aynı unit için ikinci emir kontrolü.
var (
	orderTrackerMu sync.Mutex
	ordersByTurn  = make(map[int]map[string]bool)
)

// Prototip path status state'i.
// Finalde PathKTable / WorldStateCache.Paths üzerinden okunmalı.
var (
	pathStateMu sync.Mutex
	PathStatus = make(map[string]string) // pathId -> OPEN / THREATENED / BLOCKED / TEMPORARILY_OPEN
)

// Prototip cooldown state'i.
// Finalde UnitSnapshot veya UnitKTable içinde tutulmalı.
var (
	cooldownMu    sync.Mutex
	UnitCooldown = make(map[string]int) // unitId -> cooldown
)

// Event, Kafka'dan gelen veya SSE'ye gidecek temel mesaj yapısı.
type Event struct {
	Topic   string
	Payload []byte
}

// OrderPayload proje dokümanındaki order formatını ve şu anki prototip formatı destekler.
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
	RouteRiskScore *int     `json:"routeRiskScore,omitempty"`
	ThreatenedPaths []string `json:"threatenedPaths,omitempty"`
	BlockedPaths    []string `json:"blockedPaths,omitempty"`

	// Legacy/prototip alanları:
	LegacyUnitID string `json:"unit_id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
}

// Dünya durumunu temsil eden struct.
type WorldStateSnapshot struct {
	Turn      int               `json:"turn"`
	Regions   map[string]string `json:"regions"`
	Units     map[string]string `json:"units"`
	Timestamp int64             `json:"timestamp"`
}

// DLQEntry hatalı emirleri game.dlq topic'ine yazmak için kullanılır.
type DLQEntry struct {
	OriginalTopic string `json:"originalTopic"`
	ErrorCode     string `json:"errorCode"`
	ErrorMessage  string `json:"errorMessage"`
	RawPayload    string `json:"rawPayload"`
	Timestamp     int64  `json:"timestamp"`
}

// EventRouter Kafka'dan gelen mesajları doğrular, oyun motoruna ve tarayıcılara dağıtır.
func EventRouter(
	eventCh <-chan Event,
	lightSideSSECh chan<- Event,
	darkSideSSECh chan<- Event,
	cacheUpdateCh chan<- Event,
	engineCh chan<- Event,
	producer *kafka.Producer,
) {
	fmt.Println("🔄 EventRouter goroutine'i başlatıldı, olaylar bekleniyor...")

	for event := range eventCh {
		switch event.Topic {

		case TopicOrdersRaw:
			validatedEvent, ok := validateRawOrder(event, producer)
			if ok {
				enrichedEvent := enrichRouteRisk(validatedEvent)

				if err := ProduceMessage(producer, TopicOrdersValidated, enrichedEvent.Payload); err != nil {
					fmt.Printf("❌ Validated emir Kafka'ya yazılamadı: %v\n", err)
					continue
				}

				fmt.Println("✅ Validated emir Kafka'ya yazıldı: game.orders.validated")
			}

		case TopicOrdersValidated:
			processValidatedOrder(event, lightSideSSECh, darkSideSSECh)

		case TopicRingPosition:
			// RingBearerMoved sadece Light Side'a gider. Dark Side'a asla gönderme.
			lightSideSSECh <- event

		case TopicRingDetection:
			// Detection/Spotted bilgisi sadece Dark Side'a gider.
			darkSideSSECh <- event

		case TopicBroadcast:
			fmt.Println("📡 game.broadcast event'i SSE kanallarına dağıtılıyor")

			lightSideSSECh <- event
			darkSideSSECh <-  stripRingBearer(event)

		case "game.events.unit", "game.events.region", "game.events.path":
			lightSideSSECh <- event
			darkSideSSECh <- event

		case TopicDLQ:
			fmt.Printf("🧯 DLQ event yakalandı: %s\n", string(event.Payload))

		default:
			cacheUpdateCh <- event
		}
	}
}

// validateRawOrder bütün validation kurallarını merkezi şekilde çalıştırır.
func validateRawOrder(event Event, producer *kafka.Producer) (Event, bool) {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		return rejectOrder(producer, event, "INVALID_JSON", "Emir JSON formatında okunamadı")
	}

	fmt.Println("📩 Kafka'dan yeni raw emir yakalandı, doğrulanıyor...")

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
		validateDuplicateRule,
	}

	for _, validator := range validators {
		if code, message := validator(order, unitID, isLegacyMove); code != "" {
			return rejectOrder(producer, event, code, message)
		}
	}

	markUnitOrdered(order.Turn, unitID)

	fmt.Println("✅ Emir doğrulandı: game.orders.validated aşamasına geçti")

	return Event{
		Topic:   TopicOrdersValidated,
		Payload: event.Payload,
	}, true
}

func rejectOrder(producer *kafka.Producer, event Event, errorCode string, message string) (Event, bool) {
	fmt.Println("❌ Emir geçersiz:", message)
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
		return "NOT_YOUR_UNIT", "unitId alanı boş veya eksik"
	}

	if order.OrderType == "" && !isLegacyMove {
		return "INVALID_ORDER_TYPE", "orderType alanı boş veya eksik"
	}

	return "", ""
}

func validateWrongTurn(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove {
		return "", ""
	}

	if order.Turn != GetCurrentTurn() {
		return "WRONG_TURN", fmt.Sprintf("Yanlış turn: gelen=%d, beklenen=%d", order.Turn, GetCurrentTurn())
	}

	return "", ""
}

func validateUnitOwnershipRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	// Legacy formatta playerId olmayabilir. Proje formatında playerId zorunlu gibi düşünülür.
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
			return "INVALID_PATH", fmt.Sprintf("Geçersiz legacy hareket: %s -> %s", order.Source, order.Target)
		}
		return "", ""
	}

	switch order.OrderType {
	case OrderAssignRoute:
		if len(order.PathIDs) == 0 {
			return "INVALID_PATH", "ASSIGN_ROUTE için pathIds boş"
		}
		for _, pathID := range order.PathIDs {
			if !pathExists(pathID) {
				return "INVALID_PATH", fmt.Sprintf("Geçersiz pathId: %s", pathID)
			}
		}

	case OrderRedirectUnit:
		if len(order.NewPathIDs) == 0 {
			return "INVALID_PATH", "REDIRECT_UNIT için newPathIds boş"
		}
		for _, pathID := range order.NewPathIDs {
			if !pathExists(pathID) {
				return "INVALID_PATH", fmt.Sprintf("Geçersiz pathId: %s", pathID)
			}
		}

	case OrderBlockPath, OrderSearchPath, OrderMaiaAbility:
		pathID := singlePathID(order)
		if pathID == "" {
			return "INVALID_PATH", fmt.Sprintf("%s için pathId/targetPathId boş", order.OrderType)
		}
		if !pathExists(pathID) {
			return "INVALID_PATH", fmt.Sprintf("Geçersiz pathId: %s", pathID)
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

	if len(pathIDs) == 0 {
		return "", ""
	}

	firstPath := pathIDs[0]
	if getPathStatus(firstPath) == "BLOCKED" {
		return "PATH_BLOCKED", fmt.Sprintf("Ring Bearer route için ilk path BLOCKED durumda: %s", firstPath)
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
			return "INVALID_PATH", fmt.Sprintf("path bulunamadı: %s", pathID)
		}

		currentRegion, ok := getUnitCurrentRegion(unitID)
		if !ok {
			return "NOT_YOUR_UNIT", fmt.Sprintf("unit region bilgisi bulunamadı: %s", unitID)
		}

		if currentRegion != path.From && currentRegion != path.To {
			return "UNIT_NOT_ADJACENT", fmt.Sprintf(
				"%s şu anda %s bölgesinde; %s path endpointlerinde değil (%s, %s)",
				unitID,
				currentRegion,
				pathID,
				path.From,
				path.To,
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
		return "INVALID_TARGET", "ATTACK_REGION için targetRegion/regionId boş"
	}

	currentRegion, ok := getUnitCurrentRegion(unitID)
	if !ok {
		return "NOT_YOUR_UNIT", fmt.Sprintf("unit region bilgisi bulunamadı: %s", unitID)
	}

	if !areAdjacent(currentRegion, targetRegion) {
		return "INVALID_TARGET", fmt.Sprintf("%s bölgesinden %s bölgesine saldırı adjacent değil", currentRegion, targetRegion)
	}

	occupant := NodeOccupants[targetRegion]
	if occupant == "" {
		return "INVALID_TARGET", fmt.Sprintf("Hedef bölgede saldırılacak enemy unit yok: %s", targetRegion)
	}

	attackerConfig, ok := getUnitConfig(unitID)
	if !ok {
		return "NOT_YOUR_UNIT", fmt.Sprintf("attacker config bulunamadı: %s", unitID)
	}

	defenderConfig, ok := getUnitConfig(occupant)
	if !ok {
		return "INVALID_TARGET", fmt.Sprintf("defender config bulunamadı: %s", occupant)
	}

	if attackerConfig.Side == defenderConfig.Side {
		return "INVALID_TARGET", fmt.Sprintf("Hedef bölgede enemy değil ally var: %s", occupant)
	}

	return "", ""
}

func validateCooldownRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	if isLegacyMove || order.OrderType != OrderMaiaAbility {
		return "", ""
	}

	if getCooldown(unitID) > 0 {
		return "ABILITY_ON_COOLDOWN", fmt.Sprintf("%s ability cooldown'da: %d", unitID, getCooldown(unitID))
	}

	unitConfig, ok := getUnitConfig(unitID)
	if !ok {
		return "NOT_YOUR_UNIT", fmt.Sprintf("unit config bulunamadı: %s", unitID)
	}

	if !unitConfig.Maia {
		return "INVALID_TARGET", fmt.Sprintf("%s Maia değil, MAIA_ABILITY kullanamaz", unitID)
	}

	return "", ""
}

func validateDuplicateRule(order OrderPayload, unitID string, isLegacyMove bool) (string, string) {
	turn := order.Turn
	if isLegacyMove {
		turn = GetCurrentTurn()
	}

	if isDuplicateUnitOrder(turn, unitID) {
		return "DUPLICATE_UNIT_ORDER", fmt.Sprintf("Aynı turn içinde aynı unit için ikinci emir: turn=%d, unitId=%s", turn, unitID)
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

	status := PathStatus[pathID]
	if status == "" {
		return "OPEN"
	}

	return status
}

func setPathStatus(pathID string, status string) {
	pathStateMu.Lock()
	defer pathStateMu.Unlock()
	PathStatus[pathID] = status
}
func SetPathStatusForTest(pathID string, status string) {
	setPathStatus(pathID, status)
	fmt.Printf("🧪 Test için path status ayarlandı: %s -> %s\n", pathID, status)
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
		return false, fmt.Sprintf("Geçersiz veya eksik playerId: %s", playerID)
	}

	unitSide, found := getUnitSide(unitID)
	if !found {
		return false, fmt.Sprintf("Unit bulunamadı: %s", unitID)
	}

	if unitSide != expectedSide {
		return false, fmt.Sprintf(
			"Unit bu oyuncuya ait değil: playerId=%s beklenenSide=%s unitId=%s unitSide=%s",
			playerID,
			expectedSide,
			unitID,
			unitSide,
		)
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

// writeDLQ geçersiz emirleri game.dlq topic'ine yazar.
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
		fmt.Printf("❌ DLQ payload oluşturulamadı: %v\n", err)
		return
	}

	if err := ProduceMessage(producer, TopicDLQ, payload); err != nil {
		fmt.Printf("❌ DLQ Kafka'ya yazılamadı: %v\n", err)
		return
	}

	fmt.Printf("🧯 Geçersiz emir game.dlq topic'ine yazıldı | errorCode=%s\n", errorCode)
}

// processValidatedOrder doğrulanmış emri oyun motoruna uygular.
func processValidatedOrder(event Event, lightSideSSECh chan<- Event, darkSideSSECh chan<- Event) {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		fmt.Println("❌ Validated emir okunamadı:", err)
		return
	}

	fmt.Println("📩 Kafka'dan validated emir yakalandı!")

	unitID := normalizedUnitID(order)

	source := order.Source
	target := order.Target

	if source == "" || target == "" {
		pathIDs := order.PathIDs
		if len(pathIDs) == 0 {
			pathIDs = order.NewPathIDs
		}

		if len(pathIDs) == 0 {
			// ATTACK_REGION gibi route dışı order'ları şimdilik engine'e uygulamıyoruz.
			fmt.Printf("ℹ️ %s order validated edildi fakat ProcessTurn route move olmadığı için uygulanmadı.\n", order.OrderType)
			return
		}

		var err error
		source, target, err = ResolveMoveFromPath(unitID, pathIDs[0])
		if err != nil {
			fmt.Println("❌ Route çözümlenemedi:", err)
			return
		}
	}

	if err := ProcessTurn(unitID, source, target); err != nil {
		fmt.Println("❌ Emir işlenemedi:", err)
		return
	}

	successMsg := fmt.Sprintf(`{"message":"%s, %s bölgesine başarıyla ulaştı!"}`, unitID, target)
	broadcastEvent := Event{
		Topic:   TopicBroadcast,
		Payload: []byte(successMsg),
	}

	lightSideSSECh <- broadcastEvent
	darkSideSSECh <- stripRingBearer(broadcastEvent)
}

// stripRingBearer Karanlık Taraf'a giden payload içinden Ring Bearer konumunu siler.
// Hem eski basit WorldStateSnapshot formatını hem de yeni BroadcastWorldStateSnapshot formatını destekler.
func stripRingBearer(event Event) Event {
	var payload map[string]interface{}

	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return event
	}

	// Yeni format: { eventType, turn, state: { units, lightView, darkView } }
	if stateRaw, ok := payload["state"].(map[string]interface{}); ok {
		stripRingBearerFromStateMap(stateRaw)

		newPayload, err := json.Marshal(payload)
		if err == nil {
			return Event{
				Topic:   event.Topic,
				Payload: newPayload,
			}
		}

		return event
	}

	// Eski format: { turn, units, regions, ... }
	stripRingBearerFromStateMap(payload)

	newPayload, err := json.Marshal(payload)
	if err != nil {
		return event
	}

	return Event{
		Topic:   event.Topic,
		Payload: newPayload,
	}
}

func stripRingBearerFromStateMap(state map[string]interface{}) {
	// units.ring-bearer.region = ""
	if unitsRaw, ok := state["units"].(map[string]interface{}); ok {
		if rbRaw, ok := unitsRaw[RingBearerID].(map[string]interface{}); ok {
			rbRaw["region"] = ""
			unitsRaw[RingBearerID] = rbRaw
		}
	}

	// lightView.ringBearerRegion da Dark Side payload'ında görünmesin.
	if lightViewRaw, ok := state["lightView"].(map[string]interface{}); ok {
		lightViewRaw["ringBearerRegion"] = ""
		state["lightView"] = lightViewRaw
	}

	// darkView zaten boş kalmalı.
	if darkViewRaw, ok := state["darkView"].(map[string]interface{}); ok {
		darkViewRaw["ringBearerRegion"] = ""
		state["darkView"] = darkViewRaw
	}
}
// enrichRouteRisk valid order payload'ına routeRiskScore, threatenedPaths ve blockedPaths ekler.
func enrichRouteRisk(event Event) Event {
	var order OrderPayload
	if err := json.Unmarshal(event.Payload, &order); err != nil {
		fmt.Printf("⚠️ Route risk enrichment yapılamadı: %v\n", err)
		return event
	}

	// Sadece route içeren order'ları enrich ediyoruz.
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

	score, threatenedPaths, blockedPaths := CalculateRouteRisk(normalizedUnitID(order), pathIDs)

	order.RouteRiskScore = &score
	order.ThreatenedPaths = threatenedPaths
	order.BlockedPaths = blockedPaths

	payload, err := json.Marshal(order)
	if err != nil {
		fmt.Printf("⚠️ Enriched order JSON oluşturulamadı: %v\n", err)
		return event
	}

	fmt.Printf("📊 Route risk hesaplandı | unit=%s | riskScore=%d | threatened=%v | blocked=%v\n",
		normalizedUnitID(order),
		score,
		threatenedPaths,
		blockedPaths,
	)

	return Event{
		Topic:   TopicOrdersValidated,
		Payload: payload,
	}
}

// CalculateRouteRisk proje formülüne göre route risk skorunu hesaplar.
func CalculateRouteRisk(unitID string, pathIDs []string) (int, []string, []string) {
	score := 0
	threatenedPaths := []string{}
	blockedPaths := []string{}

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
			threatenedPaths = append(threatenedPaths, pathID)

		case "BLOCKED":
			score += 5
			blockedPaths = append(blockedPaths, pathID)
		}
	}

	nazgulProximityCount := countNazgulWithinTwoHops(destinationRegions)
	score += nazgulProximityCount * 2

	return score, threatenedPaths, blockedPaths
}

// resolveRouteDestinationRegions verilen path route'una göre gidilecek region listesini çıkarır.
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
			// Route current region ile bağlanmıyorsa güvenli fallback.
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

	// Prototip PathStatus map'i testlerde BLOCKED/THREATENED set ettiyse onu öncelikli kullan.
	status := getPathStatus(pathID)
	if status != "" {
		path.Status = status
	}

	return path, true
}

func countNazgulWithinTwoHops(routeRegions []string) int {
	count := 0

	worldStateMu.RLock()
	units := make([]UnitSnapshot, 0, len(WorldState.Units))
	for _, unit := range WorldState.Units {
		units = append(units, unit)
	}
	worldStateMu.RUnlock()

	for _, unit := range units {
		if unit.Class != "Nazgul" || unit.Status != "ACTIVE" {
			continue
		}

		for _, routeRegion := range routeRegions {
			distance := graphDistance(unit.Region, routeRegion)
			if distance >= 0 && distance <= 2 {
				count++
				break
			}
		}
	}

	return count
}

// graphDistance iki region arasındaki hop sayısını BFS ile hesaplar.
func graphDistance(start string, target string) int {
	if start == target {
		return 0
	}

	visited := map[string]bool{start: true}
	queue := []struct {
		region string
		dist   int
	}{
		{region: start, dist: 0},
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, neighbor := range neighborsOf(current.region) {
			if visited[neighbor] {
				continue
			}

			if neighbor == target {
				return current.dist + 1
			}

			visited[neighbor] = true
			queue = append(queue, struct {
				region string
				dist   int
			}{
				region: neighbor,
				dist:   current.dist + 1,
			})
		}
	}

	return -1
}

func neighborsOf(regionID string) []string {
	neighbors := []string{}

	for _, path := range LoadedMap.Paths {
		if path.From == regionID {
			neighbors = append(neighbors, path.To)
		} else if path.To == regionID {
			neighbors = append(neighbors, path.From)
		}
	}

	return neighbors
}
