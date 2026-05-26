package internal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// UnitRoute tracks a unit's currently assigned route.
// FIX B10: Required for auto-advance in Step 7 of the 13-step turn processor.
type UnitRoute struct {
	PathIDs  []string `json:"pathIds"`
	RouteIdx int      `json:"routeIdx"`
}

// UnitSnapshot is the per-unit in-memory game state.
type UnitSnapshot struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Class    string `json:"class"`
	Side     string `json:"side"`
	Region   string `json:"region"`
	Strength int    `json:"strength"`
	Status   string `json:"status"` // ACTIVE | DESTROYED | RESPAWNING
}

// RegionState holds the mutable state for one region.
// FIX B3: Terrain field added — required for combat terrain bonus (FORTRESS +2, MOUNTAINS +1).
// Previously missing; terrain bonus was never applied.
type RegionState struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Terrain      string `json:"terrain"` // FIX B3: PLAINS|MOUNTAINS|FOREST|FORTRESS|VOLCANIC|SWAMP
	ControlledBy string `json:"controlledBy"`
	ThreatLevel  int    `json:"threatLevel"`
	Fortified    bool   `json:"fortified"`
}

// PathState holds the mutable state for one path.
type PathState struct {
	ID                string `json:"id"`
	From              string `json:"from"`
	To                string `json:"to"`
	Cost              int    `json:"cost"`
	Status            string `json:"status"` // OPEN|THREATENED|BLOCKED|TEMPORARILY_OPEN
	SurveillanceLevel int    `json:"surveillanceLevel"`
}

// LightSideView — only the Light Side can see these fields.
type LightSideView struct {
	RingBearerRegion string   `json:"ringBearerRegion"`
	AssignedRoute    []string `json:"assignedRoute"`
	RouteIdx         int      `json:"routeIdx"`
}

// DarkSideView — RingBearerRegion is ALWAYS "". Enforced here and in stripRingBearer.
type DarkSideView struct {
	RingBearerRegion   string `json:"ringBearerRegion"` // INVARIANT: always ""
	LastDetectedRegion string `json:"lastDetectedRegion"`
	LastDetectedTurn   int    `json:"lastDetectedTurn"`
}

// WorldStateCache is the single in-memory game state.
// FIX B2 / B10: Added timer maps required by the 13-step turn processor.
// On restart, the consumer replays its assigned Kafka partitions to rebuild this.
type WorldStateCache struct {
	Turn        int                     `json:"turn"`
	Units       map[string]UnitSnapshot `json:"units"`
	Regions     map[string]RegionState  `json:"regions"`
	Paths       map[string]PathState    `json:"paths"`
	UnitConfigs map[string]UnitConfig   `json:"-"` // read-only after startup
	LightView   LightSideView           `json:"lightView"`
	DarkView    DarkSideView            `json:"darkView"`
	UpdatedAt   int64                   `json:"updatedAt"`

	// FIX B10: 13-step turn processor state
	UnitRoutes     map[string]UnitRoute `json:"unitRoutes"`     // unitId → assigned route
	TempOpenTimers map[string]int       `json:"tempOpenTimers"` // pathId → turns remaining
	FortifyTimers  map[string]int       `json:"fortifyTimers"`  // regionId → turns remaining
	RespawnTimers  map[string]int       `json:"respawnTimers"`  // unitId → turns remaining
}

var (
	worldStateMu sync.RWMutex
	WorldState   WorldStateCache
)

// InitWorldStateFromConfig builds the initial state from loaded config files.
// FIX B3: Terrain is now populated in RegionState from RegionConfig.
func InitWorldStateFromConfig() {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	fmt.Println("🧠 Initialising WorldStateCache from config...")

	WorldState = WorldStateCache{
		Turn:           1,
		Units:          make(map[string]UnitSnapshot),
		Regions:        make(map[string]RegionState),
		Paths:          make(map[string]PathState),
		UnitConfigs:    make(map[string]UnitConfig),
		UnitRoutes:     make(map[string]UnitRoute),
		TempOpenTimers: make(map[string]int),
		FortifyTimers:  make(map[string]int),
		RespawnTimers:  make(map[string]int),
		LightView: LightSideView{
			RingBearerRegion: "",
			AssignedRoute:    []string{},
		},
		DarkView: DarkSideView{
			RingBearerRegion: "", // invariant — never set to a real value
		},
		UpdatedAt: time.Now().UnixMilli(),
	}

	// Build unit states from config
	for _, unit := range LoadedUnits {
		WorldState.UnitConfigs[unit.ID] = unit

		region := unit.StartRegion
		if unit.ID == RingBearerID {
			WorldState.LightView.RingBearerRegion = region
			WorldState.DarkView.RingBearerRegion = "" // dark side never sees this
		}

		WorldState.Units[unit.ID] = UnitSnapshot{
			ID:       unit.ID,
			Name:     unit.Name,
			Class:    unit.Class,
			Side:     unit.Side,
			Region:   region,
			Strength: unit.Strength,
			Status:   "ACTIVE",
		}
		NodeOccupants[region] = unit.ID
	}

	// FIX B3: Build region states with Terrain field
	for _, region := range LoadedMap.Regions {
		WorldState.Regions[region.ID] = RegionState{
			ID:           region.ID,
			Name:         region.Name,
			Terrain:      region.Terrain, // FIX: was missing, now populated
			ControlledBy: region.StartControl,
			ThreatLevel:  region.StartThreat,
			Fortified:    false,
		}
	}

	// Build path states
	for _, path := range LoadedMap.Paths {
		WorldState.Paths[path.ID] = PathState{
			ID:                path.ID,
			From:              path.From,
			To:                path.To,
			Cost:              path.Cost,
			Status:            "OPEN",
			SurveillanceLevel: 0,
		}
	}

	fmt.Printf("✅ WorldState ready: units=%d regions=%d paths=%d turn=%d\n",
		len(WorldState.Units), len(WorldState.Regions), len(WorldState.Paths), WorldState.Turn)
}

// GetWorldStateCopy returns a safe value copy of the current world state.
func GetWorldStateCopy() WorldStateCache {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()
	return copyWorldState(WorldState)
}

// GetPublicWorldStateJSON returns state JSON filtered for the given side.
// FIX B7: Dark Side never receives the Ring Bearer's real region via this endpoint.
func GetPublicWorldStateJSON(side string) ([]byte, error) {
	worldStateMu.RLock()
	copyState := copyWorldState(WorldState)
	worldStateMu.RUnlock()

	if side == "shadow" || side == "dark" || side == "SHADOW" {
		// Enforce information asymmetry — strip all ring bearer location data
		copyState.LightView.RingBearerRegion = ""
		copyState.DarkView.RingBearerRegion = ""
		if rb, ok := copyState.Units[RingBearerID]; ok {
			rb.Region = ""
			copyState.Units[RingBearerID] = rb
		}
	}

	return json.MarshalIndent(copyState, "", "  ")
}

func copyWorldState(src WorldStateCache) WorldStateCache {
	dst := src

	dst.Units = make(map[string]UnitSnapshot, len(src.Units))
	for k, v := range src.Units {
		dst.Units[k] = v
	}

	dst.Regions = make(map[string]RegionState, len(src.Regions))
	for k, v := range src.Regions {
		dst.Regions[k] = v
	}

	dst.Paths = make(map[string]PathState, len(src.Paths))
	for k, v := range src.Paths {
		dst.Paths[k] = v
	}

	dst.UnitConfigs = make(map[string]UnitConfig, len(src.UnitConfigs))
	for k, v := range src.UnitConfigs {
		dst.UnitConfigs[k] = v
	}

	// FIX: copy new timer maps
	dst.UnitRoutes = make(map[string]UnitRoute, len(src.UnitRoutes))
	for k, v := range src.UnitRoutes {
		pathsCopy := make([]string, len(v.PathIDs))
		copy(pathsCopy, v.PathIDs)
		dst.UnitRoutes[k] = UnitRoute{PathIDs: pathsCopy, RouteIdx: v.RouteIdx}
	}

	dst.TempOpenTimers = make(map[string]int, len(src.TempOpenTimers))
	for k, v := range src.TempOpenTimers {
		dst.TempOpenTimers[k] = v
	}

	dst.FortifyTimers = make(map[string]int, len(src.FortifyTimers))
	for k, v := range src.FortifyTimers {
		dst.FortifyTimers[k] = v
	}

	dst.RespawnTimers = make(map[string]int, len(src.RespawnTimers))
	for k, v := range src.RespawnTimers {
		dst.RespawnTimers[k] = v
	}

	dst.LightView.AssignedRoute = append([]string{}, src.LightView.AssignedRoute...)
	return dst
}

// UpdateUnitRegion updates a unit's position in the cache.
// FIX B7: Ring Bearer position is kept strictly in LightView, never in DarkView.
func UpdateUnitRegion(unitID string, newRegion string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()

	unit, ok := WorldState.Units[unitID]
	if !ok {
		return
	}
	unit.Region = newRegion
	WorldState.Units[unitID] = unit

	if unitID == RingBearerID {
		WorldState.LightView.RingBearerRegion = newRegion
		WorldState.DarkView.RingBearerRegion = "" // invariant maintained
	}
	WorldState.UpdatedAt = time.Now().UnixMilli()
}

// UpdatePathStatus updates a path's status in the cache.
func UpdatePathStatus(pathID string, status string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()
	if path, ok := WorldState.Paths[pathID]; ok {
		path.Status = status
		WorldState.Paths[pathID] = path
		WorldState.UpdatedAt = time.Now().UnixMilli()
	}
}

// UpdateSurveillanceLevel updates a path's surveillance level.
func UpdateSurveillanceLevel(pathID string, level int) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()
	if path, ok := WorldState.Paths[pathID]; ok {
		if level > 3 {
			level = 3
		}
		path.SurveillanceLevel = level
		WorldState.Paths[pathID] = path
		WorldState.UpdatedAt = time.Now().UnixMilli()
	}
}

// AssignUnitRoute stores a route for a unit (Step 2 of turn processor).
func AssignUnitRoute(unitID string, pathIDs []string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()
	WorldState.UnitRoutes[unitID] = UnitRoute{
		PathIDs:  pathIDs,
		RouteIdx: 0,
	}
	if unitID == RingBearerID {
		WorldState.LightView.AssignedRoute = pathIDs
		WorldState.LightView.RouteIdx = 0
	}
}

// RedirectUnitRoute replaces a unit's route mid-journey.
func RedirectUnitRoute(unitID string, newPathIDs []string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()
	WorldState.UnitRoutes[unitID] = UnitRoute{
		PathIDs:  newPathIDs,
		RouteIdx: 0,
	}
	if unitID == RingBearerID {
		WorldState.LightView.AssignedRoute = newPathIDs
		WorldState.LightView.RouteIdx = 0
	}
}

// AdvanceUnitRouteIdx increments a unit's route position after a successful auto-advance.
func AdvanceUnitRouteIdx(unitID string) {
	worldStateMu.Lock()
	defer worldStateMu.Unlock()
	if route, ok := WorldState.UnitRoutes[unitID]; ok {
		route.RouteIdx++
		WorldState.UnitRoutes[unitID] = route
		if unitID == RingBearerID {
			WorldState.LightView.RouteIdx = route.RouteIdx
		}
	}
}

// CacheManager goroutine reads cache update notifications.
// FIX B9: Used as case 5 target in the 7-case select loop.
func CacheManager(updateCh <-chan Event) {
	fmt.Println("🧠 CacheManager goroutine started")
	for event := range updateCh {
		fmt.Printf("📦 Cache update event received. Topic: %s\n", event.Topic)
		// In full KTable implementation this would replay partition events.
	}
}
