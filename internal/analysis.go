package internal

import (
	"encoding/json"
	"sort"
	"time"
)

type RouteCandidate struct {
	Name    string   `json:"name"`
	PathIDs []string `json:"pathIds"`
}

type RankedRoute struct {
	Name            string   `json:"name"`
	PathIDs         []string `json:"pathIds"`
	RiskScore       int      `json:"riskScore"`
	ThreatenedPaths []string `json:"threatenedPaths"`
	BlockedPaths    []string `json:"blockedPaths"`
	Warning         string   `json:"warning,omitempty"`
}

type RouteAnalysisResponse struct {
	PlayerID    string        `json:"playerId"`
	UnitID      string        `json:"unitId"`
	Turn        int           `json:"turn"`
	Routes      []RankedRoute `json:"routes"`
	Recommended RankedRoute   `json:"recommended"`
	GeneratedAt int64         `json:"generatedAt"`
}

func canonicalRoutes() []RouteCandidate {
	return []RouteCandidate{
		{
			Name: "Route 1 - Fellowship",
			PathIDs: []string{
				"shire-to-bree",
				"bree-to-weathertop",
				"weathertop-to-rivendell",
				"rivendell-to-moria",
				"moria-to-lothlorien",
				"lothlorien-to-emyn-muil",
				"emyn-muil-to-ithilien",
				"ithilien-to-cirith-ungol",
				"cirith-ungol-to-mount-doom",
			},
		},
		{
			Name: "Route 2 - Northern Bypass",
			PathIDs: []string{
				"shire-to-bree",
				"bree-to-rivendell",
				"rivendell-to-lothlorien",
				"lothlorien-to-emyn-muil",
				"emyn-muil-to-dead-marshes",
				"dead-marshes-to-ithilien",
				"ithilien-to-cirith-ungol",
				"cirith-ungol-to-mount-doom",
			},
		},
		{
			Name: "Route 3 - Dark Route",
			PathIDs: []string{
				"shire-to-bree",
				"bree-to-rivendell",
				"rivendell-to-lothlorien",
				"lothlorien-to-emyn-muil",
				"emyn-muil-to-dead-marshes",
				"dead-marshes-to-mordor",
				"mordor-to-mount-doom",
			},
		},
		{
			Name: "Route 4 - Southern Corridor",
			PathIDs: []string{
				"shire-to-tharbad",
				"tharbad-to-fords-of-isen",
				"fords-of-isen-to-edoras",
				"edoras-to-minas-tirith",
				"minas-tirith-to-osgiliath",
				"osgiliath-to-minas-morgul",
				"minas-morgul-to-cirith-ungol",
				"cirith-ungol-to-mount-doom",
			},
		},
	}
}

func AnalyzeRoutesForLightSide() RouteAnalysisResponse {
	unitID := RingBearerID
	ranked := []RankedRoute{}

	currentRegion, _ := getUnitCurrentRegion(unitID)

	for _, candidate := range canonicalRoutes() {
		score, threatened, blocked := CalculateRouteRisk(unitID, candidate.PathIDs)

		warning := ""
		if len(candidate.PathIDs) > 0 {
			firstPath, ok := getPathByID(candidate.PathIDs[0])
			if ok && currentRegion != "" && currentRegion != firstPath.From && currentRegion != firstPath.To {
				warning = "First path is not adjacent to the Ring Bearer's current region. Route may require redirect/recalculation."
			}
		}

		ranked = append(ranked, RankedRoute{
			Name:            candidate.Name,
			PathIDs:         candidate.PathIDs,
			RiskScore:       score,
			ThreatenedPaths: threatened,
			BlockedPaths:    blocked,
			Warning:         warning,
		})
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].RiskScore < ranked[j].RiskScore
	})

	recommended := RankedRoute{}
	if len(ranked) > 0 {
		recommended = ranked[0]
	}

	return RouteAnalysisResponse{
		PlayerID:    "light",
		UnitID:      unitID,
		Turn:        GetCurrentTurn(),
		Routes:      ranked,
		Recommended: recommended,
		GeneratedAt: time.Now().UnixMilli(),
	}
}

func GetRouteAnalysisJSON() ([]byte, error) {
	response := AnalyzeRoutesForLightSide()
	return json.MarshalIndent(response, "", "  ")
}
type InterceptCandidate struct {
	UnitID           string  `json:"unitId"`
	UnitRegion       string  `json:"unitRegion"`
	TargetRegion     string  `json:"targetRegion"`
	RouteName         string  `json:"routeName"`
	TurnsToIntercept int     `json:"turnsToIntercept"`
	RBTurnsToReach   int     `json:"rbTurnsToReach"`
	Score            float64 `json:"score"`
}

type InterceptAnalysisResponse struct {
	PlayerID    string               `json:"playerId"`
	Turn        int                  `json:"turn"`
	Plans       []InterceptCandidate `json:"plans"`
	GeneratedAt int64                `json:"generatedAt"`
}

func AnalyzeInterceptionForDarkSide() InterceptAnalysisResponse {
	plans := []InterceptCandidate{}

	routes := canonicalRoutes()

	for _, unit := range GetActiveNazgulUnits() {
		for _, route := range routes {
			routeRegions := resolveRouteDestinationRegions(RingBearerID, route.PathIDs)

			cumulativeCost := 0
			for i, regionID := range routeRegions {
				if i < len(route.PathIDs) {
					if path, ok := getPathByID(route.PathIDs[i]); ok {
						cumulativeCost += path.Cost
					} else {
						cumulativeCost++
					}
				}

				turnsToIntercept := graphDistance(unit.Region, regionID)
				if turnsToIntercept < 0 {
					continue
				}

				routeLength := len(route.PathIDs)
				if routeLength == 0 {
					continue
				}

				interceptWindow := cumulativeCost - turnsToIntercept

				score := 0.0
				if interceptWindow >= 0 {
					score = 1.0 - (float64(turnsToIntercept) / float64(routeLength))
					if score < 0 {
						score = 0
					}
				}

				plans = append(plans, InterceptCandidate{
					UnitID:           unit.ID,
					UnitRegion:       unit.Region,
					TargetRegion:     regionID,
					RouteName:         route.Name,
					TurnsToIntercept: turnsToIntercept,
					RBTurnsToReach:   cumulativeCost,
					Score:            score,
				})
			}
		}
	}

	sort.Slice(plans, func(i, j int) bool {
		return plans[i].Score > plans[j].Score
	})

	if len(plans) > 12 {
		plans = plans[:12]
	}

	return InterceptAnalysisResponse{
		PlayerID:    "shadow",
		Turn:        GetCurrentTurn(),
		Plans:       plans,
		GeneratedAt: time.Now().UnixMilli(),
	}
}

func GetInterceptAnalysisJSON() ([]byte, error) {
	response := AnalyzeInterceptionForDarkSide()
	return json.MarshalIndent(response, "", "  ")
}

func GetActiveNazgulUnits() []UnitSnapshot {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	nazgul := []UnitSnapshot{}

	for _, unit := range WorldState.Units {
		if unit.Class == "Nazgul" && unit.Status == "ACTIVE" {
			nazgul = append(nazgul, unit)
		}
	}

	return nazgul
}