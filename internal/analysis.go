package internal

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PIPELINE 1 — Route Analysis for Light Side (FIX B8)
//
// Architecture (spec §8):
//   Dispatcher → buffered jobCh (cap=20) → 4 worker goroutines → unbuffered resultCh
//   → Aggregator goroutine (closes resultCh when all workers done)
//   → Deliverer (collects results into slice, sorts by risk, returns)
//
// Patterns used:
//   - context.Context with 2-second timeout (returns partial result on timeout)
//   - or-done: every channel send/recv selects on ctx.Done() alongside the data case
//   - sync.WaitGroup at every stage boundary
// ─────────────────────────────────────────────────────────────────────────────

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
	Partial     bool          `json:"partial,omitempty"` // true if timeout hit
}

// AnalyzeRoutesAsync runs Pipeline 1 with full concurrent worker architecture.
// FIX B8: Previously a plain sequential loop — replaced with goroutine pipeline.
func AnalyzeRoutesAsync(ctx context.Context) RouteAnalysisResponse {
	// 2-second timeout per spec §8; if exceeded, returns partial result
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	routes := canonicalRoutes()

	// Stage A: Dispatcher — buffered channel cap=20 (spec §8)
	jobCh := make(chan RouteCandidate, 20)

	// Stage B→C: Worker → Aggregator — unbuffered result channel (spec §8)
	resultCh := make(chan RankedRoute)

	// WaitGroup tracks when all 4 workers finish so Aggregator can close resultCh
	var workerWg sync.WaitGroup

	// ── 4 Worker goroutines ───────────────────────────────────────────────
	for i := 0; i < 4; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				// or-done: stop if context cancelled OR no more jobs
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return // dispatcher closed the channel
					}
					// Compute risk score for this route
					score, threatened, blocked := CalculateRouteRisk(RingBearerID, job.PathIDs)

					// Warn if ring bearer is not adjacent to first path
					warning := ""
					currentRegion, _ := getUnitCurrentRegion(RingBearerID)
					if len(job.PathIDs) > 0 {
						firstPath, ok := getPathByID(job.PathIDs[0])
						if ok && currentRegion != "" &&
							currentRegion != firstPath.From &&
							currentRegion != firstPath.To {
							warning = "first path not adjacent to ring bearer's current position"
						}
					}

					result := RankedRoute{
						Name:            job.Name,
						PathIDs:         job.PathIDs,
						RiskScore:       score,
						ThreatenedPaths: threatened,
						BlockedPaths:    blocked,
						Warning:         warning,
					}

					// or-done: forward result or bail on context cancel
					select {
					case resultCh <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}(i)
	}

	// ── Aggregator goroutine: closes resultCh when all workers are done ──
	go func() {
		workerWg.Wait()
		close(resultCh)
	}()

	// ── Dispatcher: sends all jobs to workers ────────────────────────────
	go func() {
		defer close(jobCh)
		for _, route := range routes {
			select {
			case jobCh <- route:
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── Deliverer: collects results ──────────────────────────────────────
	var ranked []RankedRoute
	partial := false

	for {
		select {
		case result, open := <-resultCh:
			if !open {
				goto done // all workers done, resultCh closed by aggregator
			}
			ranked = append(ranked, result)
		case <-ctx.Done():
			partial = true
			goto done
		}
	}

done:
	// Sort by risk score ascending (safest route first)
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].RiskScore < ranked[j].RiskScore
	})

	recommended := RankedRoute{}
	if len(ranked) > 0 {
		recommended = ranked[0]
	}

	return RouteAnalysisResponse{
		PlayerID:    "light",
		UnitID:      RingBearerID,
		Turn:        GetCurrentTurn(),
		Routes:      ranked,
		Recommended: recommended,
		GeneratedAt: time.Now().UnixMilli(),
		Partial:     partial,
	}
}

// AnalyzeRoutesForLightSide is the backward-compat wrapper used by existing tests.
// FIX B8: Now calls the concurrent pipeline under the hood.
func AnalyzeRoutesForLightSide() RouteAnalysisResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return AnalyzeRoutesAsync(ctx)
}

func GetRouteAnalysisJSON() ([]byte, error) {
	return json.MarshalIndent(AnalyzeRoutesForLightSide(), "", "  ")
}

// ─────────────────────────────────────────────────────────────────────────────
// PIPELINE 2 — Intercept Analysis for Dark Side (FIX B8)
//
// Same pipeline architecture as Pipeline 1 but:
//   - buffered jobCh cap=30 (spec §8)
//   - 4 workers compute intercept opportunities per Nazgul × route
//   - result capped at 12 best intercept plans (spec §8)
// ─────────────────────────────────────────────────────────────────────────────

type InterceptJob struct {
	Unit  UnitSnapshot
	Route RouteCandidate
}

type InterceptCandidate struct {
	UnitID           string  `json:"unitId"`
	UnitRegion       string  `json:"unitRegion"`
	TargetRegion     string  `json:"targetRegion"`
	RouteName        string  `json:"routeName"`
	TurnsToIntercept int     `json:"turnsToIntercept"`
	RBTurnsToReach   int     `json:"rbTurnsToReach"`
	Score            float64 `json:"score"`
}

type InterceptAnalysisResponse struct {
	PlayerID    string               `json:"playerId"`
	Turn        int                  `json:"turn"`
	Plans       []InterceptCandidate `json:"plans"`
	GeneratedAt int64                `json:"generatedAt"`
	Partial     bool                 `json:"partial,omitempty"`
}

// AnalyzeInterceptAsync runs Pipeline 2 with the full concurrent worker architecture.
// FIX B8: Previously a sequential nested loop — replaced with goroutine pipeline.
func AnalyzeInterceptAsync(ctx context.Context) InterceptAnalysisResponse {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	routes := canonicalRoutes()
	nazgulUnits := GetActiveNazgulUnits()

	// Build the job list (Nazgul × route combinations)
	var jobs []InterceptJob
	for _, unit := range nazgulUnits {
		for _, route := range routes {
			jobs = append(jobs, InterceptJob{Unit: unit, Route: route})
		}
	}

	// Stage A: Dispatcher — buffered channel cap=30 (spec §8)
	jobCh := make(chan InterceptJob, 30)

	// Stage B→C: Worker → Aggregator — unbuffered result channel
	resultCh := make(chan InterceptCandidate)

	var workerWg sync.WaitGroup

	// ── 4 Worker goroutines ───────────────────────────────────────────────
	for i := 0; i < 4; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return
					}
					plans := computeInterceptPlans(job)
					for _, plan := range plans {
						select {
						case resultCh <- plan:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}(i)
	}

	// ── Aggregator ───────────────────────────────────────────────────────
	go func() {
		workerWg.Wait()
		close(resultCh)
	}()

	// ── Dispatcher ───────────────────────────────────────────────────────
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case jobCh <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── Deliverer ────────────────────────────────────────────────────────
	var plans []InterceptCandidate
	partial := false

	for {
		select {
		case plan, open := <-resultCh:
			if !open {
				goto done
			}
			plans = append(plans, plan)
		case <-ctx.Done():
			partial = true
			goto done
		}
	}

done:
	// Sort by score descending (best intercept opportunity first)
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].Score > plans[j].Score
	})
	// Cap at 12 best plans per spec §8
	if len(plans) > 12 {
		plans = plans[:12]
	}

	return InterceptAnalysisResponse{
		PlayerID:    "shadow",
		Turn:        GetCurrentTurn(),
		Plans:       plans,
		GeneratedAt: time.Now().UnixMilli(),
		Partial:     partial,
	}
}

// computeInterceptPlans generates intercept candidates for one Nazgul × route combination.
func computeInterceptPlans(job InterceptJob) []InterceptCandidate {
	var plans []InterceptCandidate
	routeRegions := resolveRouteDestinationRegions(RingBearerID, job.Route.PathIDs)

	cumulativeCost := 0
	for i, regionID := range routeRegions {
		if i < len(job.Route.PathIDs) {
			if path, ok := getPathByID(job.Route.PathIDs[i]); ok {
				cumulativeCost += path.Cost
			} else {
				cumulativeCost++
			}
		}

		turnsToIntercept := graphDistance(job.Unit.Region, regionID)
		if turnsToIntercept < 0 {
			continue
		}

		routeLength := len(job.Route.PathIDs)
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
			UnitID:           job.Unit.ID,
			UnitRegion:       job.Unit.Region,
			TargetRegion:     regionID,
			RouteName:        job.Route.Name,
			TurnsToIntercept: turnsToIntercept,
			RBTurnsToReach:   cumulativeCost,
			Score:            score,
		})
	}
	return plans
}

// AnalyzeInterceptionForDarkSide is the backward-compat wrapper used by existing tests.
func AnalyzeInterceptionForDarkSide() InterceptAnalysisResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return AnalyzeInterceptAsync(ctx)
}

func GetInterceptAnalysisJSON() ([]byte, error) {
	return json.MarshalIndent(AnalyzeInterceptionForDarkSide(), "", "  ")
}

// GetActiveNazgulUnits returns all ACTIVE units whose config has DetectionRange > 0.
// FIX B1: Previously used unit.Class == "Nazgul" — now config-driven.
func GetActiveNazgulUnits() []UnitSnapshot {
	worldStateMu.RLock()
	defer worldStateMu.RUnlock()

	var result []UnitSnapshot
	for _, unit := range WorldState.Units {
		if unit.Status != "ACTIVE" {
			continue
		}
		// FIX B1: check config.DetectionRange, not class name string
		cfg, ok := WorldState.UnitConfigs[unit.ID]
		if ok && cfg.DetectionRange > 0 {
			result = append(result, unit)
		}
	}
	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// CANONICAL ROUTES (unchanged — 4 named routes from spec §2.3)
// ─────────────────────────────────────────────────────────────────────────────

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
