package internal

import (
	"encoding/json"
	"time"
)

type GameStartRequest struct {
	Mode string `json:"mode"`
}

type GameStartResponse struct {
	Message string `json:"message"`
	Mode    string `json:"mode"`
	Turn    int    `json:"turn"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Turn      int    `json:"turn"`
	Timestamp int64  `json:"timestamp"`
}

type AvailableOrder struct {
	OrderType string   `json:"orderType"`
	Side      string   `json:"side"`
	Required  []string `json:"required"`
	Optional  []string `json:"optional,omitempty"`
	Example   string   `json:"example"`
	Notes     string   `json:"notes,omitempty"`
}

type OrdersAvailableResponse struct {
	PlayerID    string           `json:"playerId"`
	UnitID      string           `json:"unitId,omitempty"`
	Turn        int              `json:"turn"`
	Orders      []AvailableOrder `json:"orders"`
	GeneratedAt int64            `json:"generatedAt"`
}

func StartGameFromRequest(req GameStartRequest) GameStartResponse {
	if req.Mode == "" {
		req.Mode = "HVH"
	}

	// Şimdilik yeni oyun başlangıcı config state'ini tekrar kuruyor.
	// Finalde topic cleanup / persistent recovery ayrı ele alınabilir.
	InitWorldStateFromConfig()

	// CurrentTurn değerini de 1'e al.
	currentTurnMu.Lock()
	CurrentTurn = 1
	currentTurnMu.Unlock()

	// Duplicate tracker temizlensin.
	orderTrackerMu.Lock()
	ordersByTurn = make(map[int]map[string]bool)
	orderTrackerMu.Unlock()

	return GameStartResponse{
		Message: "Game started",
		Mode:    req.Mode,
		Turn:    GetCurrentTurn(),
	}
}

func GetHealthResponse() HealthResponse {
	return HealthResponse{
		Status:    "ok",
		Turn:      GetCurrentTurn(),
		Timestamp: time.Now().UnixMilli(),
	}
}

func GetOrdersAvailable(playerID string, unitID string) OrdersAvailableResponse {
	if playerID == "" {
		playerID = "light"
	}

	orders := []AvailableOrder{
		{
			OrderType: "ASSIGN_ROUTE",
			Side:      "FREE_PEOPLES",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "pathIds"},
			Example:   `{"orderType":"ASSIGN_ROUTE","playerId":"light","unitId":"ring-bearer","turn":1,"pathIds":["shire-to-bree"]}`,
			Notes:     "Assigns a route to a unit. Currently the first path is applied by the prototype engine.",
		},
		{
			OrderType: "REDIRECT_UNIT",
			Side:      "FREE_PEOPLES",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "newPathIds"},
			Example:   `{"orderType":"REDIRECT_UNIT","playerId":"light","unitId":"ring-bearer","turn":1,"newPathIds":["shire-to-tharbad"]}`,
			Notes:     "Changes an assigned route.",
		},
		{
			OrderType: "BLOCK_PATH",
			Side:      "SHADOW",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "pathId"},
			Example:   `{"orderType":"BLOCK_PATH","playerId":"shadow","unitId":"witch-king","turn":1,"pathId":"rivendell-to-moria"}`,
			Notes:     "Blocks or threatens a path depending on game logic.",
		},
		{
			OrderType: "SEARCH_PATH",
			Side:      "SHADOW",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "pathId"},
			Example:   `{"orderType":"SEARCH_PATH","playerId":"shadow","unitId":"witch-king","turn":1,"pathId":"shire-to-bree"}`,
			Notes:     "Searches a path for Ring Bearer traces.",
		},
		{
			OrderType: "ATTACK_REGION",
			Side:      "BOTH",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "targetRegion"},
			Example:   `{"orderType":"ATTACK_REGION","playerId":"light","unitId":"aragorn","turn":1,"targetRegion":"weathertop"}`,
			Notes:     "Attacks an adjacent region if an enemy unit is present.",
		},
		{
			OrderType: "MAIA_ABILITY",
			Side:      "BOTH",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "pathId"},
			Example:   `{"orderType":"MAIA_ABILITY","playerId":"light","unitId":"gandalf","turn":1,"pathId":"shire-to-bree"}`,
			Notes:     "Gandalf/Saruman/Sauron effects are config-driven/prototype level.",
		},
		{
			OrderType: "FORTIFY_REGION",
			Side:      "FREE_PEOPLES",
			Required:  []string{"orderType", "playerId", "unitId", "turn", "regionId"},
			Example:   `{"orderType":"FORTIFY_REGION","playerId":"light","unitId":"gondor-army","turn":1,"regionId":"minas-tirith"}`,
			Notes:     "Fortifies a region in the full turn processor.",
		},
	}

	return OrdersAvailableResponse{
		PlayerID:    playerID,
		UnitID:      unitID,
		Turn:        GetCurrentTurn(),
		Orders:      orders,
		GeneratedAt: time.Now().UnixMilli(),
	}
}

func ToJSON(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}
