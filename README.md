# Ring of the Middle Earth — Option B Go Implementation

A distributed, Kafka-based turn-based strategy game engine inspired by *The Lord of the Rings*.  
This implementation focuses on the **Option B — Go** requirements: Kafka pipelines, validation, route risk analysis, interception analysis, information hiding, detection, game state broadcasting, transactional GameOver events, multi-instance consumer group rebalance, and a basic demo UI.

---

## 1. Project Overview

The game is a Human-vs-Human strategy simulation between:

- **Free Peoples / Light Side**
- **Shadow / Dark Side**

The main objective of the Free Peoples is to move the Ring Bearer to Mount Doom.  
The Shadow side attempts to detect, intercept, block, or destroy the Ring Bearer.

The system is event-driven and uses Kafka topics for command processing, validation, enriched records, game state broadcasts, detection events, and dead-letter handling.

---

## 2. Architecture

The system contains the following main components:

```text
Client / UI
   |
   | HTTP API / SSE
   v
Go Game Engine
   |
   | Produces / Consumes Kafka events
   v
Kafka Topics
   |
   | raw orders, validated orders, DLQ, broadcast, detection
   v
Game State / Analysis / Event Router
```

Main Go components:

```text
main.go                  HTTP API, SSE endpoints, server startup
internal/router.go       Event routing, validation flow, SSE distribution
internal/processor.go    Basic turn processing and movement/combat handling
internal/cache.go        WorldStateCache and light/dark visibility logic
internal/analysis.go     Route risk and interception analysis
internal/detection.go    Ring Bearer detection logic
internal/gameplay_actions.go Combat, Maia abilities, block/search/fortify actions
internal/game_over.go    Transactional GameOver producer demo
internal/kafka_client.go Kafka producer/consumer setup
```

## 3. Kafka Topics

The implementation uses the following Kafka topics:

game.orders.raw
game.orders.validated
game.dlq
game.broadcast
game.ring.position
game.ring.detection
game.events.unit
game.events.region
game.events.path

Topic configuration can be checked with:

./test/kafka_topics_check.sh

This script runs kafka-topics.sh --describe for each required topic and provides evidence for partition, replication, and cleanup configuration.

## 4. HTTP API
Endpoint	Method	Description
/game/start	POST	Starts/resets the game with mode HVH
/order	POST	Publishes an order to game.orders.raw; returns 202 Accepted
/game/state?side=light	GET	Returns Light Side world state
/game/state?side=shadow	GET	Returns Dark Side world state with Ring Bearer region hidden
/orders/available	GET	Lists supported order types and example payloads
/analysis/routes	GET	Pipeline 1: Light Side route risk analysis
/analysis/intercept	GET	Pipeline 2: Shadow interception analysis
/events?side=light	GET	SSE stream for Light Side
/events?side=shadow	GET	SSE stream for Shadow Side
/health	GET	Health check endpoint
/demo/game-over-transaction	POST	K6 transactional GameOver demo

Example:

curl -s http://localhost:8080/health | jq
curl -X POST http://localhost:8080/game/start \
  -H "Content-Type: application/json" \
  -d '{"mode":"HVH"}'
## 5. Order Flow

A valid order follows this pipeline:

POST /order
   ↓
game.orders.raw
   ↓
Validation rules
   ↓
Route risk enrichment
   ↓
game.orders.validated
   ↓
Game engine / processor
   ↓
WorldState update + game.broadcast

Invalid orders are written to:

game.dlq

with an error code and original raw payload.

## 6. Validation Rules and DLQ

The validation layer checks invalid order cases and publishes rejected orders to game.dlq.

Implemented validation rules include:

WRONG_TURN
NOT_YOUR_UNIT
PATH_BLOCKED
INVALID_PATH
UNIT_NOT_ADJACENT
INVALID_TARGET
ABILITY_ON_COOLDOWN
DUPLICATE_UNIT_ORDER

Each invalid order produces a structured DLQ record:

{
  "originalTopic": "game.orders.raw",
  "errorCode": "WRONG_TURN",
  "errorMessage": "...",
  "rawPayload": "...",
  "timestamp": 1770000000000
}
## 7. Pipeline 1 — Route Risk Analysis

Endpoint:

GET /analysis/routes?playerId=light

This pipeline calculates route risk scores for canonical Ring Bearer routes.

Risk scoring considers:

region threatLevel
path surveillanceLevel
THREATENED paths
BLOCKED paths
Nazgul proximity

Example output:

{
  "playerId": "light",
  "unitId": "ring-bearer",
  "turn": 1,
  "routes": [
    {
      "name": "Route 1 - Fellowship",
      "riskScore": 24,
      "threatenedPaths": [],
      "blockedPaths": []
    }
  ],
  "recommended": {
    "name": "Route 1 - Fellowship",
    "riskScore": 24
  }
}

Validated orders are also enriched with:

{
  "routeRiskScore": 3,
  "threatenedPaths": [],
  "blockedPaths": []
}
## 8. Pipeline 2 — Interception Analysis

Endpoint:

GET /analysis/intercept?playerId=shadow

This pipeline calculates possible interception plans for Shadow units, especially Nazgul.

It estimates:

Nazgul current region
target route region
turns required to intercept
Ring Bearer turns to reach the region
interception score

Example:

{
  "playerId": "shadow",
  "turn": 4,
  "plans": [
    {
      "unitId": "witch-king",
      "unitRegion": "rivendell",
      "targetRegion": "the-shire",
      "turnsToIntercept": 2,
      "rbTurnsToReach": 4,
      "score": 0.75
    }
  ]
}
## 9. Information Hiding

The Ring Bearer’s true region must only be visible to the Light Side.

Light Side state:

curl -s "http://localhost:8080/game/state?side=light" | jq '.units["ring-bearer"].region, .lightView.ringBearerRegion'

Expected:

"the-shire"
"the-shire"

Dark Side state:

curl -s "http://localhost:8080/game/state?side=shadow" | jq '.units["ring-bearer"].region, .lightView.ringBearerRegion, .darkView.ringBearerRegion'

Expected:

""
""
""

The same hiding rule is applied to Dark Side SSE payloads through the EventRouter.

## 10. Detection System

The Ring Bearer is hidden during the first turns.

Implemented behavior:

Turn 1-3: detection disabled
Turn 4+: Nazgul detection enabled
Sauron passive amplifier: Nazgul detection range +1

If detection succeeds, the system publishes:

game.ring.detection

Example event:

{
  "eventType": "RingBearerDetected",
  "regionId": "the-shire",
  "turn": 4,
  "timestamp": 1770000000000
}

Dark Side sees detection information through darkView.lastDetectedRegion, but never sees the live Ring Bearer region directly.

## 11. World State Broadcast

At the end of each turn, the engine publishes a WorldStateSnapshot event to:

game.broadcast

Example:

{
  "eventType": "WorldStateSnapshot",
  "turn": 2,
  "state": {
    "turn": 2,
    "units": {},
    "regions": {},
    "paths": {},
    "lightView": {},
    "darkView": {}
  }
}

The EventRouter sends this snapshot to Light and Dark SSE channels.
Dark Side snapshots are filtered to remove Ring Bearer location data.

## 12. Combat and Gameplay Actions

Implemented action handlers include:

ASSIGN_ROUTE
REDIRECT_UNIT
BLOCK_PATH
SEARCH_PATH
ATTACK_REGION
MAIA_ABILITY
FORTIFY_REGION

Combat currently includes:

base unit strength
fortified region defender bonus
controlled region defender bonus
leadership bonus from Maia units
UrukHaiLegion attack modifier

Maia abilities:

Gandalf: makes a path TEMPORARILY_OPEN
Saruman: sets path surveillanceLevel to 3 and marks it THREATENED
Sauron: passive detection amplifier for Nazgul range
## 13. Transactional GameOver Demo

K6 is implemented using Kafka transactional producer semantics.

The demo endpoint:

POST /demo/game-over-transaction?gameId=demo-k6-1

The demo does two writes:

1. Writes GameOver inside a transaction and aborts it
2. Writes GameOver inside a transaction and commits it

A read_committed consumer should only see the committed GameOver event.

Run:

./test/game_over_transaction_demo.sh demo-k6-1

Expected:

ABORTED_ENGINE_CRASH should NOT appear
ENGINE_CRASH_COMMITTED should appear exactly once

Example committed record:

{
  "eventType": "GameOver",
  "gameId": "demo-k6-1",
  "winner": "SHADOW",
  "cause": "ENGINE_CRASH_COMMITTED",
  "turn": 1
}
## 14. Multi-Instance Consumer Rebalance

The Go engine can be run as three Docker instances in the same Kafka consumer group.

Run from the project root:

docker compose up -d --build --scale game-engine=3 game-engine

Check instances:

docker compose ps game-engine

View logs:

docker compose logs -f game-engine

Expected evidence:

game-engine-1 started
game-engine-2 started
game-engine-3 started
Consumer group rebalance: partitions assigned

Stop one instance:

docker stop ring-of-the-middle-earth-game-engine-3

Expected logs:

Consumer group rebalance: partitions revoked
Consumer group rebalance: partitions assigned

Each instance uses a unique transactional producer id based on its container hostname:

game-over-transactional-producer-<hostname>
## 15. Demo UI

A basic UI is available at:

http://localhost:8080

UI features:

Health check
Start Game
Refresh State
End Turn
Send Order
Analyze Routes
Analyze Intercept
Connect SSE as Light or Shadow
View SSE Event Log

Light Side sees the Ring Bearer location.
Shadow Side sees the Ring Bearer location as hidden.

## 16. Running the Project
Start Kafka stack

From the project root:

docker compose up -d
Create/check topics
cd option-b
./test/kafka_topics_check.sh
Run Go server locally
cd option-b
go run main.go
Run 3 Go instances with Docker

From the project root:

docker compose up -d --build --scale game-engine=3 game-engine
## 17. Test Commands

Run all Go tests:

go test ./...

Run race tests:

go test -race ./...

Implemented tests include:

pipeline1_test.go
pipeline2_test.go
state_visibility_test.go
combat_test.go
validation_test.go
router_test.go

These cover:

route analysis
interception analysis
Light/Dark visibility
combat modifier behavior
Maia ability behavior
validation rules
router payload filtering
race safety
## 18. Kafka Demo Scripts

Available scripts:

test/kafka_topics_check.sh
test/schema_evolution_v1_consumer.sh
test/game_over_transaction_demo.sh
K1 — Topic describe
./test/kafka_topics_check.sh
K3 — Schema evolution / V1 consumer compatibility
./test/schema_evolution_v1_consumer.sh

This script reads game.orders.validated and extracts only V1 fields while ignoring V2 enrichment fields such as:

routeRiskScore
threatenedPaths
blockedPaths
K6 — Transactional GameOver
./test/game_over_transaction_demo.sh demo-k6-1
## 19. Known Limitations

The implementation is a strong working prototype for the project requirements, but some areas are simplified:

- Full Kafka Streams KTable is approximated using Go WorldStateCache.
- Combat logic includes key modifiers but is not a complete full game simulation.
- Route assignment currently applies the first path directly in the prototype engine.
- Full long-running persistent state recovery is limited compared to a production system.
- The project specification mentions 14 units, but the provided explicit unit list/config contains 13 units.

These limitations are documented and can be extended in later iterations.

## 20. Current Status

Implemented:

Kafka validation and DLQ
Route risk enrichment
Schema evolution demo
Transactional GameOver demo
WorldStateCache
Light/Dark information hiding
Detection system
SSE event streaming
HTTP API
Demo UI
Multi-instance consumer rebalance
Go tests and race tests
Combat/Maia action layer

The project is ready for final demo preparation and architecture explanation.
