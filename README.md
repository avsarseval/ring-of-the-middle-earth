# Ring of the Middle Earth — Option B Go Implementation

A distributed, Kafka-based turn-based strategy game engine inspired by *The Lord of the Rings*.  
This implementation focuses on the **Option B — Go** requirements: Kafka pipelines, validation, route risk analysis, interception analysis, information hiding, detection, game state broadcasting, transactional GameOver events, multi-instance consumer group rebalance, and a basic demo UI.

---

## Highlights

- Kafka-based order pipeline: `raw → validated → engine`
- 8 validation rules with DLQ support
- Route risk enrichment pipeline
- Interception analysis pipeline
- Light/Dark information hiding
- Ring Bearer detection system
- Transactional GameOver demo
- 3 Go instances with consumer group rebalance
- Demo UI and SSE event stream
- Go unit tests and race tests

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
main.go                         HTTP API, SSE endpoints, server startup
internal/router.go              Event routing, validation flow, SSE distribution
internal/processor.go           Basic turn processing and movement/combat handling
internal/cache.go               WorldStateCache and light/dark visibility logic
internal/analysis.go            Route risk and interception analysis
internal/detection.go           Ring Bearer detection logic
internal/gameplay_actions.go    Combat, Maia abilities, block/search/fortify actions
internal/game_over.go           Transactional GameOver producer demo
internal/kafka_client.go        Kafka producer/consumer setup
```

---

## 3. Kafka Topics

```text
game.orders.raw
game.orders.validated
game.dlq
game.broadcast
game.ring.position
game.ring.detection
game.events.unit
game.events.region
game.events.path
```

Topic configuration can be checked with:

```bash
./test/kafka_topics_check.sh
```

---

## 4. HTTP API

| Endpoint | Method | Description |
|---|---|---|
| `/game/start` | POST | Starts/resets the game with mode `HVH` |
| `/order` | POST | Publishes an order to `game.orders.raw` |
| `/game/state?side=light` | GET | Returns Light Side world state |
| `/game/state?side=shadow` | GET | Returns Dark Side world state |
| `/analysis/routes` | GET | Route risk analysis |
| `/analysis/intercept` | GET | Interception analysis |
| `/events?side=light` | GET | SSE stream for Light Side |
| `/events?side=shadow` | GET | SSE stream for Shadow Side |
| `/health` | GET | Health check endpoint |

Example:

```bash
curl -s http://localhost:8080/health | jq
```

```bash
curl -X POST http://localhost:8080/game/start \
  -H "Content-Type: application/json" \
  -d '{"mode":"HVH"}'
```

---

## 5. Order Flow

```text
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
```

Invalid orders are written to:

```text
game.dlq
```

---

## 6. Validation Rules and DLQ

Implemented validation rules:

```text
WRONG_TURN
NOT_YOUR_UNIT
PATH_BLOCKED
INVALID_PATH
UNIT_NOT_ADJACENT
INVALID_TARGET
ABILITY_ON_COOLDOWN
DUPLICATE_UNIT_ORDER
```

Example DLQ payload:

```json
{
  "originalTopic": "game.orders.raw",
  "errorCode": "WRONG_TURN",
  "errorMessage": "...",
  "rawPayload": "...",
  "timestamp": 1770000000000
}
```

---

## 7. Pipeline 1 — Route Risk Analysis

Endpoint:

```http
GET /analysis/routes?playerId=light
```

Risk scoring considers:

```text
region threatLevel
path surveillanceLevel
THREATENED paths
BLOCKED paths
Nazgul proximity
```

Example output:

```json
{
  "playerId": "light",
  "unitId": "ring-bearer",
  "turn": 1,
  "routes": [
    {
      "name": "Route 1 - Fellowship",
      "riskScore": 24
    }
  ]
}
```

---

## 8. Pipeline 2 — Interception Analysis

Endpoint:

```http
GET /analysis/intercept?playerId=shadow
```

Example:

```json
{
  "playerId": "shadow",
  "turn": 4,
  "plans": [
    {
      "unitId": "witch-king",
      "targetRegion": "the-shire",
      "score": 0.75
    }
  ]
}
```

---

## 9. Information Hiding

Light Side state:

```bash
curl -s "http://localhost:8080/game/state?side=light"
```

Dark Side state:

```bash
curl -s "http://localhost:8080/game/state?side=shadow"
```

The Ring Bearer location is hidden from the Shadow side.

---

## 10. Detection System

Implemented behavior:

```text
Turn 1-3: detection disabled
Turn 4+: Nazgul detection enabled
Sauron passive amplifier: Nazgul detection range +1
```

Example event:

```json
{
  "eventType": "RingBearerDetected",
  "regionId": "the-shire",
  "turn": 4
}
```

---

## 11. Transactional GameOver Demo

Run:

```bash
./test/game_over_transaction_demo.sh demo-k6-1
```

Expected:

```text
ABORTED_ENGINE_CRASH should NOT appear
ENGINE_CRASH_COMMITTED should appear exactly once
```

---

## 12. Multi-Instance Consumer Rebalance

Run:

```bash
docker compose up -d --build --scale game-engine=3 game-engine
```

Stop one instance:

```bash
docker stop ring-of-the-middle-earth-game-engine-3
```

Expected logs:

```text
Consumer group rebalance: partitions revoked
Consumer group rebalance: partitions assigned
```

---

## 13. Running the Project

```bash
docker compose up -d
```

```bash
cd option-b
go run main.go
```

---

## 14. Test Commands

```bash
go test ./...
```

```bash
go test -race ./...
```

---

## 15. Current Status

Implemented:

```text
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
```

The project is ready for final demo preparation and architecture explanation.
