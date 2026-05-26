# Ring of the Middle Earth

**Distributed Application Development — Term Project**  
**Technology Paradigm: Option B — Go + Apache Kafka (Distributed Event-Driven Architecture)**

-----

## Technology Declaration

This project implements the game engine using **Go goroutines and Kafka KTable state stores** (Option B). The system is stateless at the application tier — all authoritative game state lives in Kafka topics. Three Go engine instances form a single consumer group; any instance can handle any request. Fault tolerance is delegated entirely to Kafka’s consumer group rebalance protocol.

-----

## System Overview

The architecture simulates **CQRS** (Command Query Responsibility Segregation) and **Event Sourcing**:

- **Commands** arrive as HTTP POST requests to `/order`, are published to `game.orders.raw`, validated by Topology 1, enriched with route risk scores by Topology 2, and buffered until turn end.
- **Events** are produced by the turn processor after each 60-second turn cycle and fan out through `game.events.*`, `game.ring.position`, and `game.broadcast`.
- **Queries** are served from an in-memory `WorldStateCache` that is rebuilt on restart from the `game.session` log-compacted KTable.
- **Information asymmetry** is enforced by the `EventRouter`: the Ring Bearer’s true position is stripped from all Dark Side channels.

```
Browser A (Light Side)          Browser B (Dark Side)
  POST /order                     POST /order
  GET  /events?side=light         GET  /events?side=shadow
       |                               |
       +──────────── Go HTTP Layer ────+
                         |
              Produces → game.orders.raw
              Consumes ← game.broadcast
                          game.events.*
                          game.ring.position    (Light Side only)
                          game.ring.detection   (Dark Side only)
                         |
                    Kafka Cluster
                  (3 brokers, RF=3)
                         |
              Consumes game.orders.validated
              Produces all game.events.*
                         |
              Go Game Engine
           (3 instances, 1 consumer group)
```

-----

## Prerequisites

|Tool          |Version     |Notes                                               |
|--------------|------------|----------------------------------------------------|
|Docker Desktop|24.x+       |WSL2 backend required on Windows                    |
|Docker Compose|v2.x+       |Bundled with Docker Desktop                         |
|Go            |1.22+       |Only needed for `make test` (no Docker required)    |
|WSL2          |Ubuntu 22.04|Windows users: all commands run inside WSL2 terminal|
|curl          |any         |For manual API testing                              |
|Python 3      |3.8+        |Used by `register_schemas.sh` for JSON escaping     |


> **Windows users:** open a WSL2 terminal before running any command in this guide.

-----

## Quick Start

### 1. Clone and enter the repository

```bash
git clone <your-repo-url>
cd ring-of-the-middle-earth
```

### 2. Start the full infrastructure

```bash
make up
```

This command:

1. Starts ZooKeeper, 3 Kafka brokers, Schema Registry, and 3 Go engine instances via Docker Compose
1. Waits 30 seconds for the cluster to reach quorum
1. Creates all 10 Kafka topics with `--replication-factor 3`
1. Registers all Avro schemas (V1 then V2 for schema evolution demo)

Expected final output:

```
✅ All schemas registered successfully!
✅ System ready at http://localhost:8080
```

### 3. Verify the cluster

```bash
# All 10 topics with RF=3
docker-compose exec kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --describe

# All Avro schemas registered
curl http://localhost:8081/subjects
```

### 4. Open the game UI

Navigate to `http://localhost:8080` in your browser.  
Open a second tab at `http://localhost:8080?side=shadow` for the Dark Side view.

### 5. Run unit tests (no Docker required)

```bash
make test
```

### 6. Run with race detector

```bash
make race
```

### 7. Tear down

```bash
make down
```

-----

## Makefile Targets

|Target                 |Description                                           |
|-----------------------|------------------------------------------------------|
|`make up`              |Start all services + create topics + register schemas |
|`make down`            |Stop and remove all containers and volumes            |
|`make test`            |Run `go test ./...` (no Docker needed)                |
|`make race`            |Run `go test -race ./...` (goroutine leak detection)  |
|`make setup-topics`    |Create all 10 Kafka topics (RF=3)                     |
|`make register-schemas`|Register all Avro schemas in Schema Registry          |
|`make pprof`           |Instructions for goroutine leak verification via pprof|

-----

## API Endpoints

### Game Control

|Method|Endpoint                |Body / Params        |Description                                                 |
|------|------------------------|---------------------|------------------------------------------------------------|
|`POST`|`/game/start`           |`{"mode":"HVH"}`     |Initialise a new Human vs Human game                        |
|`POST`|`/order`                |See Order Types below|Submit a validated game order                               |
|`POST`|`/turn/end`             |—                    |Manually advance to next turn (triggers 13-step processor)  |
|`GET` |`/game/state?side=light`|`side=light|shadow`  |Current world state (ring-bearer region stripped for shadow)|
|`GET` |`/health`               |—                    |`{"status":"ok","turn":N}`                                  |

### Server-Sent Events (SSE)

|Method|Endpoint |Params                         |Description                          |
|------|---------|-------------------------------|-------------------------------------|
|`GET` |`/events`|`?side=light` or `?side=shadow`|Real-time event stream for one player|

Each connecting player receives their own **dedicated channel** — events are never shared between connections.

### Analysis Dashboards

|Method|Endpoint                               |Access         |Description                                   |
|------|---------------------------------------|---------------|----------------------------------------------|
|`GET` |`/analysis/routes`                     |Light Side only|Pipeline 1: ranked route list with risk scores|
|`GET` |`/analysis/intercept`                  |Dark Side only |Pipeline 2: interception plans per Nazgul     |
|`GET` |`/orders/available?unitId=X&playerId=Y`|Both           |Legal orders for a given unit                 |

### Demo Endpoints

|Method|Endpoint                     |Description                                    |
|------|-----------------------------|-----------------------------------------------|
|`POST`|`/demo/game-over-transaction`|Trigger exactly-once GameOver (Demo Scenario 3)|
|`GET` |`/debug/pprof/goroutine`     |pprof goroutine dump (B9 verification)         |

-----

## Order Types

```bash
# Assign a route to the Ring Bearer
curl -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{"orderType":"ASSIGN_ROUTE","playerId":"light","unitId":"ring-bearer","turn":1,
       "pathIds":["shire-to-bree","bree-to-weathertop","weathertop-to-rivendell",
                  "rivendell-to-moria","moria-to-lothlorien","lothlorien-to-emyn-muil",
                  "emyn-muil-to-ithilien","ithilien-to-cirith-ungol","cirith-ungol-to-mount-doom"]}'

# Block a path (Nazgul at endpoint)
curl -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{"orderType":"BLOCK_PATH","playerId":"shadow","unitId":"witch-king","turn":1,"pathId":"bree-to-weathertop"}'

# Gandalf opens a blocked path
curl -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{"orderType":"MAIA_ABILITY","playerId":"light","unitId":"gandalf","turn":1,"pathId":"bree-to-weathertop"}'

# Destroy the Ring (win condition)
curl -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{"orderType":"DESTROY_RING","playerId":"light","unitId":"ring-bearer","turn":9}'
```

-----

## Validation Error Codes

All invalid orders are rejected synchronously and written to `game.dlq`:

|Code                       |Trigger                                               |
|---------------------------|------------------------------------------------------|
|`WRONG_TURN`               |`order.turn` does not match current turn              |
|`NOT_YOUR_UNIT`            |Unit belongs to the opposing side                     |
|`INVALID_PATH`             |Path ID does not exist in map config                  |
|`PATH_BLOCKED`             |First path in route is BLOCKED                        |
|`UNIT_NOT_ADJACENT`        |BlockPath/SearchPath: unit not at path endpoint       |
|`INVALID_TARGET`           |AttackRegion: target not adjacent or no enemy present |
|`ABILITY_ON_COOLDOWN`      |Maia ability submitted before cooldown expires        |
|`DUPLICATE_UNIT_ORDER`     |Same unit ordered twice in one turn                   |
|`MAIA_DISABLED`            |Saruman’s ability after Isengard falls to Free Peoples|
|`DESTROY_CONDITION_NOT_MET`|DestroyRing submitted when RB not at mount-doom       |

-----

## Demo Scenarios

### Scenario 1 — Information Hiding

Move Ring Bearer to `weathertop`. Move Witch-King to `bree` (2 hops, detection range 2). End turn.

- Light Side SSE: receives `RingBearerMoved` with real region
- Dark Side SSE: receives `RingBearerDetected` with region
- `GET /game/state?side=shadow` → `ring-bearer.region = ""`

### Scenario 2 — Maia Dispatch and Path Mechanics

Submit `MAIA_ABILITY` for Gandalf on a blocked path → `TEMPORARILY_OPEN`.  
Submit same `MAIA_ABILITY` order type for Saruman → `PathCorrupted`, permanent surveillance.  
After 2 turns: Gandalf’s path reverts to `BLOCKED`.

### Scenario 3 — Fault Tolerance and Exactly-Once GameOver

```bash
docker stop rotr-game-engine-2
# Observe rebalance in: docker logs rotr-kafka-1 | grep rebalance
docker start rotr-game-engine-2
# Observe recovery: docker logs rotr-game-engine-2 | grep "recovered"

# Exactly-once GameOver:
curl -X POST http://localhost:8080/demo/game-over-transaction
docker stop rotr-game-engine-1
docker-compose exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 --topic game.broadcast --from-beginning \
  | grep -c "GameOver"
# Must print: 1
```

-----

## Repository Structure

```
ring-of-the-middle-earth/
├── docker-compose.yml          3 Kafka brokers + ZooKeeper + Schema Registry + 3 Go engines
├── Makefile                    make up / make test / make register-schemas
├── README.md                   this file
├── architecture_document.pdf   system design, goroutine map, paradigm justification
├── config/
│   ├── units.conf              13 units (all behaviour config-driven, zero ID hardcoding)
│   └── map.conf                22 regions + 37 paths
├── kafka/
│   └── schemas/                13 Avro .avsc files + register_schemas.sh
├── option-b/
│   ├── go.mod
│   ├── main.go                 HTTP server + 7-case coordinator select loop
│   ├── Dockerfile
│   └── internal/
│       ├── analysis.go         Pipeline 1 (routes) + Pipeline 2 (intercept) — 4 goroutine workers each
│       ├── cache.go            WorldStateCache + timer maps
│       ├── detection.go        Detection formula + Sauron amplifier (config-driven)
│       ├── gameplay_actions.go Combat formula + Maia dispatch + path mechanics
│       ├── kafka_client.go     Producer + Consumer + RecoverStateFromSession
│       ├── processor.go        13-step turn processor + RestoreWorldStateFromSnapshot
│       ├── router.go           Topology 1 (validation) + Topology 2 (enrichment)
│       └── snapshot.go         WorldStateSnapshot publish
└── ui/
    ├── index.html
    ├── game.js
    └── style.css
```

-----

## Running Tests

```bash
# All tests (no Docker)
make test

# With race detector (required for B7 router_test.go)
make race

# Individual test files
cd option-b && go test -v -run TestCombat ./internal/
cd option-b && go test -v -run TestPipeline ./internal/
cd option-b && go test -v -run TestValidation ./internal/
cd option-b && go test -race -v -run TestStrip ./internal/

# Goroutine leak check (B9)
# Start the engine, play 10 turns, then:
curl http://localhost:8080/debug/pprof/goroutine?debug=1
```