#!/usr/bin/env bash

set -e

GAME_ID="${1:-demo-game-transaction-1}"

echo "======================================"
echo " K6 GameOver Transaction Demo"
echo "======================================"
echo "Game ID: $GAME_ID"
echo ""

echo "Calling transactional GameOver endpoint..."
curl -s -X POST "http://localhost:8080/demo/game-over-transaction?gameId=$GAME_ID" | jq

echo ""
echo "Reading committed GameOver records with isolation.level=read_committed..."
docker compose -f ../docker-compose.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic game.broadcast \
  --from-beginning \
  --timeout-ms 10000 \
  --consumer-property isolation.level=read_committed | grep "$GAME_ID" || true

echo ""
echo "✅ Expected:"
echo "- ABORTED_ENGINE_CRASH should NOT appear"
echo "- ENGINE_CRASH_COMMITTED should appear once"