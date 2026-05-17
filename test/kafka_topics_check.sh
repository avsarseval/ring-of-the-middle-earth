#!/usr/bin/env bash

set -e

echo "======================================"
echo " Kafka Topic Describe Check"
echo "======================================"

TOPICS=(
  "game.orders.raw"
  "game.orders.validated"
  "game.dlq"
  "game.broadcast"
  "game.ring.position"
  "game.ring.detection"
  "game.events.unit"
  "game.events.region"
  "game.events.path"
)

for topic in "${TOPICS[@]}"; do
  echo ""
  echo "--------------------------------------"
  echo "Topic: $topic"
  echo "--------------------------------------"

  docker compose -f ../docker-compose.yml exec kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --describe \
    --topic "$topic"
done

echo ""
echo "✅ Kafka topic describe check completed."
