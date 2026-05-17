#!/usr/bin/env bash

set -e

echo "======================================"
echo " Schema Evolution Demo - V1 Consumer"
echo "======================================"
echo "This consumer reads game.orders.validated and extracts only V1 fields."
echo "Extra V2 fields such as routeRiskScore, threatenedPaths, blockedPaths are ignored."
echo ""

docker compose -f ../docker-compose.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic game.orders.validated \
  --from-beginning \
  --timeout-ms 10000 | while read -r line; do
    if [ -z "$line" ]; then
      continue
    fi

    echo "$line" | jq '{
      orderType: .orderType,
      playerId: .playerId,
      unitId: .unitId,
      turn: .turn,
      pathIds: .pathIds
    }'
  done

echo ""
echo "✅ V1 consumer completed without errors while V2 fields existed."

