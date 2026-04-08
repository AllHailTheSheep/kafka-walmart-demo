#!/usr/bin/env bash
# Prefill inventory-ssot with an initial stock level per SKU
# Sends one compacted record per SKU keyed by productId so the SSOT topic holds the latest quantity baseline.

set -euo pipefail

# Kafka broker and docker container name (for docker exec)
BROKER="${KAFKA_BROKERS:-kafka-1:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
TOPIC="${TOPIC:-inventory-ssot}"
# Quantity to prefill per SKU (default 10k as requested)
QTY="${QTY:-2500}"
SOURCE="${SOURCE:-prefill}"
LOCATION="${LOCATION:-INIT}"

# Match the SKU list used by producers/warehouse/main.py so everything is consistent
SKUS=(
  "SKU-APPLE-001" "SKU-BANANA-002" "SKU-MILK-003" "SKU-BREAD-004"
  "SKU-EGGS-005" "SKU-RICE-006" "SKU-PASTA-007" "SKU-CHICKEN-008"
  "SKU-BEEF-009" "SKU-FISH-010" "SKU-YOGURT-011" "SKU-CHEESE-012"
  "SKU-CEREAL-013" "SKU-JUICE-014" "SKU-SODA-015" "SKU-COFFEE-016"
  "SKU-TEA-017" "SKU-SUGAR-018" "SKU-SALT-019" "SKU-FROZENPIZZA-020"
)

iso_ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

# Ensure topic exists (optional; will no-op if it already exists)
if [ "${ENSURE_TOPIC:-1}" = "1" ]; then
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --bootstrap-server "$BROKER" \
    --partitions 3 --replication-factor 2 \
    --config cleanup.policy=compact >/dev/null 2>&1 || true
fi

# Build the payload stream as KEY:VALUE lines and feed into kafka-console-producer with parse.key=true
{
  TS="$(iso_ts)"
  for SKU in "${SKUS[@]}"; do
    VALUE=$(jq -c -n \
      --arg productId "$SKU" \
      --argjson delta "$QTY" \
      --arg source "$SOURCE" \
      --arg location "$LOCATION" \
      --arg ts "$TS" \
      --arg source_id "prefill-$SKU" \
      '{productId:$productId, delta:$delta, source:$source, location:$location, timestamp:$ts, source_id:$source_id}')
    echo "$SKU:$VALUE"
  done
} | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --property parse.key=true \
  --property key.separator=:

echo "Prefill complete: sent ${#SKUS[@]} records to topic $TOPIC with quantity $QTY each."
