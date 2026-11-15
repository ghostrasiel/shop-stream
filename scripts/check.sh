#!/usr/bin/env bash
# Wait until Postgres and Mongo both have data, then print counts and a sample.
# Exit 1 if the pipeline has not landed rows before the timeout.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-shop}"
POSTGRES_DB="${POSTGRES_DB:-shop}"
MONGO_DB="${MONGO_DB:-shop}"
TOPIC="${TOPIC:-shop.events}"
TIMEOUT="${CHECK_TIMEOUT_SECONDS:-240}"

count_or_zero() {
  local result
  if result=$("$@" 2>/dev/null); then
    result="$(printf '%s\n' "$result" | tail -n 1 | tr -d '[:space:]')"
    if [[ "$result" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$result"
      return
    fi
  fi
  printf '0\n'
}

echo "waiting up to ${TIMEOUT}s for events in Postgres and Mongo"
deadline=$((SECONDS + TIMEOUT))
pg_events=0
pg_orders=0
pg_items=0
pg_payments=0
mongo_events=0
mongo_products=0

while (( SECONDS < deadline )); do
  pg_events="$(count_or_zero docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM events")"
  pg_orders="$(count_or_zero docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM orders")"
  pg_items="$(count_or_zero docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM order_items")"
  pg_payments="$(count_or_zero docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM payments")"
  mongo_events="$(count_or_zero docker compose exec -T mongo mongosh --quiet --eval "db.getSiblingDB('${MONGO_DB}').events_raw.countDocuments()")"
  mongo_products="$(count_or_zero docker compose exec -T mongo mongosh --quiet --eval "db.getSiblingDB('${MONGO_DB}').products.countDocuments()")"

  echo "postgres events=${pg_events} orders=${pg_orders} order_items=${pg_items} payments=${pg_payments} | mongo events_raw=${mongo_events} products=${mongo_products}"

  if [[ "$pg_events" -gt 0 && "$pg_orders" -gt 0 && "$pg_items" -gt 0 && "$pg_payments" -gt 0 && "$mongo_events" -gt 0 && "$mongo_products" -gt 0 ]]; then
    break
  fi
  sleep 5
done

if [[ "$pg_events" -le 0 || "$pg_orders" -le 0 || "$pg_payments" -le 0 || "$mongo_events" -le 0 || "$mongo_products" -le 0 ]]; then
  echo "pipeline did not land data before the timeout" >&2
  docker compose ps || true
  docker compose logs --tail=60 event-generator spark topic-init catalog-seed schema-registry || true
  exit 1
fi

echo
echo "=== Kafka topic ==="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic "$TOPIC"

echo
echo "=== Schema Registry ==="
docker compose exec -T schema-registry curl --fail --silent http://localhost:8081/subjects
echo
docker compose exec -T schema-registry curl --fail --silent "http://localhost:8081/subjects/${TOPIC}-value/versions"
echo

echo
echo "=== Postgres counts ==="
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT 'events' AS relation, count(*) FROM events
   UNION ALL SELECT 'orders', count(*) FROM orders
   UNION ALL SELECT 'order_items', count(*) FROM order_items
   UNION ALL SELECT 'payments', count(*) FROM payments
   ORDER BY 1;"

echo "=== Postgres by event_type ==="
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT event_type, count(*) FROM events GROUP BY event_type ORDER BY event_type;"

echo "=== Latest orders ==="
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT order_id, user_id, currency, item_count, total_amount
   FROM orders ORDER BY id DESC LIMIT 3;"

echo "=== Latest payments ==="
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT payment_id, order_id, amount, method, status
   FROM payments ORDER BY id DESC LIMIT 3;"

echo "=== Mongo ==="
docker compose exec -T mongo mongosh --quiet "$MONGO_DB" --eval '
print("events_raw " + db.events_raw.countDocuments());
print("products " + db.products.countDocuments());
print("-- attribute keys by category --");
db.products.aggregate([
  { $sort: { category: 1, sku: 1 } },
  { $project: { _id: 0, category: 1, sku: 1, keys: { $map: { input: { $objectToArray: "$attributes" }, as: "kv", in: "$$kv.k" } } } }
]).forEach(function (doc) { print(doc.category + " " + doc.sku + " [" + doc.keys.join(", ") + "]"); });
print("-- one raw event --");
const sample = db.events_raw.findOne({}, { _id: 0 });
printjson(sample);
'

echo
echo "check ok"
