#!/usr/bin/env bash
# Wait until M1 facts and M2 aggregates / DLQ have landed, prove natural keys
# are unique, then stop the generator and kill Spark. Counts must come back
# to the same numbers: a replayed micro-batch is absorbed by the upserts.
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
DLQ_TOPIC="${DLQ_TOPIC:-shop.events.dlq}"
TIMEOUT="${CHECK_TIMEOUT_SECONDS:-300}"

if docker info >/dev/null 2>&1; then
  compose() { docker compose "$@"; }
else
  compose() { sudo docker compose "$@"; }
fi

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

pg_count() {
  count_or_zero compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"
}

pg_exec() {
  compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"
}

mongo_count() {
  count_or_zero compose exec -T mongo mongosh --quiet --eval "db.getSiblingDB('${MONGO_DB}').${1}.countDocuments()"
}

restart_generator() {
  compose start event-generator >/dev/null 2>&1 || true
}
trap restart_generator EXIT

echo "waiting up to ${TIMEOUT}s for facts, windows, and the DLQ"
deadline=$((SECONDS + TIMEOUT))
pg_events=0
pg_orders=0
pg_items=0
pg_payments=0
pg_sessions=0
pg_gmv=0
pg_late=0
mongo_events=0
mongo_products=0
mongo_dlq=0

while (( SECONDS < deadline )); do
  pg_events="$(pg_count "SELECT count(*) FROM events")"
  pg_orders="$(pg_count "SELECT count(*) FROM orders")"
  pg_items="$(pg_count "SELECT count(*) FROM order_items")"
  pg_payments="$(pg_count "SELECT count(*) FROM payments")"
  pg_sessions="$(pg_count "SELECT count(*) FROM session_metrics")"
  pg_gmv="$(pg_count "SELECT count(*) FROM gmv_1m")"
  pg_late="$(pg_count "SELECT count(*) FROM events WHERE event_time < ingested_at - interval '2 minutes'")"
  mongo_events="$(mongo_count events_raw)"
  mongo_products="$(mongo_count products)"
  mongo_dlq="$(mongo_count events_dlq)"

  echo "events=${pg_events} orders=${pg_orders} items=${pg_items} payments=${pg_payments} sessions=${pg_sessions} gmv_1m=${pg_gmv} late=${pg_late} | mongo raw=${mongo_events} products=${mongo_products} dlq=${mongo_dlq}"

  if [[ "$pg_events" -gt 0 && "$pg_orders" -gt 0 && "$pg_items" -gt 0 && "$pg_payments" -gt 0 \
        && "$pg_sessions" -gt 0 && "$pg_gmv" -gt 0 && "$pg_late" -gt 0 \
        && "$mongo_events" -gt 0 && "$mongo_products" -gt 0 && "$mongo_dlq" -gt 0 ]]; then
    break
  fi
  sleep 5
done

if [[ "$pg_events" -le 0 || "$pg_orders" -le 0 || "$pg_payments" -le 0 || "$pg_sessions" -le 0 || "$pg_gmv" -le 0 || "$pg_late" -le 0 || "$mongo_events" -le 0 || "$mongo_dlq" -le 0 || "$mongo_products" -le 0 ]]; then
  echo "pipeline did not land M2 data before the timeout" >&2
  compose ps || true
  compose logs --tail=80 event-generator spark topic-init || true
  exit 1
fi

echo
echo "=== thresholds (from the Spark log) ==="
compose logs spark 2>/dev/null | grep "\[shop-stream\] starting topic=" | tail -n 1 || true
compose logs spark 2>/dev/null | grep "too-late events" | tail -n 1 || true

echo
echo "=== Kafka topics ==="
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic "$TOPIC"
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic "$DLQ_TOPIC"

echo
echo "=== Schema Registry ==="
compose exec -T schema-registry curl --fail --silent http://localhost:8081/subjects
echo
compose exec -T schema-registry curl --fail --silent "http://localhost:8081/subjects/${TOPIC}-value/versions"
echo

echo
echo "=== Postgres counts ==="
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT 'events' AS relation, count(*) FROM events
   UNION ALL SELECT 'orders', count(*) FROM orders
   UNION ALL SELECT 'order_items', count(*) FROM order_items
   UNION ALL SELECT 'payments', count(*) FROM payments
   UNION ALL SELECT 'session_metrics', count(*) FROM session_metrics
   UNION ALL SELECT 'gmv_1m', count(*) FROM gmv_1m
   ORDER BY 1;"

echo "=== uniqueness (1 means no duplicate natural key) ==="
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT 'events.event_id' AS key, (count(*) = count(DISTINCT event_id)) AS unique FROM events
   UNION ALL SELECT 'orders.order_id', (count(*) = count(DISTINCT order_id)) FROM orders
   UNION ALL SELECT 'payments.payment_id', (count(*) = count(DISTINCT payment_id)) FROM payments
   UNION ALL SELECT 'order_items.(order_id, sku)', (count(*) = count(DISTINCT (order_id, sku))) FROM order_items
   UNION ALL SELECT 'session_metrics', (count(*) = count(DISTINCT (user_id, session_start))) FROM session_metrics
   UNION ALL SELECT 'gmv_1m', (count(*) = count(DISTINCT (window_start, currency))) FROM gmv_1m;"

dupes="$(pg_exec "SELECT count(*) FROM (
  SELECT event_id FROM events GROUP BY 1 HAVING count(*) > 1
  UNION ALL SELECT order_id FROM orders GROUP BY 1 HAVING count(*) > 1
  UNION ALL SELECT payment_id FROM payments GROUP BY 1 HAVING count(*) > 1
  UNION ALL SELECT order_id || ':' || sku FROM order_items GROUP BY 1 HAVING count(*) > 1
) d")"
if [[ "$dupes" != "0" ]]; then
  echo "duplicate natural keys: ${dupes}" >&2
  exit 1
fi

echo "=== order totals that do not match line items (must be 0) ==="
mismatches="$(pg_exec "SELECT count(*) FROM (
  SELECT o.order_id
  FROM orders o
  JOIN order_items i ON i.order_id = o.order_id
  GROUP BY o.order_id, o.total_amount, o.item_count
  HAVING o.total_amount <> SUM(i.qty * i.unit_price) OR o.item_count <> COUNT(*)
) bad")"
echo "mismatches=${mismatches}"
if [[ "$mismatches" != "0" ]]; then
  echo "order total mismatch" >&2
  exit 1
fi

echo "=== late facts vs window GMV ==="
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT
     (SELECT count(*) FROM events WHERE event_time < ingested_at - interval '2 minutes') AS late_events,
     (SELECT COALESCE(sum(amount), 0) FROM payments) AS payment_sum,
     (SELECT COALESCE(sum(amount), 0) FROM payments WHERE event_time < ingested_at - interval '2 minutes') AS late_payment_sum,
     (SELECT COALESCE(sum(gmv), 0) FROM gmv_1m) AS gmv_window_sum;"

echo "=== session sample ==="
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT user_id, session_start, event_count, page_views, orders, payments, gmv
   FROM session_metrics ORDER BY event_count DESC LIMIT 3;"

echo "=== gmv windows ==="
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT window_start, window_end, payment_count, gmv FROM gmv_1m ORDER BY window_start;"

echo "=== Mongo DLQ ==="
compose exec -T mongo mongosh --quiet "$MONGO_DB" --eval '
print("events_dlq " + db.events_dlq.countDocuments());
print("-- by error_class --");
db.events_dlq.aggregate([
  { $group: { _id: "$error_class", n: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]).forEach(function (doc) { print(doc._id + " " + doc.n); });
print("-- one dlq document --");
printjson(db.events_dlq.findOne({}, { _id: 0 }));
'

echo
echo "=== Prometheus targets ==="
prom_port="${PROMETHEUS_PORT:-9090}"
prom_ok=0
prom_deadline=$((SECONDS + 120))
while (( SECONDS < prom_deadline )); do
  if curl -fsS "http://localhost:${prom_port}/api/v1/targets" -o /tmp/shop-prom-targets.json; then
    if python3 - <<'PY'
import json
import sys
data = json.load(open("/tmp/shop-prom-targets.json"))
need = {"prometheus", "spark", "generator", "kafka-exporter", "lag-exporter", "postgres"}
up = set()
for target in data["data"]["activeTargets"]:
    if target.get("health") == "up":
        up.add(target.get("labels", {}).get("job"))
missing = sorted(need - up)
print("up=" + ",".join(sorted(up)))
if missing:
    print("missing=" + ",".join(missing))
    sys.exit(1)
PY
    then
      prom_ok=1
      break
    fi
  fi
  echo "waiting for prometheus targets"
  sleep 5
done
if [[ "$prom_ok" -ne 1 ]]; then
  echo "prometheus targets are not all up" >&2
  exit 1
fi

echo
echo "=== dbt build, test, freshness, docs ==="
compose --profile dbt run --rm --build dbt

echo
echo "=== idempotent upsert probe ==="
pg_exec "INSERT INTO events (event_id, event_type, event_time, user_id, session_id, kafka_partition, kafka_offset)
VALUES ('idem-probe', 'page_view', now(), 'probe', 'probe', 0, -1)
ON CONFLICT (event_id) DO UPDATE SET event_type = EXCLUDED.event_type" >/dev/null
pg_exec "INSERT INTO events (event_id, event_type, event_time, user_id, session_id, kafka_partition, kafka_offset)
VALUES ('idem-probe', 'page_view', now(), 'probe', 'probe', 0, -1)
ON CONFLICT (event_id) DO UPDATE SET event_type = EXCLUDED.event_type" >/dev/null
probe="$(pg_exec "SELECT count(*) FROM events WHERE event_id = 'idem-probe'")"
pg_exec "DELETE FROM events WHERE event_id = 'idem-probe'" >/dev/null
echo "probe rows after two upserts=${probe}"
if [[ "$probe" != "1" ]]; then
  echo "upsert probe did not collapse to one row" >&2
  exit 1
fi

echo
echo "=== kill Spark and prove counts stay exact ==="
compose stop event-generator
echo "generator stopped; waiting until fact, window, and DLQ counts are stable"
stable=""
caught=0
for _ in $(seq 1 24); do
  current="$(pg_exec "SELECT count(*) FROM events")|$(pg_exec "SELECT count(*) FROM session_metrics")|$(pg_exec "SELECT count(*) FROM gmv_1m")|$(mongo_count events_dlq)"
  if [[ -n "$stable" && "$current" == "$stable" ]]; then
    caught=1
    break
  fi
  stable="$current"
  sleep 5
done
if [[ "$caught" -ne 1 ]]; then
  echo "event count did not settle before the restart" >&2
  exit 1
fi

snap_events="$(pg_exec "SELECT count(*) FROM events")"
snap_orders="$(pg_exec "SELECT count(*) FROM orders")"
snap_items="$(pg_exec "SELECT count(*) FROM order_items")"
snap_payments="$(pg_exec "SELECT count(*) FROM payments")"
snap_sessions="$(pg_exec "SELECT count(*) FROM session_metrics")"
snap_gmv="$(pg_exec "SELECT count(*) FROM gmv_1m")"
snap_raw="$(mongo_count events_raw)"
snap_dlq="$(mongo_count events_dlq)"
echo "snapshot events=${snap_events} orders=${snap_orders} items=${snap_items} payments=${snap_payments} sessions=${snap_sessions} gmv=${snap_gmv} raw=${snap_raw} dlq=${snap_dlq}"

compose kill -s KILL spark
sleep 3
if ! compose ps --status running --services | grep -qx spark; then
  compose start spark
fi

echo "waiting for Spark to replay without moving the counts"
restored=0
restore_deadline=$((SECONDS + 180))
while (( SECONDS < restore_deadline )); do
  now_events="$(pg_count "SELECT count(*) FROM events")"
  now_orders="$(pg_count "SELECT count(*) FROM orders")"
  now_items="$(pg_count "SELECT count(*) FROM order_items")"
  now_payments="$(pg_count "SELECT count(*) FROM payments")"
  now_sessions="$(pg_count "SELECT count(*) FROM session_metrics")"
  now_gmv="$(pg_count "SELECT count(*) FROM gmv_1m")"
  now_raw="$(mongo_count events_raw)"
  now_dlq="$(mongo_count events_dlq)"
  echo "after kill events=${now_events} orders=${now_orders} items=${now_items} payments=${now_payments} sessions=${now_sessions} gmv=${now_gmv} raw=${now_raw} dlq=${now_dlq}"
  if [[ "$now_events" == "$snap_events" && "$now_orders" == "$snap_orders" && "$now_items" == "$snap_items" \
        && "$now_payments" == "$snap_payments" && "$now_sessions" == "$snap_sessions" && "$now_gmv" == "$snap_gmv" \
        && "$now_raw" == "$snap_raw" && "$now_dlq" == "$snap_dlq" ]]; then
    sleep 10
    again="$(pg_exec "SELECT count(*) FROM events")"
    if [[ "$again" == "$snap_events" ]]; then
      restored=1
      break
    fi
  fi
  sleep 5
done

if [[ "$restored" -ne 1 ]]; then
  echo "counts moved after the Spark kill" >&2
  compose logs --tail=60 spark || true
  exit 1
fi

echo
echo "check ok"
