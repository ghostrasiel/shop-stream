"""Micro-batch sinks.

Facts are upserted. A replayed micro-batch (Spark died after the database
commit and before the checkpoint) writes the same keys again and the row
count does not move. That is at-least-once delivery plus idempotent sinks,
not a transactional exactly-once spanning Postgres, Mongo, and Kafka.

Too-late events are still upserted here. The session and GMV queries are the
ones that drop them. Invalid records go to the Kafka DLQ and to Mongo
events_dlq, keyed by the source offset so a replay does not duplicate the
Mongo copy. The DLQ topic itself is append-only.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from pymongo import MongoClient, ReplaceOne, UpdateOne

from config import Config
from rows import dedupe_last_offset
from validate import error_class, parse_event_time

UPSERT_EVENTS = """
INSERT INTO events (
    event_id, event_type, event_time, user_id, session_id,
    device_type, device_os, device_app_version,
    utm_source, utm_medium, utm_campaign,
    page_url, product_sku, order_id, order_total,
    payment_id, payment_amount, payment_method,
    kafka_partition, kafka_offset, schema_id
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (event_id) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    event_time = EXCLUDED.event_time,
    user_id = EXCLUDED.user_id,
    session_id = EXCLUDED.session_id,
    device_type = EXCLUDED.device_type,
    device_os = EXCLUDED.device_os,
    device_app_version = EXCLUDED.device_app_version,
    utm_source = EXCLUDED.utm_source,
    utm_medium = EXCLUDED.utm_medium,
    utm_campaign = EXCLUDED.utm_campaign,
    page_url = EXCLUDED.page_url,
    product_sku = EXCLUDED.product_sku,
    order_id = EXCLUDED.order_id,
    order_total = EXCLUDED.order_total,
    payment_id = EXCLUDED.payment_id,
    payment_amount = EXCLUDED.payment_amount,
    payment_method = EXCLUDED.payment_method,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    schema_id = EXCLUDED.schema_id
"""

UPSERT_ORDERS = """
INSERT INTO orders (
    order_id, event_id, event_time, user_id, session_id,
    currency, item_count, total_amount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    event_time = EXCLUDED.event_time,
    user_id = EXCLUDED.user_id,
    session_id = EXCLUDED.session_id,
    currency = EXCLUDED.currency,
    item_count = EXCLUDED.item_count,
    total_amount = EXCLUDED.total_amount
"""

UPSERT_ORDER_ITEMS = """
INSERT INTO order_items (
    order_id, event_id, sku, product_name, category, qty, unit_price
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id, sku) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    qty = EXCLUDED.qty,
    unit_price = EXCLUDED.unit_price
"""

UPSERT_PAYMENTS = """
INSERT INTO payments (
    payment_id, order_id, event_id, event_time, user_id, session_id,
    amount, method, status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (payment_id) DO UPDATE SET
    order_id = EXCLUDED.order_id,
    event_id = EXCLUDED.event_id,
    event_time = EXCLUDED.event_time,
    user_id = EXCLUDED.user_id,
    session_id = EXCLUDED.session_id,
    amount = EXCLUDED.amount,
    method = EXCLUDED.method,
    status = EXCLUDED.status
"""

UPSERT_SESSIONS = """
INSERT INTO session_metrics (
    user_id, session_start, session_end, event_count, page_views,
    add_to_carts, orders, payments, gmv
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (user_id, session_start) DO UPDATE SET
    session_end = EXCLUDED.session_end,
    event_count = EXCLUDED.event_count,
    page_views = EXCLUDED.page_views,
    add_to_carts = EXCLUDED.add_to_carts,
    orders = EXCLUDED.orders,
    payments = EXCLUDED.payments,
    gmv = EXCLUDED.gmv,
    updated_at = now()
"""

UPSERT_GMV = """
INSERT INTO gmv_1m (
    window_start, window_end, currency, payment_count, gmv
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (window_start, currency) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    payment_count = EXCLUDED.payment_count,
    gmv = EXCLUDED.gmv,
    updated_at = now()
"""


def wait_for_sinks(cfg: Config, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        client = None
        try:
            with psycopg.connect(cfg.postgres_dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[cfg.mongo_db]
            db.events_raw.create_index("event_id", unique=True)
            db.events_raw.create_index("event_time")
            db.events_raw.create_index([("user_id", 1), ("event_time", 1)])
            db.events_raw.create_index("event_type")
            db.events_dlq.create_index(
                [("source_topic", 1), ("source_partition", 1), ("source_offset", 1)],
                unique=True,
            )
            db.events_dlq.create_index("error_class")
            return
        except Exception as exc:  # noqa: BLE001 - sink warmup retry
            last_error = exc
            time.sleep(2)
        finally:
            if client is not None:
                client.close()
    raise TimeoutError("postgres or mongo not ready: %s" % last_error)


def write_batch(batch_df, epoch_id: int, cfg: Config) -> None:
    rows = batch_df.collect()
    good = []
    bad = []
    for row in rows:
        if row.reject_reason:
            bad.append(row)
        elif row.event_id and row.event_type:
            good.append(row)
        else:
            bad.append(row)
    good = dedupe_last_offset(good, lambda row: row.event_id)
    bad = dedupe_last_offset(bad, lambda row: (int(row.partition), int(row.offset)))

    if good:
        _write_postgres(cfg, good)
        _write_mongo_raw(cfg, good)
    if bad:
        _write_dlq(batch_df.sparkSession, cfg, bad)

    orders = sum(1 for row in good if row.event_type == "order_created")
    payments = sum(1 for row in good if row.event_type == "payment_completed")
    print(
        "[shop-stream] facts epoch=%s events=%s orders=%s payments=%s dlq=%s"
        % (epoch_id, len(good), orders, payments, len(bad)),
        flush=True,
    )


def write_sessions(batch_df, epoch_id: int, cfg: Config) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    tuples = [_session_tuple(row) for row in rows]
    with psycopg.connect(cfg.postgres_dsn, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SESSIONS, tuples)
        conn.commit()
    print("[shop-stream] sessions epoch=%s windows=%s" % (epoch_id, len(tuples)), flush=True)


def write_gmv(batch_df, epoch_id: int, cfg: Config) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    tuples = [_gmv_tuple(row) for row in rows]
    with psycopg.connect(cfg.postgres_dsn, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_GMV, tuples)
        conn.commit()
    print("[shop-stream] gmv epoch=%s windows=%s" % (epoch_id, len(tuples)), flush=True)


def _write_postgres(cfg: Config, rows: list) -> None:
    events = [_event_tuple(row) for row in rows]
    order_rows = dedupe_last_offset(
        [row for row in rows if row.event_type == "order_created" and row.order_id],
        lambda row: row.order_id,
    )
    orders = [_order_tuple(row) for row in order_rows]
    items = dedupe_last_offset(_item_rows(order_rows), lambda row: (row[0], row[2]))
    payment_rows = dedupe_last_offset(
        [row for row in rows if row.event_type == "payment_completed" and row.payment_id and row.order_id],
        lambda row: row.payment_id,
    )
    payments = [_payment_tuple(row) for row in payment_rows]
    with psycopg.connect(cfg.postgres_dsn, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_EVENTS, events)
            if orders:
                cur.executemany(UPSERT_ORDERS, orders)
            if items:
                cur.executemany(UPSERT_ORDER_ITEMS, [row.values for row in items])
            if payments:
                cur.executemany(UPSERT_PAYMENTS, payments)
        conn.commit()


def _item_rows(order_rows):
    """Attach the parent offset so de-dupe can keep the later line."""
    enriched = []
    for row in order_rows:
        for item in _item_tuples(row):
            enriched.append(_Item(item, int(row.offset)))
    return enriched


class _Item(object):
    def __init__(self, values, offset):
        self.values = values
        self.offset = offset

    def __getitem__(self, index):
        if index == 0:
            return self.values[0]
        if index == 2:
            return self.values[2]
        raise IndexError(index)


def _write_mongo_raw(cfg: Config, rows: list) -> None:
    now = datetime.now(timezone.utc)
    ops = []
    for row in rows:
        doc = _raw_doc(row, now)
        ingested = doc.pop("ingested_at")
        ops.append(
            UpdateOne(
                {"event_id": row.event_id},
                {"$set": doc, "$setOnInsert": {"ingested_at": ingested}},
                upsert=True,
            )
        )
    client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client[cfg.mongo_db].events_raw.bulk_write(ops, ordered=False)
    finally:
        client.close()


def _write_dlq(spark, cfg: Config, rows: list) -> None:
    now = datetime.now(timezone.utc)
    docs = [_dlq_doc(row, now) for row in rows]
    client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        ops = [
            ReplaceOne(
                {
                    "source_topic": doc["source_topic"],
                    "source_partition": doc["source_partition"],
                    "source_offset": doc["source_offset"],
                },
                doc,
                upsert=True,
            )
            for doc in docs
        ]
        client[cfg.mongo_db].events_dlq.bulk_write(ops, ordered=False)
    finally:
        client.close()

    payload = [
        (
            doc.get("message_key"),
            json.dumps(doc, default=_json_default),
        )
        for doc in docs
    ]
    frame = spark.createDataFrame(payload, "key STRING, value STRING")
    (
        frame.write.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("topic", cfg.dlq_topic)
        .save()
    )


def _dlq_doc(row, now: datetime) -> dict:
    reason = row.reject_reason or "unparseable: record failed validation"
    return {
        "error_reason": reason,
        "error_class": error_class(reason),
        "source_topic": row.topic,
        "source_partition": int(row.partition),
        "source_offset": int(row.offset),
        "message_key": row.message_key,
        "schema_id": None if row.schema_id is None else int(row.schema_id),
        "raw_payload": row.raw_json or row.raw_preview,
        "failed_at": now,
    }


def _raw_doc(row, now: datetime) -> dict:
    body = json.loads(row.raw_json)
    body["_kafka"] = {
        "topic": row.topic,
        "partition": int(row.partition),
        "offset": int(row.offset),
        "key": row.message_key,
    }
    body["_schema_id"] = None if row.schema_id is None else int(row.schema_id)
    body["ingested_at"] = now
    return body


def _event_tuple(row) -> tuple:
    return (
        row.event_id,
        row.event_type,
        _as_datetime(row.event_time),
        row.user_id,
        row.session_id,
        row.device_type,
        row.device_os,
        row.device_app_version,
        row.utm_source,
        row.utm_medium,
        row.utm_campaign,
        row.page_url,
        row.product_sku,
        row.order_id,
        _money(row.order_total),
        row.payment_id,
        _money(row.payment_amount),
        row.payment_method,
        int(row.partition),
        int(row.offset),
        None if row.schema_id is None else int(row.schema_id),
    )


def _order_tuple(row) -> tuple:
    return (
        row.order_id,
        row.event_id,
        _as_datetime(row.event_time),
        row.user_id,
        row.session_id,
        row.currency or "USD",
        int(row.item_count or 0),
        _money(row.order_total) or Decimal("0.00"),
    )


def _item_tuples(row) -> list:
    if not row.order_items_json:
        return []
    parsed = json.loads(row.order_items_json)
    if not isinstance(parsed, list):
        return []
    tuples = []
    for item in parsed:
        qty = int(item.get("qty") or 0)
        sku = item.get("sku")
        name = item.get("name")
        if not sku or not name or qty < 1:
            continue
        tuples.append(
            (
                row.order_id,
                row.event_id,
                sku,
                name,
                item.get("category"),
                qty,
                _money(item.get("unit_price")) or Decimal("0.00"),
            )
        )
    return tuples


def _payment_tuple(row) -> tuple:
    return (
        row.payment_id,
        row.order_id,
        row.event_id,
        _as_datetime(row.event_time),
        row.user_id,
        row.session_id,
        _money(row.payment_amount) or Decimal("0.00"),
        row.payment_method or "card",
        row.payment_status or "captured",
    )


def _session_tuple(row) -> tuple:
    window = row.session_window
    return (
        row.user_id,
        _as_datetime(window.start),
        _as_datetime(window.end),
        int(row.event_count),
        int(row.page_views),
        int(row.add_to_carts),
        int(row.orders),
        int(row.payments),
        _money(row.gmv) or Decimal("0.00"),
    )


def _gmv_tuple(row) -> tuple:
    window = row.bucket
    return (
        _as_datetime(window.start),
        _as_datetime(window.end),
        "USD",
        int(row.payment_count),
        _money(row.gmv) or Decimal("0.00"),
    )


def _as_datetime(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    parsed = parse_event_time(value)
    if parsed is None:
        raise ValueError("unparseable event time: %r" % (value,))
    return parsed


def _money(value):
    if value is None:
        return None
    return Decimal("%.2f" % float(value))


def _json_default(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    raise TypeError(type(value))
