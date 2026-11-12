"""Micro-batch sinks.

Postgres gets typed projections. Mongo gets the raw JSON document plus the
Kafka coordinates. Both writes happen in one foreachBatch call. The checkpoint
advances only if this function returns, so a raised error retries the batch.

That is at-least-once. A crash after Postgres commits and before the checkpoint
is written inserts the same rows again. event_id is therefore not unique yet.
M2 changes insert_events / insert_orders / insert_payments to upserts and adds
the unique constraints. Do that in these functions; the streaming plan stays
the same.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from pymongo import MongoClient

from config import Config

INSERT_EVENTS = """
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
"""

INSERT_ORDERS = """
INSERT INTO orders (
    order_id, event_id, event_time, user_id, session_id,
    currency, item_count, total_amount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_ORDER_ITEMS = """
INSERT INTO order_items (
    order_id, event_id, sku, product_name, category, qty, unit_price
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

INSERT_PAYMENTS = """
INSERT INTO payments (
    payment_id, order_id, event_id, event_time, user_id, session_id,
    amount, method, status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def wait_for_sinks(cfg: Config, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        client = None
        try:
            with psycopg.connect(cfg.postgres_dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[cfg.mongo_db]
            # Not unique. M2 adds a unique index on event_id with the upsert.
            db.events_raw.create_index("event_id")
            db.events_raw.create_index("event_time")
            db.events_raw.create_index([("user_id", 1), ("event_time", 1)])
            db.events_raw.create_index("event_type")
            return
        except Exception as exc:  # noqa: BLE001 - sink warmup retry
            last_error = exc
            time.sleep(2)
        finally:
            if client is not None:
                client.close()
    raise TimeoutError(f"postgres or mongo not ready: {last_error}")


def write_batch(batch_df, epoch_id: int, cfg: Config) -> None:
    rows = batch_df.collect()
    good = []
    bad = 0
    for row in rows:
        if not row.event_id or not row.event_type or not row.raw_json:
            bad += 1
            continue
        good.append(row)

    if good:
        _write_postgres(cfg, good)
        _write_mongo(cfg, good)

    orders = sum(1 for row in good if row.event_type == "order_created")
    payments = sum(1 for row in good if row.event_type == "payment_completed")
    print(
        f"[shop-stream] epoch={epoch_id} events={len(good)} "
        f"orders={orders} payments={payments} bad={bad}",
        flush=True,
    )
    if bad:
        # M2: write these to shop.events.dlq and then Mongo, in this function,
        # before returning. Returning here commits the offset without them.
        print(
            f"[shop-stream] dropped {bad} unparseable records this epoch",
            flush=True,
        )


def _write_postgres(cfg: Config, rows: list) -> None:
    events = [_event_tuple(row) for row in rows]
    orders = [_order_tuple(row) for row in rows if row.event_type == "order_created" and row.order_id]
    items = []
    for row in rows:
        if row.event_type != "order_created" or not row.order_id:
            continue
        items.extend(_item_tuples(row))
    payments = [
        _payment_tuple(row)
        for row in rows
        if row.event_type == "payment_completed" and row.payment_id and row.order_id
    ]
    with psycopg.connect(cfg.postgres_dsn, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_EVENTS, events)
            if orders:
                cur.executemany(INSERT_ORDERS, orders)
            if items:
                cur.executemany(INSERT_ORDER_ITEMS, items)
            if payments:
                cur.executemany(INSERT_PAYMENTS, payments)
        conn.commit()


def _write_mongo(cfg: Config, rows: list) -> None:
    docs = [_raw_doc(row) for row in rows]
    client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client[cfg.mongo_db].events_raw.insert_many(docs, ordered=True)
    finally:
        client.close()


def _event_tuple(row) -> tuple:
    return (
        row.event_id,
        row.event_type,
        _event_time(row.event_time),
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
        _event_time(row.event_time),
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
        _event_time(row.event_time),
        row.user_id,
        row.session_id,
        _money(row.payment_amount) or Decimal("0.00"),
        row.payment_method or "card",
        row.payment_status or "captured",
    )


def _raw_doc(row) -> dict:
    body = json.loads(row.raw_json)
    body["_kafka"] = {
        "topic": row.topic,
        "partition": int(row.partition),
        "offset": int(row.offset),
        "key": row.message_key,
    }
    body["_schema_id"] = None if row.schema_id is None else int(row.schema_id)
    body["ingested_at"] = datetime.now(timezone.utc)
    return body


def _event_time(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(text)


def _money(value) -> Decimal | None:
    if value is None:
        return None
    return Decimal(f"{float(value):.2f}")
