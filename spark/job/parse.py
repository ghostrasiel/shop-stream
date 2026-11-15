"""Kafka binary value -> one flat row per event.

Parsing stays in the streaming plan so the Spark UI shows it. Writes do not:
foreachBatch is the M2 extension point (upsert, dead-letter, watermark are
not applied here).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from schema import EVENT_SCHEMA, FRAME_SCHEMA
from wire import decode_confluent_wire


def parse_stream(kafka_df: DataFrame) -> DataFrame:
    decode = F.udf(decode_confluent_wire, FRAME_SCHEMA)
    framed = kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").cast("string").alias("message_key"),
        decode(F.col("value")).alias("frame"),
    )
    decoded = framed.select(
        "topic",
        "partition",
        "offset",
        "message_key",
        F.col("frame.schema_id").alias("schema_id"),
        F.col("frame.json_payload").alias("raw_json"),
    )
    parsed = decoded.withColumn(
        "event",
        F.from_json(F.col("raw_json"), EVENT_SCHEMA, {"mode": "PERMISSIVE"}),
    )
    # M2: parsed.withWatermark("event_time", "10 minutes") and
    # dropDuplicates(["event_id"]) belong here, once event_time is a timestamp
    # and the sink upserts by event_id. Left out so M1 is a straight ingest.
    return _flatten(parsed)


def _flatten(parsed: DataFrame) -> DataFrame:
    event = F.col("event")
    payload = event.getField("payload")
    order_id = F.coalesce(
        payload.getField("order").getField("order_id"),
        payload.getField("payment").getField("order_id"),
    )
    item_count = F.when(
        event.getField("event_type") == F.lit("order_created"),
        F.size(payload.getField("order").getField("items")),
    ).cast("int")
    return parsed.select(
        event.getField("event_id").alias("event_id"),
        event.getField("event_type").alias("event_type"),
        event.getField("event_time").alias("event_time"),
        event.getField("user_id").alias("user_id"),
        event.getField("session_id").alias("session_id"),
        payload.getField("device").getField("type").alias("device_type"),
        payload.getField("device").getField("os").alias("device_os"),
        payload.getField("device").getField("app_version").alias("device_app_version"),
        payload.getField("utm").getField("source").alias("utm_source"),
        payload.getField("utm").getField("medium").alias("utm_medium"),
        payload.getField("utm").getField("campaign").alias("utm_campaign"),
        payload.getField("page").getField("url").alias("page_url"),
        payload.getField("product").getField("sku").alias("product_sku"),
        order_id.alias("order_id"),
        payload.getField("order").getField("currency").alias("currency"),
        item_count.alias("item_count"),
        payload.getField("order").getField("total_amount").alias("order_total"),
        F.to_json(payload.getField("order").getField("items")).alias("order_items_json"),
        payload.getField("payment").getField("payment_id").alias("payment_id"),
        payload.getField("payment").getField("amount").alias("payment_amount"),
        payload.getField("payment").getField("method").alias("payment_method"),
        payload.getField("payment").getField("status").alias("payment_status"),
        F.col("schema_id"),
        F.col("raw_json"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("message_key"),
    )
