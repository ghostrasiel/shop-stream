"""Kafka binary value -> one flat row per event, plus a reject reason.

Parsing stays in the streaming plan. The fact sink, the session window, and
the GMV window each read the topic on their own query; call this once per
source. Watermarks are not applied here. A fact row that is merely late is
still a fact. Window queries put the watermark on their own aggregation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from schema import EVENT_SCHEMA, FRAME_SCHEMA
from validate import reject_reason
from wire import decode_confluent_wire


def parse_stream(kafka_df: DataFrame) -> DataFrame:
    decode = F.udf(decode_confluent_wire, FRAME_SCHEMA)
    classify = F.udf(reject_reason, StringType())
    framed = kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").cast("string").alias("message_key"),
        # Kept for the DLQ when the frame itself is not JSON.
        F.substring(F.col("value").cast("string"), 1, 200).alias("raw_preview"),
        decode(F.col("value")).alias("frame"),
    )
    decoded = framed.select(
        "topic",
        "partition",
        "offset",
        "message_key",
        "raw_preview",
        F.col("frame.schema_id").alias("schema_id"),
        F.col("frame.json_payload").alias("raw_json"),
    )
    parsed = decoded.withColumn(
        "event",
        F.from_json(F.col("raw_json"), EVENT_SCHEMA, {"mode": "PERMISSIVE"}),
    )
    flat = _flatten(parsed)
    return flat.withColumn(
        "reject_reason",
        classify(F.col("raw_json").isNotNull(), F.col("raw_json")),
    )


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
    # Strip the trailing Z and parse as UTC. The session timezone is UTC.
    event_ts = F.to_timestamp(
        F.regexp_replace(event.getField("event_time"), "Z$", ""),
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
    )
    return parsed.select(
        event.getField("event_id").alias("event_id"),
        event.getField("event_type").alias("event_type"),
        event.getField("event_time").alias("event_time"),
        event_ts.alias("event_ts"),
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
        F.col("raw_preview"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("message_key"),
    )
