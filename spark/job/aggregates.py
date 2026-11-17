"""Session windows and 1-minute GMV.

Session windows only support append mode: a row is emitted when the watermark
passes the session end (last event + gap). Tumbling GMV supports update mode,
so the open minute is rewritten every trigger and the sink upserts it.

The watermark is what makes a record too late. The fact sink does not use it,
so a late payment is still stored.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import Config


def valid_events(flat: DataFrame) -> DataFrame:
    return flat.filter(F.col("reject_reason").isNull() & F.col("event_ts").isNotNull())


def session_aggregation(flat: DataFrame, cfg: Config) -> DataFrame:
    valid = valid_events(flat)
    return (
        valid.withWatermark("event_ts", cfg.watermark_delay)
        .groupBy(
            F.session_window(F.col("event_ts"), cfg.session_gap).alias("session_window"),
            F.col("user_id"),
        )
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            _count_type("page_view").alias("page_views"),
            _count_type("add_to_cart").alias("add_to_carts"),
            _count_type("order_created").alias("orders"),
            _count_type("payment_completed").alias("payments"),
            F.sum(
                F.when(F.col("event_type") == F.lit("payment_completed"), F.col("payment_amount")).otherwise(
                    F.lit(0.0)
                )
            ).alias("gmv"),
        )
    )


def gmv_aggregation(flat: DataFrame, cfg: Config) -> DataFrame:
    payments = valid_events(flat).filter(F.col("event_type") == F.lit("payment_completed"))
    return (
        payments.withWatermark("event_ts", cfg.watermark_delay)
        .groupBy(F.window(F.col("event_ts"), cfg.gmv_window).alias("bucket"))
        .agg(
            F.count(F.lit(1)).alias("payment_count"),
            F.sum(F.col("payment_amount")).alias("gmv"),
        )
    )


def _count_type(event_type: str):
    return F.sum(F.when(F.col("event_type") == F.lit(event_type), F.lit(1)).otherwise(F.lit(0)))
