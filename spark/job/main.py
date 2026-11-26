"""Structured Streaming: facts, session windows, and 1-minute GMV.

Three queries read shop.events. Each has its own checkpoint, because a
streaming dataframe can only be written once and the offsets of an aggregate
must not move the fact query. Offsets still live in the checkpoint, not in
a Kafka consumer group.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from aggregates import gmv_aggregation, session_aggregation
from config import Config
from listener import ShopStreamListener
from metrics_server import METRICS, start_metrics_server
from parse import parse_stream
from sinks import wait_for_sinks, write_batch, write_gmv, write_sessions


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("shop-events")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark: SparkSession, cfg: Config):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", "earliest")
        # A laptop that sleeps past retention should not kill the query.
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(cfg.max_offsets_per_trigger))
        # Do not set kafka.client.id. Local mode opens one consumer per
        # partition, and a shared id collides on the JVM's JMX registry.
        .load()
    )


def main() -> None:
    cfg = Config.from_env()
    print(
        "[shop-stream] starting topic=%s dlq=%s watermark=%s gmv_window=%s session_gap=%s"
        % (cfg.topic, cfg.dlq_topic, cfg.watermark_delay, cfg.gmv_window, cfg.session_gap),
        flush=True,
    )
    print(
        "[shop-stream] too-late events stay in the fact tables and are dropped from session and GMV windows",
        flush=True,
    )
    METRICS.help("shop_spark_input_rows_per_second", "Rows per second arriving at the query.")
    METRICS.help("shop_spark_processed_rows_per_second", "Rows per second the trigger finished.")
    METRICS.help("shop_spark_batch_duration_ms", "triggerExecution duration of the last micro-batch.")
    METRICS.help("shop_spark_watermark_dropped_total", "Late rows dropped from window state.")
    METRICS.help("shop_spark_state_rows", "Rows currently held in streaming state.")
    METRICS.help("shop_dlq_events_total", "Invalid records written to the DLQ.")
    start_metrics_server(int(os.environ.get("METRICS_PORT", "9095")))
    wait_for_sinks(cfg)
    spark = build_spark()
    spark.streams.addListener(ShopStreamListener())

    facts = parse_stream(read_kafka(spark, cfg))
    sessions = session_aggregation(parse_stream(read_kafka(spark, cfg)), cfg)
    gmv = gmv_aggregation(parse_stream(read_kafka(spark, cfg)), cfg)

    queries = [
        facts.writeStream.queryName("shop-facts")
        .foreachBatch(lambda batch_df, epoch_id: write_batch(batch_df, epoch_id, cfg))
        .option("checkpointLocation", cfg.checkpoint("facts"))
        .trigger(processingTime=cfg.trigger_interval)
        .start(),
        sessions.writeStream.queryName("shop-sessions")
        # Spark rejects update mode for session windows. Append emits a session
        # once the watermark passes its end, which is what "closed" means.
        .outputMode("append")
        .foreachBatch(lambda batch_df, epoch_id: write_sessions(batch_df, epoch_id, cfg))
        .option("checkpointLocation", cfg.checkpoint("shop-sessions"))
        .trigger(processingTime=cfg.trigger_interval)
        .start(),
        gmv.writeStream.queryName("shop-gmv")
        .outputMode("update")
        .foreachBatch(lambda batch_df, epoch_id: write_gmv(batch_df, epoch_id, cfg))
        .option("checkpointLocation", cfg.checkpoint("shop-gmv"))
        .trigger(processingTime=cfg.trigger_interval)
        .start(),
    ]
    print("[shop-stream] queries started", flush=True)
    spark.streams.awaitAnyTermination()
    failed = [query for query in queries if query.exception() is not None]
    for query in queries:
        if query.isActive:
            query.stop()
    if failed:
        for query in failed:
            print("[shop-stream] %s failed: %s" % (query.name, query.exception()), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
