"""Structured Streaming job: Kafka -> Postgres projections + Mongo raw events.

Local mode, one process. Offsets live in the checkpoint directory, not in a
Kafka consumer group. A group-lag metric will not describe this job; M3 should
use the streaming query metrics (or start committing a group on purpose).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import Config
from parse import parse_stream
from sinks import wait_for_sinks, write_batch


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("shop-events")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    cfg = Config.from_env()
    print(
        f"[shop-stream] starting topic={cfg.topic} "
        f"bootstrap={cfg.kafka_bootstrap_servers} checkpoint={cfg.checkpoint_dir}",
        flush=True,
    )
    wait_for_sinks(cfg)
    spark = build_spark()
    raw = (
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
    parsed = parse_stream(raw)
    query = (
        parsed.writeStream.queryName("shop-events")
        .foreachBatch(lambda batch_df, epoch_id: write_batch(batch_df, epoch_id, cfg))
        .option("checkpointLocation", cfg.checkpoint_dir)
        .trigger(processingTime=cfg.trigger_interval)
        .start()
    )
    print("[shop-stream] query started", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
