"""Publish Structured Streaming progress. Spark does not commit a consumer group.

Lag for alerts is computed outside this process, from the checkpoint versus
the Kafka end offset, so the number keeps moving after the driver is stopped.
The numbers here are the query's own progress: throughput, batch duration,
rows the watermark dropped, and state size.
"""

from __future__ import annotations

import json

from pyspark.sql.streaming import StreamingQueryListener

from metrics_server import METRICS


class ShopStreamListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:  # noqa: N802
        return

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        return

    def onQueryProgress(self, event) -> None:  # noqa: N802
        progress = _as_dict(event.progress)
        query = progress.get("name") or "unknown"
        labels = {"query": query}
        METRICS.set_gauge(
            "shop_spark_input_rows_per_second",
            float(progress.get("inputRowsPerSecond") or 0),
            **labels,
        )
        METRICS.set_gauge(
            "shop_spark_processed_rows_per_second",
            float(progress.get("processedRowsPerSecond") or 0),
            **labels,
        )
        duration = progress.get("durationMs") or {}
        METRICS.set_gauge(
            "shop_spark_batch_duration_ms",
            float(duration.get("triggerExecution") or 0),
            **labels,
        )
        dropped = 0
        state_rows = 0
        for operator in progress.get("stateOperators") or []:
            dropped += int(operator.get("numRowsDroppedByWatermark") or 0)
            state_rows += int(operator.get("numRowsTotal") or 0)
        if dropped:
            METRICS.inc("shop_spark_watermark_dropped_total", dropped, **labels)
        METRICS.set_gauge("shop_spark_state_rows", state_rows, **labels)
        METRICS.set_gauge("shop_spark_batch_id", float(progress.get("batchId") or 0), **labels)


def _as_dict(progress) -> dict:
    raw = getattr(progress, "json", None)
    if callable(raw):
        raw = raw()
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    raise TypeError("cannot read streaming progress json")
