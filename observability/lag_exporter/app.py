"""Checkpoint lag exporter.

kafka-exporter only sees consumer groups. Structured Streaming keeps offsets
in its checkpoint, so stopping the driver does not move a group lag series.
This process keeps running, reads the frozen checkpoint, and subtracts it
from the Kafka end offset. That is the series the lag alert uses.
"""

from __future__ import annotations

import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from offsets import lag_records, latest_committed_offsets

# Directory name -> streaming query name used on the dashboard.
QUERIES = {
    "shop-events": "shop-facts",
    "shop-sessions": "shop-sessions",
    "shop-gmv": "shop-gmv",
}


def end_offsets(bootstrap: str, topic: str) -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "shop-lag-exporter",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=10)
        err = meta.topics[topic].error
        if err is not None:
            raise RuntimeError(err)
        ends = {}
        for partition in meta.topics[topic].partitions:
            _low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            ends[partition] = int(high)
        return ends
    finally:
        consumer.close()


def sample(root: Path, bootstrap: str, topic: str) -> str:
    lines = [
        "# HELP shop_spark_checkpoint_lag_records Kafka end offset minus the checkpoint resume offset.",
        "# TYPE shop_spark_checkpoint_lag_records gauge",
    ]
    try:
        ends = end_offsets(bootstrap, topic)
    except Exception as exc:  # noqa: BLE001 - keep the endpoint up while Kafka restarts
        lines.append("# kafka error: %s" % exc)
        return "\n".join(lines) + "\n"
    for directory, query in sorted(QUERIES.items()):
        committed = latest_committed_offsets(root / directory)
        for partition, end in sorted(ends.items()):
            resume = committed.get((topic, partition), 0)
            lag = lag_records(end, resume)
            lines.append(
                'shop_spark_checkpoint_lag_records{query="%s",topic="%s",partition="%s"} %s'
                % (query, topic, partition, lag)
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    root = Path(os.environ.get("CHECKPOINT_ROOT", "/checkpoints"))
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("TOPIC", "shop.events")
    port = int(os.environ.get("METRICS_PORT", "9097"))
    body = {"text": ""}
    lock = threading.Lock()

    def _refresh() -> None:
        while True:
            text = sample(root, bootstrap, topic)
            with lock:
                body["text"] = text
            time.sleep(5)

    threading.Thread(target=_refresh, name="lag", daemon=True).start()
    time.sleep(0.2)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.split("?", 1)[0] != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            with lock:
                payload = body["text"].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt, *args):
            return

    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
