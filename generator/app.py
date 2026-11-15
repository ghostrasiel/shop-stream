"""Produce shop events to Kafka with a JSON Schema registered in Schema Registry.

Record key is user_id (UTF-8). Kafka hashes the key, so every event for a user
stays on one partition, in producer order. See README "Design decisions".
"""

from __future__ import annotations

import logging
import os
import random
import signal
import threading
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from catalog_data import load_catalog
from simulator import ShopSimulator

log = logging.getLogger("generator")

SCHEMA_PATH = Path(__file__).resolve().parent / "schema" / "ecommerce_event.json"
SUBJECT_SUFFIX = "-value"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _wait_until_ready(bootstrap: str, schema_registry_url: str, topic: str, timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            # AdminClient would also work; a metadata fetch from the producer
            # config is enough to prove the broker is accepting connections.
            probe = Producer({"bootstrap.servers": bootstrap, "socket.timeout.ms": 5000})
            metadata = probe.list_topics(timeout=5)
            topic_md = metadata.topics.get(topic)
            if topic_md is None or topic_md.error is not None:
                raise RuntimeError(f"topic {topic} is not visible yet")
            SchemaRegistryClient({"url": schema_registry_url}).get_subjects()
            return
        except Exception as exc:  # noqa: BLE001 - retry until the stack is up
            last_error = exc
            time.sleep(2)
    raise TimeoutError(f"kafka or schema registry not ready: {last_error}")


def _register_schema(client: SchemaRegistryClient, subject: str, schema_str: str) -> int:
    schema = Schema(schema_str, schema_type="JSON")
    schema_id = client.register_schema(subject, schema)
    log.info("registered subject=%s schema_id=%s", subject, schema_id)
    return schema_id


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    schema_registry_url = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    topic = _env("TOPIC", "shop.events")
    rate = float(_env("EVENTS_PER_SECOND", "5"))
    if rate <= 0:
        raise SystemExit("EVENTS_PER_SECOND must be > 0")
    seed_raw = os.environ.get("SIM_SEED", "")
    rng_seed = int(seed_raw) if seed_raw else None

    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    _wait_until_ready(bootstrap, schema_registry_url, topic)

    registry = SchemaRegistryClient({"url": schema_registry_url})
    subject = f"{topic}{SUBJECT_SUFFIX}"
    schema_id = _register_schema(registry, subject, schema_str)
    serializer = JSONSerializer(
        schema_str,
        registry,
        conf={
            # Registered above. use.latest.version stamps that id into the
            # Confluent wire prefix (magic byte + schema id + JSON).
            "auto.register.schemas": False,
            "use.latest.version": True,
        },
    )
    context = SerializationContext(topic, MessageField.VALUE)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "shop-event-generator",
            # Producer idempotence only. A retried produce does not append the
            # record twice. Sink writes are still at-least-once; M2 fixes that.
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )

    catalog = load_catalog()
    simulator = ShopSimulator(
        catalog,
        rng=random.Random(rng_seed) if rng_seed is not None else None,
    )

    stop = False

    def _handle(signum: int, _frame: object) -> None:
        nonlocal stop
        log.info("signal %s, draining", signum)
        stop = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    errors = 0
    error_lock = threading.Lock()

    def _on_delivery(err: object, _msg: object) -> None:
        nonlocal errors
        if err is not None:
            with error_lock:
                errors += 1
            log.error("delivery failed: %s", err)

    produced = 0
    by_type: dict[str, int] = {}
    interval = 1.0 / rate
    next_at = time.monotonic()
    last_report = time.monotonic()
    log.info(
        "producing topic=%s key=user_id rate=%s/s schema_id=%s",
        topic,
        rate,
        schema_id,
    )

    while not stop:
        event = simulator.next_event()
        payload = serializer(event, context)
        while not stop:
            try:
                producer.produce(
                    topic=topic,
                    key=event["user_id"].encode("utf-8"),
                    value=payload,
                    headers=[("event_type", event["event_type"].encode("utf-8"))],
                    on_delivery=_on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)
        else:
            break
        producer.poll(0)
        produced += 1
        by_type[event["event_type"]] = by_type.get(event["event_type"], 0) + 1

        now = time.monotonic()
        if now - last_report >= 10:
            with error_lock:
                failed = errors
            log.info("produced=%s errors=%s by_type=%s", produced, failed, by_type)
            last_report = now

        next_at += interval
        sleep_for = next_at - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_at = time.monotonic()

    remaining = producer.flush(15)
    log.info("stopped produced=%s flush_remaining=%s", produced, remaining)


if __name__ == "__main__":
    main()
