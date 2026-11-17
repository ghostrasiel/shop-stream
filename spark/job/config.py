"""Runtime config from the environment. Defaults match docker-compose.yml."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    topic: str
    dlq_topic: str
    postgres_dsn: str
    mongo_uri: str
    mongo_db: str
    checkpoint_dir: str
    trigger_interval: str
    max_offsets_per_trigger: int
    # Spark interval literals. See the README watermark section for the
    # default of 1 minute / 1 minute / 2 minutes and what "too late" means.
    watermark_delay: str
    gmv_window: str
    session_gap: str

    @classmethod
    def from_env(cls) -> "Config":
        user = _env("POSTGRES_USER", "shop")
        password = _env("POSTGRES_PASSWORD", "shop")
        host = _env("POSTGRES_HOST", "postgres")
        port = _env("POSTGRES_PORT", "5432")
        database = _env("POSTGRES_DB", "shop")
        dsn = f"host={host} port={port} dbname={database} user={user} password={password}"
        return cls(
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            topic=_env("TOPIC", "shop.events"),
            dlq_topic=_env("DLQ_TOPIC", "shop.events.dlq"),
            postgres_dsn=dsn,
            mongo_uri=_env("MONGO_URI", "mongodb://mongo:27017/?retryWrites=false"),
            mongo_db=_env("MONGO_DB", "shop"),
            checkpoint_dir=_env("CHECKPOINT_DIR", "/checkpoints/shop-events"),
            trigger_interval=_env("TRIGGER_INTERVAL", "5 seconds"),
            max_offsets_per_trigger=int(_env("MAX_OFFSETS_PER_TRIGGER", "500")),
            watermark_delay=_env("WATERMARK_DELAY", "1 minute"),
            gmv_window=_env("GMV_WINDOW", "1 minute"),
            session_gap=_env("SESSION_GAP", "30 seconds"),
        )

    def checkpoint(self, name: str) -> str:
        if name == "facts":
            return self.checkpoint_dir
        parent = os.path.dirname(self.checkpoint_dir.rstrip("/")) or "/checkpoints"
        return os.path.join(parent, name)
