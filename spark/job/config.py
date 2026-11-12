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
    postgres_dsn: str
    mongo_uri: str
    mongo_db: str
    checkpoint_dir: str
    trigger_interval: str
    max_offsets_per_trigger: int

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
            postgres_dsn=dsn,
            mongo_uri=_env("MONGO_URI", "mongodb://mongo:27017/?retryWrites=false"),
            mongo_db=_env("MONGO_DB", "shop"),
            checkpoint_dir=_env("CHECKPOINT_DIR", "/checkpoints/shop-events"),
            trigger_interval=_env("TRIGGER_INTERVAL", "5 seconds"),
            max_offsets_per_trigger=int(_env("MAX_OFFSETS_PER_TRIGGER", "500")),
        )
