"""Upsert the product catalog into Mongo. Safe to run more than once."""

from __future__ import annotations

import os
import time
from collections import Counter

from pymongo import MongoClient

from catalog_data import load_catalog


def _client(uri: str) -> MongoClient:
    # The healthcheck can go green before another container can complete a
    # handshake. Retry the ping instead of failing the one-shot seed.
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command("ping")
            return client
        except Exception as exc:  # noqa: BLE001 - warmup retry
            last_error = exc
            client.close()
            time.sleep(2)
    raise TimeoutError(f"mongo not ready: {last_error}")


def main() -> None:
    uri = os.environ.get("MONGO_URI", "mongodb://mongo:27017/?retryWrites=false")
    db_name = os.environ.get("MONGO_DB", "shop")
    products = load_catalog()

    client = _client(uri)
    try:
        db = client[db_name]
        for product in products:
            db.products.replace_one({"sku": product["sku"]}, product, upsert=True)
        db.products.create_index("sku", unique=True)
        db.products.create_index("category")
        counts = Counter(product["category"] for product in products)
        summary = ", ".join(f"{name}={n}" for name, n in sorted(counts.items()))
        print(f"seeded {len(products)} products into {db_name}.products ({summary})", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    main()
