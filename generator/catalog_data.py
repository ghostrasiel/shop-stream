"""Load the product catalog shared by the generator and the Mongo seed."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def default_catalog_path() -> Path:
    env = os.environ.get("CATALOG_PATH")
    if env:
        return Path(env)
    docker_path = Path("/app/catalog/products.json")
    if docker_path.exists():
        return docker_path
    return Path(__file__).resolve().parents[1] / "catalog" / "products.json"


def load_catalog(path: Path | None = None) -> list[dict[str, Any]]:
    catalog_path = path or default_catalog_path()
    raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"catalog at {catalog_path} must be a non-empty list")
    required = ("sku", "name", "category", "price", "currency", "attributes")
    for product in raw:
        missing = [key for key in required if key not in product]
        if missing:
            raise ValueError(f"product missing {missing}: {product.get('sku')}")
        if not isinstance(product["attributes"], dict):
            raise ValueError(f"attributes must be an object for {product['sku']}")
    return raw
