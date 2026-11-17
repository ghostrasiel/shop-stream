"""In-batch de-duplication before an upsert.

PostgreSQL rejects an INSERT ... ON CONFLICT that touches the same key twice
in one statement. A micro-batch can contain that (a replayed record beside a
new one, or two faults that share a key). Keep the copy with the higher
Kafka offset: that is the later write on the partition.
"""

from __future__ import annotations


def dedupe_last_offset(rows, key_fn):
    chosen = {}
    for row in rows:
        key = key_fn(row)
        previous = chosen.get(key)
        if previous is None or _offset(row) >= _offset(previous):
            chosen[key] = row
    return list(chosen.values())


def _offset(row):
    value = row.offset if hasattr(row, "offset") else row["offset"]
    return int(value)
