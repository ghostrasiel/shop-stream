"""Confluent Schema Registry wire format.

A value produced by JSONSerializer (default prefix serializer) is:

    byte 0      magic byte 0x00
    bytes 1-4   schema id, big-endian unsigned int
    bytes 5-    UTF-8 JSON

Magic byte 0x01 would mean a schema GUID instead of an id. M1 does not accept
that framing. The schema id is stored with the row; the reader still parses
with a fixed StructType. Fetching the writer schema by id is left for later.
"""

from __future__ import annotations


def decode_confluent_wire(value):
    # type: (bytes | bytearray | None) -> tuple[int, str] | None
    if value is None:
        return None
    raw = bytes(value)
    if len(raw) < 6 or raw[0] != 0:
        return None
    schema_id = int.from_bytes(raw[1:5], "big")
    try:
        payload = raw[5:].decode("utf-8")
    except UnicodeDecodeError:
        return None
    return (schema_id, payload)
