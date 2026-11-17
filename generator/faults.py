"""Fault injection for the dead-letter path and the watermark.

Bad records bypass the JSON Schema serializer on purpose. A record that
fails validation never reaches the topic if it goes through the serializer,
so the DLQ would stay empty. Late records are valid documents with an
event_time behind the watermark; they exercise the aggregate, not the DLQ.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

BAD_KINDS = ("bare_json", "truncated_frame", "garbage_json", "bad_type")


def bad_event_bytes(kind: str) -> bytes:
    if kind == "bare_json":
        # No Confluent prefix. The first byte is '{', not the magic byte.
        return b'{"event_id":"bad-bare","event_type":"page_view"}'
    if kind == "truncated_frame":
        return b"\x00\x00\x01"
    if kind == "garbage_json":
        # Valid prefix, payload is not JSON, so the class is unparseable.
        return b"\x00" + (1).to_bytes(4, "big") + b"{not-json"
    if kind == "bad_type":
        body = {
            "event_id": "bad-type",
            "event_type": "refund_issued",
            "event_time": "2026-01-01T00:00:00.000Z",
            "user_id": "u_bad",
            "session_id": "ses_bad",
            "payload": {"device": {"type": "mobile", "os": "ios", "app_version": "0"}},
        }
        encoded = json.dumps(body).encode("utf-8")
        return b"\x00" + (1).to_bytes(4, "big") + encoded
    raise ValueError("unknown bad event kind: %s" % kind)


def with_event_time(event: dict, when: datetime) -> dict:
    stamped = dict(event)
    utc = when.astimezone(timezone.utc)
    stamped["event_time"] = utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return stamped


def late_event_time(now: datetime, lag_seconds: int) -> datetime:
    return now - timedelta(seconds=lag_seconds)
