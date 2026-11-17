"""Decide whether a decoded payload may enter the fact tables.

Late data is not a rejection. A payment from three minutes ago is still a
real payment; the watermark on the session and GMV queries decides whether
it can move an aggregate. This function only catches frames and documents
that do not match the contract.

The same strings are stored on the DLQ record, so the prefix before the
colon is the error class (invalid_frame, unparseable, schema_violation).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

ALLOWED_EVENT_TYPES = (
    "page_view",
    "add_to_cart",
    "order_created",
    "payment_completed",
)


def parse_event_time(value):
    # type: (object) -> datetime | None
    if not isinstance(value, str) or len(value) < 20:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def reject_reason(frame_ok, raw_json):
    # type: (object, object) -> str | None
    """Return None when the record may be upserted, otherwise a DLQ reason."""
    if not frame_ok:
        return "invalid_frame: Confluent magic byte or schema id prefix is missing"
    if not isinstance(raw_json, str) or raw_json == "":
        return "unparseable: empty payload"
    try:
        body = json.loads(raw_json)
    except json.JSONDecodeError:
        return "unparseable: payload is not JSON"
    if not isinstance(body, dict):
        return "schema_violation: JSON value is not an object"

    event_id = body.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        return "schema_violation: event_id is required"
    if body.get("event_type") not in ALLOWED_EVENT_TYPES:
        return "schema_violation: event_type is not in the contract"
    if parse_event_time(body.get("event_time")) is None:
        return "schema_violation: event_time is not ISO-8601 UTC"
    for field in ("user_id", "session_id"):
        value = body.get(field)
        if not isinstance(value, str) or not value:
            return "schema_violation: %s is required" % field

    payload = body.get("payload")
    if not isinstance(payload, dict):
        return "schema_violation: payload object is required"
    device = payload.get("device")
    if not isinstance(device, dict) or device.get("type") not in ("mobile", "desktop"):
        return "schema_violation: payload.device is required"

    event_type = body["event_type"]
    if event_type == "order_created":
        order = payload.get("order")
        if not isinstance(order, dict) or not order.get("order_id"):
            return "schema_violation: order_created requires payload.order.order_id"
        items = order.get("items") if isinstance(order, dict) else None
        if not isinstance(items, list) or not items:
            return "schema_violation: order_created requires at least one item"
    if event_type == "payment_completed":
        payment = payload.get("payment")
        if not isinstance(payment, dict) or not payment.get("payment_id"):
            return "schema_violation: payment_completed requires payload.payment.payment_id"
        if not isinstance(payment, dict) or not payment.get("order_id"):
            return "schema_violation: payment_completed requires payload.payment.order_id"
        amount = payment.get("amount") if isinstance(payment, dict) else None
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            return "schema_violation: payment_completed requires payload.payment.amount"
    return None


def error_class(reason):
    # type: (str) -> str
    return reason.split(":", 1)[0]
