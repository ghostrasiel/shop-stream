import json
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "job"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generator"))

from faults import BAD_KINDS, bad_event_bytes, late_event_time, with_event_time  # noqa: E402
from rows import dedupe_last_offset  # noqa: E402
from validate import reject_reason  # noqa: E402
from wire import decode_confluent_wire  # noqa: E402

from datetime import datetime, timezone


def _valid_page() -> dict:
    return {
        "event_id": "e1",
        "event_type": "page_view",
        "event_time": "2026-11-16T00:00:00.000Z",
        "user_id": "u_0001",
        "session_id": "ses_1",
        "payload": {"device": {"type": "mobile", "os": "ios", "app_version": "1"}},
    }


class RejectReasonTest(unittest.TestCase):
    def test_valid_page_view_is_kept(self) -> None:
        self.assertIsNone(reject_reason(True, json.dumps(_valid_page())))

    def test_missing_frame_is_invalid(self) -> None:
        reason = reject_reason(False, None)
        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("invalid_frame:"))

    def test_garbage_json_is_unparseable(self) -> None:
        reason = reject_reason(True, "{not-json")
        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("unparseable:"))

    def test_unknown_event_type_is_schema_violation(self) -> None:
        body = _valid_page()
        body["event_type"] = "refund_issued"
        reason = reject_reason(True, json.dumps(body))
        self.assertIsNotNone(reason)
        self.assertIn("event_type", reason)
        self.assertTrue(reason.startswith("schema_violation:"))

    def test_order_without_items_is_rejected(self) -> None:
        body = _valid_page()
        body["event_type"] = "order_created"
        body["payload"]["order"] = {"order_id": "ord_1", "items": []}
        self.assertIsNotNone(reject_reason(True, json.dumps(body)))

    def test_payment_without_amount_is_rejected(self) -> None:
        body = _valid_page()
        body["event_type"] = "payment_completed"
        body["payload"]["payment"] = {"payment_id": "pay_1", "order_id": "ord_1", "method": "card", "status": "captured"}
        self.assertIsNotNone(reject_reason(True, json.dumps(body)))


class FaultInjectionTest(unittest.TestCase):
    def test_each_bad_kind_is_rejected(self) -> None:
        self.assertEqual(len(BAD_KINDS), 4)
        for kind in BAD_KINDS:
            raw = bad_event_bytes(kind)
            decoded = decode_confluent_wire(raw)
            if decoded is None:
                reason = reject_reason(False, None)
            else:
                _schema_id, payload = decoded
                reason = reject_reason(True, payload)
            self.assertIsNotNone(reason, kind)
            self.assertFalse(reason.startswith("unparseable:") and kind == "bad_type")

    def test_garbage_frame_is_unparseable(self) -> None:
        _schema_id, payload = decode_confluent_wire(bad_event_bytes("garbage_json"))
        reason = reject_reason(True, payload)
        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("unparseable:"))

    def test_bad_type_names_the_contract(self) -> None:
        _schema_id, payload = decode_confluent_wire(bad_event_bytes("bad_type"))
        reason = reject_reason(True, payload)
        self.assertTrue(reason.startswith("schema_violation:"))

    def test_late_stamp_is_behind_now(self) -> None:
        now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
        stamped = with_event_time({"event_id": "e"}, late_event_time(now, 180))
        self.assertEqual(stamped["event_time"], "2026-11-16T11:57:00.000Z")


class DedupeTest(unittest.TestCase):
    def test_keeps_higher_offset(self) -> None:
        rows = [
            {"event_id": "e", "offset": 1, "n": "old"},
            {"event_id": "e", "offset": 4, "n": "new"},
            {"event_id": "other", "offset": 2, "n": "keep"},
        ]
        kept = dedupe_last_offset(rows, lambda row: row["event_id"])
        by_id = {row["event_id"]: row["n"] for row in kept}
        self.assertEqual(by_id, {"e": "new", "other": "keep"})


if __name__ == "__main__":
    unittest.main()
