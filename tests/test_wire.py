import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "job"))

from wire import decode_confluent_wire  # noqa: E402


class WireFormatTest(unittest.TestCase):
    def test_round_trip(self) -> None:
        payload = '{"event_id":"abc"}'
        framed = b"\x00" + (42).to_bytes(4, "big") + payload.encode("utf-8")
        decoded = decode_confluent_wire(framed)
        self.assertEqual(decoded, (42, payload))

    def test_rejects_guid_magic(self) -> None:
        framed = b"\x01" + b"\x00" * 16 + b"{}"
        self.assertIsNone(decode_confluent_wire(framed))

    def test_rejects_short_frame(self) -> None:
        self.assertIsNone(decode_confluent_wire(b"\x00\x00\x01"))
        self.assertIsNone(decode_confluent_wire(None))

    def test_rejects_bad_utf8(self) -> None:
        framed = b"\x00" + (1).to_bytes(4, "big") + b"\xff"
        self.assertIsNone(decode_confluent_wire(framed))


if __name__ == "__main__":
    unittest.main()
