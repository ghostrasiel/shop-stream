import random
import unittest
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generator"))

from catalog_data import load_catalog  # noqa: E402
from simulator import ShopSimulator, order_total  # noqa: E402

try:
    import jsonschema
except ImportError:  # pragma: no cover - optional, the generator image always has it
    jsonschema = None


CATALOG = load_catalog(Path(__file__).resolve().parents[1] / "catalog" / "products.json")
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "generator" / "schema" / "ecommerce_event.json"


class SimulatorTest(unittest.TestCase):
    def test_funnel_invariants(self) -> None:
        sim = ShopSimulator(CATALOG, rng=random.Random(7))
        events = [sim.next_event() for _ in range(400)]
        self.assertEqual(
            {event["event_type"] for event in events},
            {"page_view", "add_to_cart", "order_created", "payment_completed"},
        )
        self.assertEqual(len({event["event_id"] for event in events}), len(events))

        by_session: dict[str, list] = defaultdict(list)
        for event in events:
            by_session[event["session_id"]].append(event)

        for session_id, group in by_session.items():
            users = {event["user_id"] for event in group}
            self.assertEqual(len(users), 1, session_id)
            devices = {
                (
                    event["payload"]["device"]["type"],
                    event["payload"]["device"]["os"],
                )
                for event in group
            }
            self.assertEqual(len(devices), 1, session_id)
            orders = [event for event in group if event["event_type"] == "order_created"]
            payments = [event for event in group if event["event_type"] == "payment_completed"]
            self.assertLessEqual(len(orders), 1)
            self.assertLessEqual(len(payments), 1)
            if orders:
                items = orders[0]["payload"]["order"]["items"]
                self.assertGreaterEqual(len(items), 1)
                expected = order_total(items)
                self.assertEqual(orders[0]["payload"]["order"]["total_amount"], expected)
                summed = sum(
                    (Decimal(str(item["unit_price"])) * item["qty"] for item in items),
                    Decimal("0"),
                )
                self.assertEqual(Decimal(str(expected)), summed.quantize(Decimal("0.01")))
            if payments:
                self.assertEqual(len(orders), 1)
                payment = payments[0]["payload"]["payment"]
                order = orders[0]["payload"]["order"]
                self.assertEqual(payment["order_id"], order["order_id"])
                self.assertEqual(payment["amount"], order["total_amount"])
                self.assertEqual(payment["status"], "captured")

    def test_events_match_json_schema(self) -> None:
        if jsonschema is None:
            self.skipTest("jsonschema is not installed")
        import json

        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        validator = jsonschema.Draft7Validator(schema)
        sim = ShopSimulator(CATALOG, rng=random.Random(11))
        for event in (sim.next_event() for _ in range(200)):
            errors = sorted(validator.iter_errors(event), key=lambda err: list(err.path))
            self.assertEqual(errors, [], msg=f"{event['event_type']} {errors[:3]}")


if __name__ == "__main__":
    unittest.main()
