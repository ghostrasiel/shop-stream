import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "observability" / "lag_exporter"))

from offsets import lag_records, latest_committed_offsets, parse_offset_log  # noqa: E402


class OffsetLogTest(unittest.TestCase):
    def test_parse_v1_log(self) -> None:
        text = '\n'.join(
            [
                "v1",
                '{"shop.events":{"0":10,"1":11,"2":4}}',
            ]
        )
        self.assertEqual(
            parse_offset_log(text),
            {("shop.events", 0): 10, ("shop.events", 1): 11, ("shop.events", 2): 4},
        )

    def test_ignores_watermark_metadata_line(self) -> None:
        text = '\n'.join(
            [
                "v2",
                '{"batchWatermarkMs":0,"batchTimestampMs":10}',
                '{"shop.events":{"2":7}}',
            ]
        )
        self.assertEqual(parse_offset_log(text), {("shop.events", 2): 7})

    def test_lag_is_never_negative(self) -> None:
        self.assertEqual(lag_records(20, 15), 5)
        self.assertEqual(lag_records(15, 20), 0)

    def test_prefers_the_offset_after_the_last_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "shop-events"
            (checkpoint / "commits").mkdir(parents=True)
            (checkpoint / "offsets").mkdir()
            (checkpoint / "commits" / "3").write_text("", encoding="utf-8")
            (checkpoint / "offsets" / "3").write_text('v1\n{"shop.events":{"0":1}}\n', encoding="utf-8")
            (checkpoint / "offsets" / "4").write_text('v1\n{"shop.events":{"0":9}}\n', encoding="utf-8")
            self.assertEqual(latest_committed_offsets(checkpoint), {("shop.events", 0): 9})


class ReconMathTest(unittest.TestCase):
    def test_diff_inside_late_gmv_is_explained(self) -> None:
        self.assertFalse(_unexplained(batch=100, streaming=80, late=30))
        self.assertFalse(_unexplained(batch=100, streaming=100, late=10))

    def test_stream_above_batch_is_not_explained(self) -> None:
        self.assertTrue(_unexplained(batch=90, streaming=100, late=20))

    def test_diff_larger_than_late_gmv_is_not_explained(self) -> None:
        self.assertTrue(_unexplained(batch=100, streaming=50, late=10))


def _unexplained(batch: float, streaming: float, late: float, tol: float = 0.01) -> bool:
    diff = batch - streaming
    return diff < -tol or diff > late + tol


if __name__ == "__main__":
    unittest.main()
