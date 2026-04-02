from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from oldiron_core.delivery.engine import validate_day_sequence  # noqa: E402


class DeliveryEngineTests(unittest.TestCase):
    def test_first_delivery_must_be_day1(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "首个交付只能执行 day1"):
                validate_day_sequence(Path(tmpdir), "Japan", "day2")

    def test_only_latest_or_latest_plus_one_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            delivery_root = Path(tmpdir)
            (delivery_root / "Japan_day001").mkdir()
            (delivery_root / "Japan_day002").mkdir()
            self.assertEqual((2, 2), validate_day_sequence(delivery_root, "Japan", "day2"))
            self.assertEqual((3, 2), validate_day_sequence(delivery_root, "Japan", "day3"))
            with self.assertRaisesRegex(ValueError, "只能执行 day2（重跑）或 day3（新一天）"):
                validate_day_sequence(delivery_root, "Japan", "day4")
