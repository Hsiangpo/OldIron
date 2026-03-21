from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import product  # noqa: E402


class RootProductTests(unittest.TestCase):
    def test_main_runs_shared_builder_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            country_dir = Path(tmp) / "England"
            (country_dir / "output" / "delivery").mkdir(parents=True, exist_ok=True)
            called: dict[str, object] = {}

            def fake_builder(*, data_root: Path, delivery_root: Path, day_label: str):
                called["data_root"] = data_root
                called["delivery_root"] = delivery_root
                called["day_label"] = day_label
                return {
                    "day": 1,
                    "baseline_day": 0,
                    "delta_companies": 3,
                    "total_current_companies": 5,
                }

            with patch.object(product, "_country_root", return_value=country_dir):
                with patch.object(product, "_import_country_builder", return_value=fake_builder):
                    code = product.main(["England", "day1"])

            self.assertEqual(0, code)
            self.assertEqual(country_dir / "output", called["data_root"])
            self.assertEqual(country_dir / "output" / "delivery", called["delivery_root"])
            self.assertEqual("day1", called["day_label"])


if __name__ == "__main__":
    unittest.main()

