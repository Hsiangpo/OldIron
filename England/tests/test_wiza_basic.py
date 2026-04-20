from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from england_crawler.delivery import build_delivery_bundle
from england_crawler.sites.wiza.client import COMPANY_FILTER


class EnglandWizaTests(unittest.TestCase):
    def test_company_filter_targets_united_kingdom(self) -> None:
        self.assertEqual(COMPANY_FILTER["v"], "united kingdom")

    def test_england_websites_delivery_uses_independent_day_sequence(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            data_root = root / "output"
            delivery_root = root / "delivery"
            site_dir = data_root / "wiza"
            site_dir.mkdir(parents=True, exist_ok=True)
            (site_dir / "websites.txt").write_text(
                "https://example.co.uk\nhttps://example.co.uk\nhttps://another.co.uk\n",
                encoding="utf-8",
            )
            (delivery_root / "England_day001").mkdir(parents=True, exist_ok=True)

            summary = build_delivery_bundle(data_root, delivery_root, "day1", delivery_kind="websites")

            package_dir = delivery_root / "England_websites_day001"
            lines = (package_dir / "websites.txt").read_text(encoding="utf-8").splitlines()

        self.assertEqual(summary["day"], 1)
        self.assertEqual(summary["baseline_day"], 0)
        self.assertEqual(summary["delta_websites"], 2)
        self.assertEqual(summary["total_current_websites"], 2)
        self.assertEqual(lines, ["https://another.co.uk", "https://example.co.uk"])


if __name__ == "__main__":
    unittest.main()
