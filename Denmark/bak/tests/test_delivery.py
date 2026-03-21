from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DeliveryTests(unittest.TestCase):
    def test_day1_creates_denmark_directory(self) -> None:
        from denmark_crawler.delivery import build_delivery_bundle

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dnb_dir = root / "output" / "dnb"
            delivery_dir = root / "output" / "delivery"
            dnb_dir.mkdir(parents=True, exist_ok=True)
            row = {
                "company_name": "Alpha ApS",
                "ceo": "Jane Doe",
                "homepage": "https://alpha.dk",
                "phone": "+45 12345678",
                "emails": ["hello@alpha.dk"],
            }
            (dnb_dir / "final_companies.jsonl").write_text(
                json.dumps(row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(
                data_root=root / "output",
                delivery_root=delivery_dir,
                day_label="day1",
            )

            self.assertEqual(1, summary["day"])
            self.assertTrue((delivery_dir / "Denmark_day001" / "companies.csv").exists())


if __name__ == "__main__":
    unittest.main()
