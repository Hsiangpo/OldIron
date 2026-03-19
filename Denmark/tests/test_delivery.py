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
    def test_day1_creates_three_column_csv(self) -> None:
        from denmark_crawler.delivery import build_delivery_bundle

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proff_dir = root / "output" / "proff"
            delivery_dir = root / "output" / "delivery"
            proff_dir.mkdir(parents=True, exist_ok=True)
            row = {
                "company_name": "Alpha ApS",
                "representative": "Jane Doe",
                "email": "hello@alpha.dk",
            }
            (proff_dir / "final_companies.jsonl").write_text(
                json.dumps(row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            summary = build_delivery_bundle(
                data_root=root / "output",
                delivery_root=delivery_dir,
                day_label="day1",
            )
            csv_path = delivery_dir / "Denmark_day001" / "companies.csv"
            self.assertEqual(1, summary["day"])
            self.assertTrue(csv_path.exists())
            header = csv_path.read_text(encoding="utf-8-sig").splitlines()[0]
            self.assertEqual("company_name,representative,email", header)

    def test_day2_only_outputs_delta(self) -> None:
        from denmark_crawler.delivery import build_delivery_bundle

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proff_dir = root / "output" / "proff"
            delivery_dir = root / "output" / "delivery"
            proff_dir.mkdir(parents=True, exist_ok=True)
            day1_rows = [
                {"company_name": "Alpha ApS", "representative": "Jane Doe", "email": "hello@alpha.dk"},
            ]
            (proff_dir / "final_companies.jsonl").write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in day1_rows) + "\n",
                encoding="utf-8",
            )
            build_delivery_bundle(
                data_root=root / "output",
                delivery_root=delivery_dir,
                day_label="day1",
            )
            day2_rows = day1_rows + [
                {"company_name": "Beta ApS", "representative": "John Doe", "email": "team@beta.dk"},
            ]
            (proff_dir / "final_companies.jsonl").write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in day2_rows) + "\n",
                encoding="utf-8",
            )
            summary = build_delivery_bundle(
                data_root=root / "output",
                delivery_root=delivery_dir,
                day_label="day2",
            )
            csv_path = delivery_dir / "Denmark_day002" / "companies.csv"
            rows = csv_path.read_text(encoding="utf-8-sig").splitlines()
            self.assertEqual(1, summary["delta_companies"])
            self.assertEqual(2, summary["total_current_companies"])
            self.assertEqual(2, len(rows))
            self.assertIn("Beta ApS,John Doe,team@beta.dk", rows[1])


if __name__ == "__main__":
    unittest.main()
