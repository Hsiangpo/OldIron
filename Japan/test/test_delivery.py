"""Japan 国家级交付测试。"""

from __future__ import annotations

import csv
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


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

from japan_crawler.delivery import build_delivery_bundle


class JapanDeliveryTests(unittest.TestCase):
    def test_day2_outputs_country_delta_and_deduplicates_across_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            delivery_root = output_root / "delivery"

            bizmaps_dir = output_root / "bizmaps"
            bizmaps_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(bizmaps_dir / "bizmaps_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_name, representative, website, detail_url, emails) VALUES
                    ('Alpha', 'Jane', 'https://alpha.example', 'https://alpha.example/about', 'a@gmail.com');
                """
            )
            conn.commit()
            conn.close()

            xlsximport_dir = output_root / "xlsximport"
            xlsximport_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(xlsximport_dir / "xlsximport_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    website TEXT,
                    email TEXT,
                    company_name TEXT,
                    representative TEXT
                );
                INSERT INTO companies (website, email, company_name, representative) VALUES
                    ('https://alpha.example', 'alpha@gmail.com', 'Alpha', 'Jane'),
                    ('https://beta.example', 'beta@gmail.com', 'Beta', 'John');
                """
            )
            conn.commit()
            conn.close()

            day1 = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(2, day1["total_current_companies"])
            self.assertEqual(2, day1["delta_companies"])

            conn = sqlite3.connect(str(bizmaps_dir / "bizmaps_store.db"))
            conn.execute(
                "INSERT INTO companies (company_name, representative, website, detail_url, emails) VALUES (?, ?, ?, ?, ?)",
                ("Gamma", "Mary", "https://gamma.example", "https://gamma.example/about", "gamma@gmail.com"),
            )
            conn.commit()
            conn.close()

            day2 = build_delivery_bundle(output_root, delivery_root, "day2")
            self.assertEqual(3, day2["total_current_companies"])
            self.assertEqual(1, day2["delta_companies"])

            csv_path = delivery_root / "Japan_day002" / "companies.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Gamma", rows[0]["company_name"])


if __name__ == "__main__":
    unittest.main()
