"""Japan 交付测试。"""

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
    def test_generic_site_store_is_packed_per_site(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "onecareer"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "onecareer_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    industry TEXT,
                    detail_url TEXT,
                    emails TEXT,
                    source_job_url TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, emails, source_job_url)
                VALUES ('Delta', 'Jane', 'https://delta.example', 'Tokyo', 'jane@gmail.com', 'https://example.com/job/1');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["delta_companies"])
            self.assertEqual(1, summary["sites"]["onecareer"]["qualified_current"])

            csv_path = delivery_root / "Japan_day001" / "onecareer.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Delta", rows[0]["company_name"])
            self.assertEqual("https://example.com/job/1", rows[0]["source_job_url"])

    def test_day2_outputs_only_site_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
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
                    address TEXT,
                    industry TEXT,
                    phone TEXT,
                    founded_year TEXT,
                    capital TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, emails) VALUES
                    ('Alpha', 'Jane', 'https://alpha.example', 'Tokyo', 'a@gmail.com'),
                    ('Beta', 'John', 'https://beta.example', 'Osaka', 'corp@beta.co.jp');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            day1 = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, day1["delta_companies"])
            self.assertEqual(1, day1["sites"]["bizmaps"]["qualified_current"])

            conn = sqlite3.connect(str(bizmaps_dir / "bizmaps_store.db"))
            conn.execute(
                "INSERT INTO companies (company_name, representative, website, address, emails) VALUES (?, ?, ?, ?, ?)",
                ("Gamma", "Mary", "https://gamma.example", "Nagoya", "g@gmail.com"),
            )
            conn.commit()
            conn.close()

            day2 = build_delivery_bundle(output_root, delivery_root, "day2")
            self.assertEqual(1, day2["delta_companies"])
            self.assertEqual(2, day2["sites"]["bizmaps"]["qualified_current"])

            csv_path = delivery_root / "Japan_day002" / "bizmaps.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Gamma", rows[0]["company_name"])

    def test_openwork_site_is_packaged_independently(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            openwork_dir = output_root / "openwork"
            openwork_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(openwork_dir / "openwork_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    company_id TEXT PRIMARY KEY,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    industry TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_id, company_name, representative, website, address, industry, detail_url, emails) VALUES
                    ('ow1', 'OpenWork Alpha', 'Jane', 'https://alpha.example', 'Tokyo', 'IT', 'https://www.openwork.jp/company.php?m_id=ow1', 'alpha@gmail.com'),
                    ('ow2', 'OpenWork Beta', '', 'https://beta.example', 'Osaka', 'Consulting', 'https://www.openwork.jp/company.php?m_id=ow2', 'corp@beta.co.jp');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["sites"]["openwork"]["qualified_current"])
            csv_path = delivery_root / "Japan_day001" / "openwork.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("OpenWork Alpha", rows[0]["company_name"])

    def test_generic_site_loader_picks_up_new_site_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "onecareer"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "onecareer_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    detail_url TEXT,
                    email TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, detail_url, email) VALUES
                    ('OneCareer Delta', 'Hanako', 'https://delta.example', 'Tokyo', 'https://www.onecareer.jp/companies/1', 'delta@gmail.com');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["sites"]["onecareer"]["qualified_current"])
            csv_path = delivery_root / "Japan_day001" / "onecareer.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("OneCareer Delta", rows[0]["company_name"])
