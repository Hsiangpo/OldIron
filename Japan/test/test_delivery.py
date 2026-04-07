"""Japan 交付测试。"""

from __future__ import annotations

import csv
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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
                VALUES ('Delta', 'Jane', 'https://delta.example', 'Tokyo', 'hr@delta.example', 'https://example.com/job/1');
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
            self.assertEqual(2, day1["delta_companies"])
            self.assertEqual(2, day1["sites"]["bizmaps"]["qualified_current"])

            conn = sqlite3.connect(str(bizmaps_dir / "bizmaps_store.db"))
            conn.execute(
                "INSERT INTO companies (company_name, representative, website, address, emails) VALUES (?, ?, ?, ?, ?)",
                ("Gamma", "Mary", "https://gamma.example", "Nagoya", "g@gmail.com"),
            )
            conn.commit()
            conn.close()

            day2 = build_delivery_bundle(output_root, delivery_root, "day2")
            self.assertEqual(1, day2["delta_companies"])
            self.assertEqual(3, day2["sites"]["bizmaps"]["qualified_current"])

            csv_path = delivery_root / "Japan_day002" / "bizmaps.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Gamma", rows[0]["company_name"])

    def test_non_xlsximport_delivery_filters_fake_emails_and_merges_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "bizmaps"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "bizmaps_store.db"))
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
                INSERT INTO companies (company_name, representative, website, address, emails, detail_url) VALUES
                    ('Alpha', 'Jane', 'https://alpha.example', 'Tokyo', 'info@example.jp; sales@alpha.co.jp', 'https://example.com/a'),
                    ('Alpha', 'Jane', 'https://alpha.example', 'Tokyo', 'sales@alpha.co.jp; support@alpha.co.jp', '');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["delta_companies"])
            self.assertEqual(1, summary["sites"]["bizmaps"]["qualified_current"])

            csv_path = delivery_root / "Japan_day001" / "bizmaps.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("sales@alpha.co.jp; support@alpha.co.jp", rows[0]["emails"])
            self.assertEqual("https://example.com/a", rows[0]["detail_url"])

    def test_strict_sites_drop_unrelated_email_domains(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "onecareer"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "onecareer_store.db"))
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
                    ('oc-1', 'AIGグループ', '代表者A', 'http://www-154.aig.com', 'Tokyo', '金融', '/companies/317', 'aviationworklist@aig.com; paul.smith@talbotuw.com'),
                    ('oc-2', 'J.P.モルガン', '代表者B', 'https://toushin-plaza.jp', 'Tokyo', '金融', '/companies/104', 'info@fan-sec.co.jp');
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
            self.assertEqual("aviationworklist@aig.com", rows[0]["emails"])

    def test_delivery_merges_same_company_when_domain_and_representative_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "mynavi"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "mynavi_store.db"))
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
                    source_job_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_id, company_name, representative, website, address, industry, detail_url, source_job_url, emails) VALUES
                    ('324993', 'コストコホールセールジャパン株式会社', '代表取締役社長 ケン・テリオ', 'https://www.costco.co.jp', '木更津瓜倉361番地金田西2街区2各地', '流通', '/company/324993', '/job/1', 'w5180opt@costco.co.jp'),
                    ('387502', 'コストコホールセールジャパン株式会社', '代表執行役社長 ケン・テリオ', 'https://www.costco.co.jp/company', '千葉県木更津市瓜倉361番地', '流通', '/company/387502', '/job/2', 'w5181opt@costco.co.jp');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["delta_companies"])
            self.assertEqual(1, summary["sites"]["mynavi"]["qualified_current"])

            csv_path = delivery_root / "Japan_day001" / "mynavi.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual(
                "w5180opt@costco.co.jp; w5181opt@costco.co.jp",
                rows[0]["emails"],
            )

    def test_xlsximport_delivery_keeps_source_emails_but_still_dedupes_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "xlsximport"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "xlsximport_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, detail_url, emails) VALUES
                    ('Alpha', 'Jane', 'https://alpha.example', '', 'https://example.com/a', 'alpha@gmail.jp'),
                    ('Alpha', 'Jane', 'https://alpha.example', '', 'https://example.com/a', 'alpha@gmail.jp');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            summary = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, summary["delta_companies"])
            self.assertEqual(1, summary["sites"]["xlsximport"]["qualified_current"])

            csv_path = delivery_root / "Japan_day001" / "xlsximport.csv"
            with csv_path.open(encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("alpha@gmail.jp", rows[0]["emails"])

    def test_same_record_with_empty_address_is_not_redelivered_on_next_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "xlsximport"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "xlsximport_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, detail_url, emails)
                VALUES ('Alpha', 'Jane', 'https://alpha.example', '', 'https://example.com/a', 'jane@gmail.com');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            day1 = build_delivery_bundle(output_root, delivery_root, "day1")
            self.assertEqual(1, day1["delta_companies"])

            day2 = build_delivery_bundle(output_root, delivery_root, "day2")
            self.assertEqual(0, day2["delta_companies"])
            self.assertEqual(1, day2["sites"]["xlsximport"]["qualified_current"])
            self.assertEqual(["xlsximport"], day2["skipped_sites_no_delta"])
            self.assertFalse((delivery_root / "Japan_day002" / "xlsximport.csv").exists())
            self.assertFalse((delivery_root / "Japan_day002" / "xlsximport.keys.txt").exists())

    def test_old_spaced_keys_are_normalized_before_baseline_compare(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            site_dir = output_root / "xlsximport"
            site_dir.mkdir(parents=True)
            conn = sqlite3.connect(str(site_dir / "xlsximport_store.db"))
            conn.executescript(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    address TEXT,
                    detail_url TEXT,
                    emails TEXT
                );
                INSERT INTO companies (company_name, representative, website, address, detail_url, emails)
                VALUES ('Alpha', 'Jane', 'https://alpha.example', '', 'https://example.com/a', 'jane@gmail.com');
                """
            )
            conn.commit()
            conn.close()

            delivery_root = output_root / "delivery"
            day2_dir = delivery_root / "Japan_day002"
            day2_dir.mkdir(parents=True)
            (day2_dir / "xlsximport.keys.txt").write_text(
                "alpha | jane | https://alpha.example | \n",
                encoding="utf-8",
            )

            day3 = build_delivery_bundle(output_root, delivery_root, "day3")
            self.assertEqual(0, day3["delta_companies"])
            self.assertEqual(1, day3["sites"]["xlsximport"]["qualified_current"])
            self.assertEqual(["xlsximport"], day3["skipped_sites_no_delta"])
            self.assertFalse((delivery_root / "Japan_day003" / "xlsximport.csv").exists())
            self.assertFalse((delivery_root / "Japan_day003" / "xlsximport.keys.txt").exists())

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
                    ('OneCareer Delta', 'Hanako', 'https://delta.example', 'Tokyo', 'https://www.onecareer.jp/companies/1', 'recruit@delta.example');
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

    def test_site_filter_only_writes_owned_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            onecareer_dir = output_root / "onecareer"
            onecareer_dir.mkdir(parents=True)
            openwork_dir = output_root / "openwork"
            openwork_dir.mkdir(parents=True)
            self._seed_company_db(
                onecareer_dir / "onecareer_store.db",
                "onecareer-a",
                "OneCareer A",
                "Hanako",
                "https://onecareer.example",
                "hr@onecareer.example",
            )
            self._seed_company_db(
                openwork_dir / "openwork_store.db",
                "openwork-a",
                "OpenWork A",
                "Taro",
                "https://openwork.example",
                "openwork@gmail.com",
            )

            delivery_root = output_root / "delivery"
            with patch.dict(os.environ, {"JAPAN_DELIVERY_SITES": "onecareer"}, clear=False):
                summary = build_delivery_bundle(output_root, delivery_root, "day1")

            self.assertEqual({"onecareer": {"qualified_current": 1, "delta": 1}}, summary["sites"])
            self.assertTrue((delivery_root / "Japan_day001" / "onecareer.csv").exists())
            self.assertFalse((delivery_root / "Japan_day001" / "openwork.csv").exists())

    def test_same_day_rerun_preserves_copied_remote_site_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            onecareer_dir = output_root / "onecareer"
            onecareer_dir.mkdir(parents=True)
            self._seed_company_db(
                onecareer_dir / "onecareer_store.db",
                "onecareer-a",
                "OneCareer A",
                "Hanako",
                "https://onecareer.example",
                "hr@onecareer.example",
            )

            delivery_root = output_root / "delivery"
            with patch.dict(os.environ, {"JAPAN_DELIVERY_SITES": "onecareer"}, clear=False):
                build_delivery_bundle(output_root, delivery_root, "day1")

            day_dir = delivery_root / "Japan_day001"
            self._write_remote_site_artifacts(day_dir, "openwork", "OpenWork Remote", "remote-key")

            with patch.dict(os.environ, {"JAPAN_DELIVERY_SITES": "onecareer"}, clear=False):
                summary = build_delivery_bundle(output_root, delivery_root, "day1")

            self.assertEqual(2, summary["delta_companies"])
            self.assertEqual(2, summary["total_current_companies"])
            self.assertIn("onecareer", summary["sites"])
            self.assertIn("openwork", summary["sites"])
            self.assertTrue((day_dir / "openwork.csv").exists())
            self.assertTrue((day_dir / "openwork.keys.txt").exists())

    def test_summary_only_rebuilds_from_existing_site_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_root = root / "output"
            delivery_root = output_root / "delivery"
            day_dir = delivery_root / "Japan_day001"
            day_dir.mkdir(parents=True)
            self._write_remote_site_artifacts(day_dir, "openwork", "OpenWork Remote", "openwork-key")
            self._write_remote_site_artifacts(day_dir, "onecareer", "OneCareer Remote", "onecareer-key")

            with patch.dict(os.environ, {"JAPAN_DELIVERY_SUMMARY_ONLY": "1"}, clear=False):
                summary = build_delivery_bundle(output_root, delivery_root, "day1")

            self.assertEqual(2, summary["delta_companies"])
            self.assertEqual(2, summary["total_current_companies"])
            self.assertEqual({"onecareer", "openwork"}, set(summary["sites"]))

    def _seed_company_db(
        self,
        db_path: Path,
        company_id: str,
        company_name: str,
        representative: str,
        website: str,
        emails: str,
    ) -> None:
        conn = sqlite3.connect(str(db_path))
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
            """
        )
        conn.execute(
            """
            INSERT INTO companies (company_id, company_name, representative, website, address, industry, detail_url, emails)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                company_name,
                representative,
                website,
                "Tokyo",
                "IT",
                f"https://example.com/{company_id}",
                emails,
            ),
        )
        conn.commit()
        conn.close()

    def _write_remote_site_artifacts(self, day_dir: Path, site_name: str, company_name: str, key_value: str) -> None:
        csv_path = day_dir / f"{site_name}.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=[
                    "company_name",
                    "representative",
                    "website",
                    "emails",
                    "phone",
                    "address",
                    "industry",
                    "founded_year",
                    "capital",
                    "detail_url",
                    "source_job_url",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "company_name": company_name,
                    "representative": "Remote Rep",
                    "website": "https://remote.example",
                    "emails": "remote@gmail.com",
                    "phone": "",
                    "address": "Tokyo",
                    "industry": "IT",
                    "founded_year": "",
                    "capital": "",
                    "detail_url": "https://example.com/remote",
                    "source_job_url": "",
                }
            )
        (day_dir / f"{site_name}.keys.txt").write_text(f"{key_value}\n", encoding="utf-8")
