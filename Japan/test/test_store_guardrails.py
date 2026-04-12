"""Japan 存储保护测试。"""

from __future__ import annotations

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

from japan_crawler.sites.bizmaps.store import BizmapsStore
from japan_crawler.sites.bizmaps.pipeline2_gmap import _clean_website
from japan_crawler.sites.bizmaps.pipeline2_gmap import _repair_dirty_gmap_websites
from japan_crawler.sites.hellowork.store import HelloworkStore


class StoreGuardrailTests(unittest.TestCase):
    def test_bizmaps_dash_representative_does_not_overwrite_real_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "bizmaps.db"
            store = BizmapsStore(db_path)
            try:
                store.upsert_companies(
                    "01",
                    [
                        {
                            "company_name": "Acme",
                            "address": "Tokyo",
                            "representative": "Jane Doe",
                        }
                    ],
                )
                store.upsert_companies(
                    "01",
                    [
                        {
                            "company_name": "Acme",
                            "address": "Tokyo",
                            "representative": "-",
                        }
                    ],
                )
                conn = sqlite3.connect(str(db_path))
                row = conn.execute(
                    "SELECT representative FROM companies WHERE company_name = 'Acme' AND address = 'Tokyo'"
                ).fetchone()
                conn.close()
                self.assertEqual("Jane Doe", row[0])
            finally:
                store._conn().close()

    def test_bizmaps_repairs_directory_like_email_sets_back_to_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "bizmaps.db"
            conn = sqlite3.connect(str(db_path))
            conn.executescript(
                """
                CREATE TABLE prefs (
                    pref_code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    total INTEGER DEFAULT 0
                );
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pref_code TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    representative TEXT DEFAULT '',
                    website TEXT DEFAULT '',
                    address TEXT DEFAULT '',
                    industry TEXT DEFAULT '',
                    phone TEXT DEFAULT '',
                    founded_year TEXT DEFAULT '',
                    capital TEXT DEFAULT '',
                    detail_url TEXT DEFAULT '',
                    emails TEXT DEFAULT '',
                    email_status TEXT DEFAULT 'done',
                    UNIQUE(company_name, address)
                );
                CREATE TABLE checkpoints (
                    pref_code TEXT PRIMARY KEY,
                    last_page INTEGER DEFAULT 0,
                    total_pages INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    last_ph TEXT DEFAULT ''
                );
                INSERT INTO companies (
                    pref_code, company_name, representative, website, address, emails, email_status
                ) VALUES (
                    '01',
                    'Acme',
                    'Jane Doe',
                    'https://example.co.jp/member-list',
                    'Tokyo',
                    'a@alpha.co.jp,b@beta.co.jp,c@gamma.co.jp,d@delta.co.jp,e@epsilon.co.jp,f@zeta.co.jp,g@eta.co.jp,h@theta.co.jp',
                    'done'
                );
                """
            )
            conn.commit()
            conn.close()

            store = BizmapsStore(db_path)
            try:
                conn = sqlite3.connect(str(db_path))
                row = conn.execute(
                    "SELECT emails, email_status FROM companies WHERE company_name = 'Acme' AND address = 'Tokyo'"
                ).fetchone()
                conn.close()
                self.assertEqual("", row[0])
                self.assertEqual("pending", row[1])
            finally:
                store._conn().close()

    def test_bizmaps_repair_dirty_gmap_websites_resets_booking_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "bizmaps.db"
            store = BizmapsStore(db_path)
            try:
                conn = sqlite3.connect(str(db_path))
                conn.execute(
                    """
                    INSERT INTO companies (
                        pref_code, company_name, representative, website, address,
                        emails, email_status, gmap_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "01",
                        "Hotel A",
                        "Jane Doe",
                        "https://Booking.com",
                        "Sapporo",
                        "stay@booking.com",
                        "done",
                        "done",
                    ),
                )
                conn.commit()
                conn.close()

                repaired = _repair_dirty_gmap_websites(store)

                conn = sqlite3.connect(str(db_path))
                row = conn.execute(
                    """
                    SELECT website, emails, email_status, gmap_status, representative
                    FROM companies
                    WHERE company_name = 'Hotel A' AND address = 'Sapporo'
                    """
                ).fetchone()
                conn.close()

                self.assertEqual(1, repaired)
                self.assertEqual("", row[0])
                self.assertEqual("", row[1])
                self.assertEqual("pending", row[2])
                self.assertEqual("pending", row[3])
                self.assertEqual("Jane Doe", row[4])
            finally:
                store._conn().close()

    def test_bizmaps_clean_website_blocks_portal_host(self) -> None:
        self.assertEqual("", _clean_website("https://Booking.com"))

    def test_bizmaps_clean_website_blocks_portal_fragment_host(self) -> None:
        self.assertEqual("", _clean_website("https://booking.comstandard"))

    def test_bizmaps_clean_website_blocks_new_portal_hosts(self) -> None:
        self.assertEqual("", _clean_website("https://getyourguide.com"))
        self.assertEqual("", _clean_website("https://www.goo-net.com/usedcar_shop/0303191/stock.html"))
        self.assertEqual("", _clean_website("https://www.carsensor.net/shop/hokkaido/329995001/"))
        self.assertEqual("", _clean_website("https://i.giatamedia.com/m.php?m=abc"))
        self.assertEqual(
            "",
            _clean_website(
                "https://dr.r-ad.ne.jp/o?__url__=https://www.carsensor.net/usedcar/detail/AU123/index.html"
            ),
        )

    def test_bizmaps_upsert_companies_skips_blocked_source_websites(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "bizmaps.db"
            store = BizmapsStore(db_path)
            try:
                store.upsert_companies(
                    "01",
                    [
                        {
                            "company_name": "Portal Co",
                            "address": "Tokyo",
                            "website": "https://getyourguide.com",
                        }
                    ],
                )
                conn = sqlite3.connect(str(db_path))
                row = conn.execute(
                    "SELECT website FROM companies WHERE company_name = 'Portal Co' AND address = 'Tokyo'"
                ).fetchone()
                conn.close()
                self.assertEqual("", row[0])
            finally:
                store._conn().close()

    def test_hellowork_dash_representative_does_not_overwrite_real_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hellowork.db"
            store = HelloworkStore(db_path)
            try:
                store.upsert_company(
                    "01",
                    {
                        "company_name": "Acme",
                        "address": "Tokyo",
                        "representative": "Jane Doe",
                        "corp_number": "123",
                    },
                )
                store.upsert_company(
                    "01",
                    {
                        "company_name": "Acme",
                        "address": "Tokyo",
                        "representative": "-",
                        "corp_number": "123",
                    },
                )
                conn = sqlite3.connect(str(db_path))
                row = conn.execute(
                    "SELECT representative FROM companies WHERE corp_number = '123'"
                ).fetchone()
                conn.close()
                self.assertEqual("Jane Doe", row[0])
            finally:
                store._conn().close()
