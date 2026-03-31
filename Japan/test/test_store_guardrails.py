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
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.bizmaps.store import BizmapsStore
from japan_crawler.sites.hellowork.store import HelloworkStore


class StoreGuardrailTests(unittest.TestCase):
    def test_bizmaps_dash_representative_does_not_overwrite_real_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "bizmaps.db"
            store = BizmapsStore(db_path)
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

    def test_hellowork_dash_representative_does_not_overwrite_real_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hellowork.db"
            store = HelloworkStore(db_path)
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
