"""OneCareer 存储测试。"""

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

from japan_crawler.sites.onecareer.store import OnecareerStore


class OnecareerStoreTests(unittest.TestCase):
    def test_mark_gmap_done_also_finishes_email_status_when_no_website(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "onecareer_store.db"
            store = OnecareerStore(db_path)
            try:
                store.upsert_companies(
                    [
                        {
                            "company_id": "84",
                            "company_name": "東京ガス",
                            "representative": "",
                            "website": "",
                            "address": "",
                            "industry": "インフラ",
                            "detail_url": "/companies/84",
                        }
                    ]
                )
                store.mark_gmap_done("84")
                conn = sqlite3.connect(db_path)
                row = conn.execute("SELECT gmap_status, email_status FROM companies WHERE company_id = '84'").fetchone()
                conn.close()
                self.assertEqual(("done", "done"), row)
            finally:
                store.close()

    def test_store_repairs_stale_pending_statuses_on_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "onecareer_store.db"
            store = OnecareerStore(db_path)
            try:
                store.upsert_companies(
                    [
                        {
                            "company_id": "84",
                            "company_name": "東京ガス",
                            "website": "",
                            "address": "",
                            "industry": "インフラ",
                            "detail_url": "/companies/84",
                        },
                        {
                            "company_id": "85",
                            "company_name": "大阪ガス",
                            "website": "https://www.osakagas.co.jp",
                            "address": "",
                            "industry": "インフラ",
                            "detail_url": "/companies/85",
                        },
                    ]
                )
                conn = sqlite3.connect(db_path)
                conn.execute("UPDATE companies SET gmap_status = 'done', email_status = 'pending' WHERE company_id = '84'")
                conn.execute("UPDATE companies SET gmap_status = 'pending', email_status = 'pending' WHERE company_id = '85'")
                conn.commit()
                conn.close()
            finally:
                store.close()
            repaired = OnecareerStore(db_path)
            try:
                conn = sqlite3.connect(db_path)
                rows = {
                    row[0]: (row[1], row[2])
                    for row in conn.execute("SELECT company_id, gmap_status, email_status FROM companies").fetchall()
                }
                conn.close()
                self.assertEqual(("done", "done"), rows["84"])
                self.assertEqual(("done", "pending"), rows["85"])
            finally:
                repaired.close()

    def test_mark_email_retry_moves_failed_company_behind_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "onecareer_store.db"
            store = OnecareerStore(db_path)
            try:
                store.upsert_companies(
                    [
                        {
                            "company_id": "84",
                            "company_name": "東京ガス",
                            "website": "https://example-a.co.jp",
                            "detail_url": "/companies/84",
                        },
                        {
                            "company_id": "85",
                            "company_name": "大阪ガス",
                            "website": "https://example-b.co.jp",
                            "detail_url": "/companies/85",
                        },
                    ]
                )
                conn = sqlite3.connect(db_path)
                conn.execute("UPDATE companies SET updated_at = '2026-01-01 00:00:00' WHERE company_id = '84'")
                conn.execute("UPDATE companies SET updated_at = '2026-01-01 00:00:01' WHERE company_id = '85'")
                conn.commit()
                conn.close()

                initial = [row["company_id"] for row in store.get_email_pending()]
                store.mark_email_retry("84")
                updated = [row["company_id"] for row in store.get_email_pending()]

                self.assertEqual(["84", "85"], initial)
                self.assertEqual(["85", "84"], updated)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
