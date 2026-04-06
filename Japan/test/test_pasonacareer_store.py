"""PasonaCareer 存储测试。"""

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

from japan_crawler.sites.pasonacareer.store import PasonacareerStore


class PasonacareerStoreTests(unittest.TestCase):
    def test_mark_gmap_done_also_finishes_email_status_when_no_website(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pasonacareer_store.db"
            store = PasonacareerStore(db_path)
            try:
                store.upsert_companies(
                    [
                        {
                            "company_name": "東急建設株式会社",
                            "address": "東京都渋谷区",
                            "detail_url": "/job/81204678/",
                            "source_job_url": "/job/81204678/",
                        }
                    ]
                )
                conn = sqlite3.connect(db_path)
                row_id = conn.execute("SELECT id FROM companies WHERE company_name = '東急建設株式会社'").fetchone()[0]
                conn.close()
                store.mark_gmap_done(row_id)
                conn = sqlite3.connect(db_path)
                row = conn.execute("SELECT gmap_status, email_status FROM companies WHERE id = ?", (row_id,)).fetchone()
                conn.close()
                self.assertEqual(("done", "done"), row)
            finally:
                store.close()

    def test_store_repairs_stale_statuses_on_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pasonacareer_store.db"
            store = PasonacareerStore(db_path)
            try:
                store.upsert_companies(
                    [
                        {
                            "company_name": "東急建設株式会社",
                            "address": "東京都渋谷区",
                            "website": "",
                            "detail_url": "/job/81204678/",
                            "source_job_url": "/job/81204678/",
                        },
                        {
                            "company_name": "アビームコンサルティング株式会社",
                            "address": "東京都中央区",
                            "website": "https://www.abeam.com/jp/ja",
                            "detail_url": "/job/1/",
                            "source_job_url": "/job/1/",
                        },
                    ]
                )
                conn = sqlite3.connect(db_path)
                conn.execute(
                    "UPDATE companies SET gmap_status = 'done', email_status = 'pending' WHERE company_name = '東急建設株式会社'"
                )
                conn.execute(
                    "UPDATE companies SET gmap_status = 'pending', email_status = 'pending' WHERE company_name = 'アビームコンサルティング株式会社'"
                )
                conn.commit()
                conn.close()
            finally:
                store.close()
            repaired = PasonacareerStore(db_path)
            try:
                conn = sqlite3.connect(db_path)
                rows = {
                    row[0]: (row[1], row[2])
                    for row in conn.execute("SELECT company_name, gmap_status, email_status FROM companies").fetchall()
                }
                conn.close()
                self.assertEqual(("done", "done"), rows["東急建設株式会社"])
                self.assertEqual(("done", "pending"), rows["アビームコンサルティング株式会社"])
            finally:
                repaired.close()


if __name__ == "__main__":
    unittest.main()
