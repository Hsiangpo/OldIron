"""mynavi 存储测试。"""

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

from japan_crawler.sites.mynavi.store import MynaviStore, build_company_key


class MynaviStoreTests(unittest.TestCase):
    def test_build_company_key_prefers_website_host(self) -> None:
        key = build_company_key("株式会社ABC", "https://www.abc.co.jp/about", "東京都千代田区")
        self.assertEqual("株式会社abc|abc.co.jp", key)

    def test_upsert_companies_dedupes_same_company(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MynaviStore(Path(tmpdir) / "mynavi_store.db")
            inserted_first = store.upsert_companies(
                [
                    {
                        "company_name": "株式会社ABC",
                        "website": "https://www.abc.co.jp",
                        "address": "東京都千代田区",
                        "source_job_url": "https://example.com/job/1",
                    }
                ]
            )
            inserted_second = store.upsert_companies(
                [
                    {
                        "company_name": "株式会社ABC",
                        "website": "https://www.abc.co.jp/company",
                        "address": "東京都千代田区",
                        "representative": "山田 太郎",
                        "emails": "recruit@abc.co.jp",
                        "source_job_url": "https://example.com/job/2",
                    }
                ]
            )
            conn = sqlite3.connect(str(Path(tmpdir) / "mynavi_store.db"))
            row = conn.execute(
                "SELECT COUNT(*), representative, emails, source_job_url FROM companies"
            ).fetchone()
            conn.close()
            self.assertEqual(1, inserted_first)
            self.assertEqual(0, inserted_second)
            self.assertEqual(1, row[0])
            self.assertEqual("山田 太郎", row[1])
            self.assertEqual("recruit@abc.co.jp", row[2])
            self.assertEqual("https://example.com/job/2", row[3])
