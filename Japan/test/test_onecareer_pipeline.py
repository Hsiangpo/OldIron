"""OneCareer pipeline 测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.onecareer.pipeline import _fetch_company_detail


class OnecareerPipelineTests(unittest.TestCase):
    def test_fetch_company_detail_falls_back_when_detail_html_missing(self) -> None:
        class _MissingDetailClient:
            def fetch_detail_page(self, detail_url: str) -> str | None:
                _ = detail_url
                return None

        card = {
            "company_id": "89011",
            "company_name": "メディクルード",
            "address": "東京都港区",
            "industry": "コンサル・シンクタンク",
            "detail_url": "/companies/89011",
        }
        company = _fetch_company_detail(_MissingDetailClient(), card)
        self.assertEqual("89011", company["company_id"])
        self.assertEqual("メディクルード", company["company_name"])
        self.assertEqual("", company["representative"])
        self.assertEqual("", company["website"])
        self.assertEqual("東京都港区", company["address"])


if __name__ == "__main__":
    unittest.main()
