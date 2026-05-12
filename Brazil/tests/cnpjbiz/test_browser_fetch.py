"""CNPJ Biz browser fetch 测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.browser_fetch import _build_fetch_expression
from brazil_crawler.sites.cnpjbiz.browser_fetch import _is_usable_cnpjbiz_page


class BrowserFetchTests(unittest.TestCase):
    def test_build_fetch_expression_for_get(self) -> None:
        expr = _build_fetch_expression(
            url="https://cnpj.biz/empresas/estado/SP",
            referer="https://cnpj.biz/empresas/estado/SP",
            method="GET",
            body=None,
        )
        self.assertIn("fetch(", expr)
        self.assertIn("credentials: 'include'", expr)
        self.assertIn("referrer:", expr)

    def test_build_fetch_expression_for_post(self) -> None:
        expr = _build_fetch_expression(
            url="https://cnpj.biz/api/getContactCNPJ",
            referer="https://cnpj.biz/123",
            method="POST",
            body={"data": "abc"},
        )
        self.assertIn("JSON.stringify", expr)
        self.assertIn("X-Requested-With", expr)
        self.assertIn("strict-origin-when-cross-origin", expr)
        self.assertIn("catch (error)", expr)

    def test_usable_cnpjbiz_page_accepts_generic_title_url(self) -> None:
        self.assertTrue(_is_usable_cnpjbiz_page("https://cnpj.biz/empresas/estado/SP"))
        self.assertFalse(_is_usable_cnpjbiz_page("chrome-error://chromewebdata/"))
