"""bizmaps 分页保护测试。"""

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

from japan_crawler.sites.bizmaps.parser import parse_current_page
from japan_crawler.sites.bizmaps.pipeline import _crawl_remaining_pages


PAGE_ONE_HTML = """
<html><body>
  <ul class="pagination">
    <li class="page-item current active">1</li>
    <li class="page-item link">
      <a href="https://biz-maps.com/s/prefs/01?ph=%242y%2405%24abc&page=2">2</a>
    </li>
  </ul>
</body></html>
"""


LAST_PAGE_HTML = """
<html><body>
  <div class="pagination__wrap"><div class="pagination__num">159801 ~ 159809件 / 159809件</div></div>
  <ul class="pagination">
    <li class="page-item prev"><a href="https://biz-maps.com/s/prefs/01?ph=%242y%2405%24prev&page=7990">‹</a></li>
    <li class="page-item link"><a href="https://biz-maps.com/s/prefs/01?ph=%242y%2405%24page1&page=1">1</a></li>
    <li class="page-item current active">7991</li>
  </ul>
</body></html>
"""


BROKEN_CHAIN_HTML = """
<html><body>
  <ul class="pagination">
    <li class="page-item prev"><a href="https://biz-maps.com/s/prefs/01?ph=%242y%2405%24prev&page=5787">‹</a></li>
    <li class="page-item current active">5788</li>
  </ul>
</body></html>
"""


class _FakeClient:
    def __init__(self, html: str) -> None:
        self._html = html

    def fetch_list_page(self, pref_code: str, page: int, ph: str = "") -> str:
        return self._html


class _FakeStore:
    def __init__(self) -> None:
        self.checkpoints: list[tuple[str, int, int, str, str]] = []

    def update_checkpoint(
        self,
        pref_code: str,
        last_page: int,
        total_pages: int,
        status: str = "running",
        last_ph: str = "",
    ) -> None:
        self.checkpoints.append((pref_code, last_page, total_pages, status, last_ph))

    def upsert_companies(self, pref_code: str, companies: list[dict[str, str]]) -> int:
        return len(companies)


class BizmapsPaginationTest(unittest.TestCase):
    def test_parse_current_page_supports_first_page(self) -> None:
        self.assertEqual(parse_current_page(PAGE_ONE_HTML), 1)

    def test_parse_current_page_supports_deep_page(self) -> None:
        self.assertEqual(parse_current_page(LAST_PAGE_HTML), 7991)

    def test_crawl_remaining_pages_stops_when_server_falls_back_to_page_one(self) -> None:
        store = _FakeStore()
        result = _crawl_remaining_pages(
            client=_FakeClient(PAGE_ONE_HTML),
            store=store,
            pref_code="01",
            pref_name="北海道",
            start_page=5788,
            total_pages=7991,
            initial_ph="$2y$05$bad-token",
        )

        self.assertEqual(result, {"new": 0, "completed": False})
        self.assertEqual(store.checkpoints[-1], ("01", 5787, 7991, "error", ""))

    def test_crawl_remaining_pages_stops_when_next_ph_missing(self) -> None:
        store = _FakeStore()
        result = _crawl_remaining_pages(
            client=_FakeClient(BROKEN_CHAIN_HTML),
            store=store,
            pref_code="01",
            pref_name="北海道",
            start_page=5788,
            total_pages=7991,
            initial_ph="$2y$05$good-token",
        )

        self.assertEqual(result, {"new": 0, "completed": False})
        self.assertEqual(store.checkpoints[-1], ("01", 5788, 7991, "error", ""))
