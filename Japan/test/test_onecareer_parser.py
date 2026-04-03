"""OneCareer 解析测试。"""

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

from japan_crawler.sites.onecareer.parser import (
    parse_business_categories,
    parse_company_cards,
    parse_company_detail,
    parse_total_pages,
)


INDEX_HTML = """
<html><body>
  <a href="/companies/business_categories/5">IT・通信の企業</a>
  <a href="/companies/business_categories/8">メーカーの企業</a>
  <a href="/companies/business_categories/5">IT・通信の企業</a>
</body></html>
"""

LIST_HTML = """
<html><body>
  <ul class="v2-companies">
    <li class="v2-companies__item">
      <div class="v2-companies__business-field"><span>IT・通信</span><span>インターネット・Webサービス</span></div>
      <div class="v2-companies__title-field">
        <a class="v2-companies__title" href="/companies/70">サイバーエージェント</a>
      </div>
    </li>
  </ul>
  <a href="/companies/business_categories/5?page=2">2</a>
  <a href="/companies/business_categories/5?page=7">7</a>
  <div>1 / 7</div>
</body></html>
"""

DETAIL_HTML = """
<html><body>
  <table>
    <tr><th>会社名</th><td>X Mile株式会社</td></tr>
    <tr><th>代表者名</th><td>野呂寛之</td></tr>
    <tr><th>所在地</th><td>東京都中央区銀座7-13-6 サガミビル2F</td></tr>
    <tr><th>ホームページURL</th><td>https://www.xmile.co.jp/</td></tr>
  </table>
</body></html>
"""


class OnecareerParserTests(unittest.TestCase):
    def test_parse_business_categories(self) -> None:
        categories = parse_business_categories(INDEX_HTML)
        self.assertEqual(
            [
                {"category_id": "5", "category_name": "5"},
                {"category_id": "8", "category_name": "8"},
            ],
            categories,
        )

    def test_parse_company_cards(self) -> None:
        cards = parse_company_cards(LIST_HTML)
        self.assertEqual(1, len(cards))
        self.assertEqual("70", cards[0]["company_id"])
        self.assertEqual("サイバーエージェント", cards[0]["company_name"])
        self.assertEqual("IT・通信 / インターネット・Webサービス", cards[0]["industry"])

    def test_parse_total_pages(self) -> None:
        self.assertEqual(7, parse_total_pages(LIST_HTML))

    def test_parse_company_detail(self) -> None:
        detail = parse_company_detail(DETAIL_HTML)
        self.assertEqual("X Mile株式会社", detail["company_name"])
        self.assertEqual("野呂寛之", detail["representative"])
        self.assertEqual("東京都中央区銀座7-13-6 サガミビル2F", detail["address"])
        self.assertEqual("https://www.xmile.co.jp", detail["website"])


if __name__ == "__main__":
    unittest.main()
