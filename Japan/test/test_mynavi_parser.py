"""Mynavi 解析测试。"""

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

from japan_crawler.sites.mynavi.parser import (
    parse_company_cards,
    parse_company_detail,
    parse_kana_groups,
    parse_total_pages,
    parse_total_results,
)


INDEX_HTML = """
<html><body>
  <a href="//tenshoku.mynavi.jp/company/list/na/">あ行</a>
  <a href="//tenshoku.mynavi.jp/company/list/nh/">は行</a>
  <a href="//tenshoku.mynavi.jp/company/list/na/">あ行</a>
</body></html>
"""

LIST_HTML = """
<html><body>
  <title>【「あ行」】の企業情報一覧（16302社）</title>
  <section class="card companySearchList">
    <ul>
      <li class="companySearchList__content">
        <a href="//tenshoku.mynavi.jp/company/426522/">
          <div class="navItem">
            <h2 class="companySearchList__company-name">株式会社NTTデータグループ</h2>
            <div class="companySearchList__item">
              <h3 class="companySearchList__icon-parent">〒135-6033 東京都江東区豊洲3‐3‐3豊洲センタービル</h3>
              <h3 class="companySearchList__icon-parent">インターネット関連</h3>
            </div>
          </div>
        </a>
      </li>
    </ul>
  </section>
  <a href="//tenshoku.mynavi.jp/company/list/na/pg2/">2</a>
  <a href="//tenshoku.mynavi.jp/company/list/na/pg816/">816</a>
</body></html>
"""

DETAIL_HTML = """
<html><body>
  <h1 class="headingBlock">株式会社マイナビの会社概要</h1>
  <table>
    <tr><th class="formTable__head">代表者</th><td>代表取締役 社長執行役員 粟井 俊介</td></tr>
    <tr><th class="formTable__head">本社所在地</th><td>東京都千代田区一ツ橋1-1-1 パレスサイドビル</td></tr>
    <tr><th class="formTable__head">企業ホームページ</th><td><a href="https://www.mynavi.jp/recruit/career/">https://www.mynavi.jp/recruit/career/</a></td></tr>
  </table>
</body></html>
"""


class MynaviParserTests(unittest.TestCase):
    def test_parse_kana_groups(self) -> None:
        groups = parse_kana_groups(INDEX_HTML)
        self.assertEqual(
            [
                {"group_code": "na", "group_name": "na"},
                {"group_code": "nh", "group_name": "nh"},
            ],
            groups,
        )

    def test_parse_total_results_and_pages(self) -> None:
        self.assertEqual(16302, parse_total_results(LIST_HTML))
        self.assertEqual(816, parse_total_pages(LIST_HTML))

    def test_parse_company_cards(self) -> None:
        cards = parse_company_cards(LIST_HTML)
        self.assertEqual(1, len(cards))
        self.assertEqual("426522", cards[0]["company_id"])
        self.assertEqual("株式会社NTTデータグループ", cards[0]["company_name"])
        self.assertEqual("インターネット関連", cards[0]["industry"])

    def test_parse_company_detail(self) -> None:
        detail = parse_company_detail(DETAIL_HTML)
        self.assertEqual("株式会社マイナビ", detail["company_name"])
        self.assertEqual("代表取締役 社長執行役員 粟井 俊介", detail["representative"])
        self.assertEqual("東京都千代田区一ツ橋1-1-1 パレスサイドビル", detail["address"])
        self.assertEqual("https://www.mynavi.jp/recruit/career", detail["website"])


if __name__ == "__main__":
    unittest.main()
