"""OpenWork 解析测试。"""

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

from japan_crawler.sites.openwork.parser import parse_company_cards, parse_company_detail, parse_total_pages, parse_total_results


LIST_HTML = """
<html><body>
<ul class="mt-20 borderGray-top testCompanyList">
  <li class="box-15 p-r noBorder-top">
    <div class="searchCompanyName">
      <div><h3 class="fs-18 lh-1o3 p-r"><a href="/company.php?m_id=a0C1000000s1mgI">株式会社リクルート</a></h3></div>
      <div class="f-l w-295"><p class="gray mt-5">情報サービス、リサーチ業界</p></div>
    </div>
  </li>
</ul>
<div>193,415 件中 1～50件を表示</div>
<a href="/company_list?field=&pref=&src_str=&sort=1&next_page=2">次へ</a>
<a href="/company_list?field=&pref=&src_str=&sort=1&next_page=6">6</a>
</body></html>
"""

DETAIL_HTML = """
<html><body>
<a class="noLink v-m">株式会社リクルート</a>
<table class="definitionList-wiki jsDefinitionList w-100p">
  <tr><th>業界</th><td><ul><li>情報サービス、リサーチ</li></ul></td></tr>
  <tr><th>URL</th><td><a href="https://www.recruit.co.jp/">https://www.recruit.co.jp/</a></td></tr>
  <tr><th>所在地</th><td>東京都千代田区丸の内1-9-2 グラントウキョウサウスタワー</td></tr>
  <tr><th>代表者</th><td>代表取締役社長 牛田 圭一</td></tr>
</table>
</body></html>
"""


class OpenworkParserTests(unittest.TestCase):
    def test_parse_company_cards(self) -> None:
        cards = parse_company_cards(LIST_HTML)
        self.assertEqual(1, len(cards))
        self.assertEqual("a0C1000000s1mgI", cards[0]["company_id"])
        self.assertEqual("株式会社リクルート", cards[0]["company_name"])
        self.assertEqual("情報サービス、リサーチ業界", cards[0]["industry"])

    def test_parse_total_results_and_pages(self) -> None:
        self.assertEqual(193415, parse_total_results(LIST_HTML))
        self.assertEqual(6, parse_total_pages(LIST_HTML))

    def test_parse_company_detail(self) -> None:
        detail = parse_company_detail(DETAIL_HTML)
        self.assertEqual("株式会社リクルート", detail["company_name"])
        self.assertEqual("https://www.recruit.co.jp", detail["website"])
        self.assertEqual("代表取締役社長 牛田 圭一", detail["representative"])
        self.assertEqual("東京都千代田区丸の内1-9-2 グラントウキョウサウスタワー", detail["address"])
        self.assertEqual("情報サービス、リサーチ", detail["industry"])


if __name__ == "__main__":
    unittest.main()
