"""PasonaCareer 解析测试。"""

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

from japan_crawler.sites.pasonacareer.parser import parse_job_cards, parse_job_detail, parse_total_pages, parse_total_results


LIST_HTML = """
<html><body>
  <div>検索結果一覧50873件（1～51件表示）</div>
  <article class="job-info">
    <a class="link-job-detail" href="/job/81204678/">
      <header class="job-info__header">
        <h3 class="job-info__title">
          <div class="title">【海外駐在・管理部門※MGR】数十億～数百円規模ODA案件</div>
          <div class="company"><p class="text-ommit02">東急建設株式会社</p></div>
        </h3>
      </header>
      <div class="job-info__body">
        <div class="summary">
          <dl><dt class="location">勤務地</dt><dd>東京都</dd></dl>
        </div>
      </div>
    </a>
  </article>
</body></html>
"""

DETAIL_HTML = """
<html><body>
  <h1>東急建設株式会社 【海外駐在・管理部門※MGR】数十億～数百円規模ODA案件</h1>
  <a href="/company/80224721/">東急建設株式会社</a>
  <table>
    <tr><th><h3>本社所在地</h3></th><td>東京都 渋谷区渋谷１丁目１６－１４渋谷地下鉄ビル</td></tr>
    <tr><th><h3>企業URL</h3></th><td><a target="_blank" href="https://www.tokyu-cnst.co.jp/">https://www.tokyu-cnst.co.jp/</a></td></tr>
  </table>
</body></html>
"""


class PasonacareerParserTests(unittest.TestCase):
    def test_parse_total_results_and_pages(self) -> None:
        self.assertEqual(50873, parse_total_results(LIST_HTML))
        self.assertEqual(998, parse_total_pages(LIST_HTML))

    def test_parse_job_cards(self) -> None:
        cards = parse_job_cards(LIST_HTML)
        self.assertEqual(1, len(cards))
        self.assertEqual("/job/81204678/", cards[0]["detail_url"])
        self.assertEqual("東急建設株式会社", cards[0]["company_name"])
        self.assertEqual("東京都", cards[0]["job_location"])

    def test_parse_job_detail(self) -> None:
        detail = parse_job_detail(DETAIL_HTML)
        self.assertEqual("東急建設株式会社", detail["company_name"])
        self.assertEqual("", detail["representative"])
        self.assertEqual("東京都 渋谷区渋谷１丁目１６－１４渋谷地下鉄ビル", detail["address"])
        self.assertEqual("https://www.tokyu-cnst.co.jp", detail["website"])


if __name__ == "__main__":
    unittest.main()
