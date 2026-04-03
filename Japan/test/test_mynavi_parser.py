"""mynavi 解析测试。"""

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

from japan_crawler.sites.mynavi.parser import parse_detail_page, parse_has_next, parse_job_cards


LIST_HTML = """
<html><head><link rel="next" href="/shutoken/list/p13/new/pg2/"></head><body>
<section class="recruit">
  <div class="new box">
    <div class="recruit_head">
      <h2 class="recruit_title">
        <p class="main_title">株式会社セイコー社 | ★面接1回★9割以上が未経験</p>
        <p class="txt">
          <a href="/jobinfo-390057-1-5-1/" class="link entry_click entry3" target="_blank">賞与平均4ヶ月支給！福利厚生が充実！【配送・整備スタッフ】</a>
        </p>
      </h2>
    </div>
    <div class="recruit_content">
      <table class="detaile_table">
        <tr><th>勤務地</th><td>東京都大田区東糀谷5-5-8</td></tr>
      </table>
      <p class="company_data"><span>企業データ</span>設立：1979年7月／従業員数：20人／本社所在地：東京都</p>
    </div>
  </div>
</section>
</body></html>
"""

DETAIL_HTML = """
<html><head>
<link rel="canonical" href="https://tenshoku.mynavi.jp/jobinfo-159246-1-164-1/" />
<script type="application/ld+json">
{"@context":"https://schema.org/","@type":"JobPosting","hiringOrganization":{"type":"Organization","name":"株式会社Genki Global Dining Concepts","sameAs":"https://www.genki-gdc.co.jp/"}}
</script>
</head><body>
<a href="/company/159246/">株式会社Genki Global Dining Concepts</a>
<section class="company">
  <table>
    <tr><th>代表者</th><td>藤尾 益造</td></tr>
    <tr><th>本社所在地</th><td>東京都台東区上野3-24-6 上野フロンティアタワー19階</td></tr>
    <tr><th>企業ホームページ</th><td><a href="https://tenshoku.mynavi.jp/url-forwarder/?clientId=159246">https://www.genki-gdc.co.jp/</a></td></tr>
  </table>
</section>
<a href="mailto:genki-career@genkisushi.co.jp">genki-career@genkisushi.co.jp</a>
<div>電話番号 03-6924-9203（直通）</div>
</body></html>
"""


class MynaviParserTests(unittest.TestCase):
    def test_parse_job_cards(self) -> None:
        cards = parse_job_cards(LIST_HTML)
        self.assertEqual(1, len(cards))
        self.assertEqual("株式会社セイコー社", cards[0]["company_name"])
        self.assertEqual("/jobinfo-390057-1-5-1/", cards[0]["detail_url"])
        self.assertEqual("東京都大田区東糀谷5-5-8", cards[0]["address"])
        self.assertTrue(parse_has_next(LIST_HTML))

    def test_parse_job_cards_normalizes_message_variant_url(self) -> None:
        html_text = LIST_HTML.replace("/jobinfo-390057-1-5-1/", "/jobinfo-390057-1-5-1/msg/?af=foo")
        cards = parse_job_cards(html_text)
        self.assertEqual("/jobinfo-390057-1-5-1/", cards[0]["detail_url"])

    def test_parse_detail_page(self) -> None:
        detail = parse_detail_page(DETAIL_HTML)
        self.assertEqual("株式会社Genki Global Dining Concepts", detail["company_name"])
        self.assertEqual("https://www.genki-gdc.co.jp", detail["website"])
        self.assertEqual("藤尾 益造", detail["representative"])
        self.assertEqual("東京都台東区上野3-24-6 上野フロンティアタワー19階", detail["address"])
        self.assertEqual("genki-career@genkisushi.co.jp", detail["emails"])
        self.assertEqual("03-6924-9203", detail["phone"])
        self.assertEqual("https://tenshoku.mynavi.jp/jobinfo-159246-1-164-1", detail["source_job_url"])


if __name__ == "__main__":
    unittest.main()
