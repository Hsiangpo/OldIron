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

from japan_crawler.sites.pasonacareer.parser import (
    extract_company_id_from_url,
    parse_company_page,
    parse_company_sitemap_urls,
    parse_filter_options,
    parse_job_cards,
    parse_job_detail,
    parse_total_pages,
    parse_total_results,
)


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

RESULTS_SUMMARY_HTML = """
<html><body>
  <div class="p results-summary">該当求人数<span class="count js-hit-num">50873</span>件</div>
</body></html>
"""

DETAIL_HTML = """
<html><body>
  <script type="application/ld+json">{"@context":"https://schema.org/","@type":"JobPosting","hiringOrganization":{"@type":"Organization","name":"東急建設株式会社","sameAs":"https://www.tokyu-cnst.co.jp/"}}</script>
  <h1>東急建設株式会社 【海外駐在・管理部門※MGR】数十億～数百円規模ODA案件</h1>
  <a href="/company/search/">企業を探す</a>
  <a href="/company/80224721/">東急建設株式会社</a>
  <table>
    <tr><th><h3>本社所在地</h3></th><td>東京都 渋谷区渋谷１丁目１６－１４渋谷地下鉄ビル</td></tr>
    <tr><th><h3>企業URL</h3></th><td><a target="_blank" href="https://www.tokyu-cnst.co.jp/">https://www.tokyu-cnst.co.jp/</a></td></tr>
  </table>
</body></html>
"""

DETAIL_HTML_WITH_GENERIC_COMPANY_LINK = """
<html><body>
  <h1>機械・精密機器メーカー【インド駐在】経営企画補佐～将来的なディレクター候補～</h1>
  <a href="/company/search/">企業を探す</a>
  <a href="/company/1/">採用動画</a>
  <a href="/company/2/">企業インタビュー</a>
  <a href="/company/3/">採用企業検索</a>
</body></html>
"""

FILTER_HTML = """
<html><body>
  <input data-name="関東" data-parent-value="" data-has-children="true" data-root-value="pb200" data-is-virtual="false" type="checkbox" value="pb200" name="f[s3][]" />
  <input data-name="東京都" data-parent-value="pb200" data-has-children="true" data-root-value="pb200" data-is-virtual="false" type="checkbox" value="pm210" name="f[s3][]" />
  <input data-name="営業" data-parent-value="" data-has-children="true" data-root-value="jb100" data-is-virtual="false" type="checkbox" value="jb100" name="f[s1][]" />
</body></html>
"""

COMPANY_SITEMAP_XML = """
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.pasonacareer.jp/company/80100144/</loc></url>
  <url><loc>https://www.pasonacareer.jp/company/80204433/</loc></url>
</urlset>
"""

COMPANY_DETAIL_HTML = """
<html><body>
  <h1>株式会社三菱ＵＦＪ銀行 の中途採用・転職・求人情報</h1>
  <table>
    <tr><th>事業内容</th><td>金融業</td></tr>
    <tr><th>本社所在地</th><td>東京都 千代田区丸の内１丁目４－５</td></tr>
    <tr><th>企業URL</th><td><a href="https://www.bk.mufg.jp/">https://www.bk.mufg.jp/</a></td></tr>
  </table>
</body></html>
"""


class PasonacareerParserTests(unittest.TestCase):
    def test_parse_total_results_and_pages(self) -> None:
        self.assertEqual(50873, parse_total_results(LIST_HTML))
        self.assertEqual(998, parse_total_pages(LIST_HTML))

    def test_parse_total_results_from_summary_block(self) -> None:
        self.assertEqual(50873, parse_total_results(RESULTS_SUMMARY_HTML))

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

    def test_parse_job_detail_empty_html(self) -> None:
        detail = parse_job_detail("")
        self.assertEqual("", detail["company_name"])
        self.assertEqual("", detail["representative"])
        self.assertEqual("", detail["address"])
        self.assertEqual("", detail["website"])

    def test_parse_job_detail_ignores_generic_company_navigation(self) -> None:
        detail = parse_job_detail(DETAIL_HTML_WITH_GENERIC_COMPANY_LINK)
        self.assertEqual("", detail["company_name"])
        self.assertEqual("", detail["address"])
        self.assertEqual("", detail["website"])

    def test_parse_filter_options(self) -> None:
        area_options = parse_filter_options(FILTER_HTML, "f[s3][]")
        job_options = parse_filter_options(FILTER_HTML, "f[s1][]")
        self.assertEqual("pb200", area_options[0]["value"])
        self.assertEqual("東京都", area_options[1]["label"])
        self.assertEqual("pb200", area_options[1]["parent_value"])
        self.assertTrue(area_options[1]["has_children"])
        self.assertEqual("jb100", job_options[0]["value"])
        self.assertEqual("", job_options[0]["parent_value"])

    def test_parse_company_sitemap_urls(self) -> None:
        urls = parse_company_sitemap_urls(COMPANY_SITEMAP_XML)
        self.assertEqual(
            [
                "https://www.pasonacareer.jp/company/80100144/",
                "https://www.pasonacareer.jp/company/80204433/",
            ],
            urls,
        )

    def test_parse_company_page(self) -> None:
        detail = parse_company_page(COMPANY_DETAIL_HTML)
        self.assertEqual("株式会社三菱ＵＦＪ銀行", detail["company_name"])
        self.assertEqual("東京都 千代田区丸の内１丁目４－５", detail["address"])
        self.assertEqual("https://www.bk.mufg.jp", detail["website"])
        self.assertEqual("", detail["representative"])

    def test_extract_company_id_from_url(self) -> None:
        self.assertEqual("80204433", extract_company_id_from_url("https://www.pasonacareer.jp/company/80204433/"))
        self.assertEqual("", extract_company_id_from_url("https://www.pasonacareer.jp/search/"))


if __name__ == "__main__":
    unittest.main()
