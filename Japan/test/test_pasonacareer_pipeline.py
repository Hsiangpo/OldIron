"""PasonaCareer pipeline 测试。"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.pasonacareer.pipeline import _fetch_search_page_with_retries
from japan_crawler.sites.pasonacareer.pipeline import _fetch_company_sitemap_with_retries
from japan_crawler.sites.pasonacareer.pipeline import _fetch_job_detail
from japan_crawler.sites.pasonacareer.pipeline import _plan_search_scopes
from japan_crawler.sites.pasonacareer.pipeline import _resolve_start_page
from japan_crawler.sites.pasonacareer.pipeline import run_pipeline_list
from japan_crawler.sites.pasonacareer.client import PasonacareerClient


class PasonacareerPipelineTests(unittest.TestCase):
    def test_client_fetch_company_sitemap_prefers_requests_path(self) -> None:
        client = PasonacareerClient(request_delay=0.0, proxy="http://127.0.0.1:7897")
        response = mock.Mock(status_code=200, text="<urlset></urlset>")
        with (
            mock.patch("japan_crawler.sites.pasonacareer.client.requests.get", return_value=response) as get_mock,
            mock.patch.object(client, "_get_with_retry", side_effect=AssertionError("不该回退到 curl 路径")),
        ):
            xml_text = client.fetch_company_sitemap()
        self.assertEqual("<urlset></urlset>", xml_text)
        get_mock.assert_called_once()

    def test_fetch_company_sitemap_with_retries_eventually_succeeds(self) -> None:
        class _Client:
            def __init__(self) -> None:
                self.calls = 0

            def fetch_company_sitemap(self) -> str | None:
                self.calls += 1
                return "<urlset></urlset>" if self.calls >= 2 else None

        client = _Client()
        from japan_crawler.sites.pasonacareer import pipeline as target

        original_sleep = target.time.sleep
        target.time.sleep = lambda seconds: None  # type: ignore[assignment]
        try:
            xml_text = _fetch_company_sitemap_with_retries(client)
        finally:
            target.time.sleep = original_sleep  # type: ignore[assignment]
        self.assertEqual("<urlset></urlset>", xml_text)
        self.assertEqual(2, client.calls)

    def test_fetch_search_page_with_retries_eventually_succeeds(self) -> None:
        class _Client:
            def __init__(self) -> None:
                self.calls = 0

            def fetch_search_page(self, page: int, *, filters=None) -> str | None:
                _ = page
                _ = filters
                self.calls += 1
                return "<html>ok</html>" if self.calls >= 3 else None

        client = _Client()
        from japan_crawler.sites.pasonacareer import pipeline as target

        original_sleep = target.time.sleep
        target.time.sleep = lambda seconds: None  # type: ignore[assignment]
        try:
            html = _fetch_search_page_with_retries(client, 197, {})
        finally:
            target.time.sleep = original_sleep  # type: ignore[assignment]
        self.assertEqual("<html>ok</html>", html)
        self.assertEqual(3, client.calls)

    def test_resolve_start_page_resumes_stale_done_checkpoint(self) -> None:
        self.assertEqual(
            197,
            _resolve_start_page({"last_page": 196, "total_pages": 997, "status": "done"}, discovered_total_pages=997),
        )

    def test_resolve_start_page_skips_true_done_checkpoint(self) -> None:
        self.assertIsNone(
            _resolve_start_page({"last_page": 997, "total_pages": 997, "status": "done"}, discovered_total_pages=997)
        )

    def test_plan_search_scopes_splits_large_tokyo_scope_by_job_root(self) -> None:
        base_html = """
        <html><body>
          <div class="p results-summary">該当求人数<span class="count js-hit-num">50812</span>件</div>
          <input data-name="関東" data-parent-value="" data-has-children="true" data-root-value="pb200" data-is-virtual="false" type="checkbox" value="pb200" name="f[s3][]" />
          <input data-name="東京都" data-parent-value="pb200" data-has-children="true" data-root-value="pb200" data-is-virtual="false" type="checkbox" value="pm210" name="f[s3][]" />
          <input data-name="千葉県" data-parent-value="pb200" data-has-children="false" data-root-value="pb200" data-is-virtual="false" type="checkbox" value="pm230" name="f[s3][]" />
          <input data-name="営業" data-parent-value="" data-has-children="true" data-root-value="jb100" data-is-virtual="false" type="checkbox" value="jb100" name="f[s1][]" />
          <input data-name="管理・事務" data-parent-value="" data-has-children="true" data-root-value="jb200" data-is-virtual="false" type="checkbox" value="jb200" name="f[s1][]" />
        </body></html>
        """

        class _Client:
            def fetch_search_page(self, page: int, *, filters=None) -> str | None:
                _ = page
                filters = filters or {}
                location = filters.get("f[s3][]", "")
                job = filters.get("f[s1][]", "")
                if location == "pm230":
                    return '<html><body><div class="p results-summary">該当求人数<span class="count js-hit-num">580</span>件</div></body></html>'
                if location == "pm210" and not job:
                    return '<html><body><div class="p results-summary">該当求人数<span class="count js-hit-num">23661</span>件</div></body></html>'
                if location == "pm210" and job == "jb100":
                    return '<html><body><div class="p results-summary">該当求人数<span class="count js-hit-num">3801</span>件</div></body></html>'
                if location == "pm210" and job == "jb200":
                    return '<html><body><div class="p results-summary">該当求人数<span class="count js-hit-num">5341</span>件</div></body></html>'
                raise AssertionError(f"unexpected filters: {filters}")

        scopes = _plan_search_scopes(_Client(), base_html)
        scope_keys = [scope.key for scope in scopes]
        self.assertIn("job_list:f[s3][]=pm230", scope_keys)
        self.assertNotIn("job_list:f[s3][]=pm210", scope_keys)
        self.assertIn("job_list:f[s1][]=jb100&f[s3][]=pm210", scope_keys)
        self.assertIn("job_list:f[s1][]=jb200&f[s3][]=pm210", scope_keys)

    def test_pipeline_keeps_running_checkpoint_when_next_page_fetch_fails(self) -> None:
        class _Client:
            stats = {"requests": 2, "errors": 1}

            def fetch_search_page(self, page: int = 1, *, filters=None) -> str | None:
                _ = filters
                if page == 1:
                    return """
                    <html><body>
                      <div>検索結果一覧102件（1～51件表示）</div>
                      <article class="job-info">
                        <a class="link-job-detail" href="/job/81204678/">
                          <header class="job-info__header">
                            <h3 class="job-info__title">
                              <div class="title">求人1</div>
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
                return None

            def fetch_job_page(self, detail_url: str) -> str | None:
                _ = detail_url
                return """
                <html><body>
                  <script type="application/ld+json">{"@context":"https://schema.org/","@type":"JobPosting","hiringOrganization":{"@type":"Organization","name":"東急建設株式会社","sameAs":"https://www.tokyu-cnst.co.jp/"}}</script>
                  <h1>東急建設株式会社 求人1</h1>
                  <a href="/company/80224721/">東急建設株式会社</a>
                  <table>
                    <tr><th><h3>本社所在地</h3></th><td>東京都 渋谷区渋谷１丁目１６－１４</td></tr>
                    <tr><th><h3>企業URL</h3></th><td><a target="_blank" href="https://www.tokyu-cnst.co.jp/">https://www.tokyu-cnst.co.jp/</a></td></tr>
                  </table>
                </body></html>
                """

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            from japan_crawler.sites.pasonacareer import pipeline as target

            original_client = target.PasonacareerClient
            target.PasonacareerClient = lambda **kwargs: _Client()  # type: ignore[assignment]
            try:
                stats = run_pipeline_list(output_dir=output_dir, request_delay=0.0, proxy="", max_pages=0, detail_workers=1)
            finally:
                target.PasonacareerClient = original_client  # type: ignore[assignment]
            self.assertEqual(1, stats["pages_done"])
            conn = sqlite3.connect(output_dir / "pasonacareer_store.db")
            checkpoint = conn.execute(
                "SELECT last_page, total_pages, status FROM checkpoints WHERE scope = 'job_list'"
            ).fetchone()
            conn.close()
            self.assertEqual((1, 2, "running"), checkpoint)

    def test_fetch_job_detail_falls_back_when_detail_page_is_empty(self) -> None:
        class _Client:
            def fetch_job_page(self, detail_url: str) -> str | None:
                _ = detail_url
                return None

        company = _fetch_job_detail(
            _Client(),
            {
                "detail_url": "/job/81224876/",
                "company_name": "株式会社テスト",
                "job_title": "求人",
                "job_location": "北海道",
            },
        )
        self.assertEqual("株式会社テスト", company["company_name"])
        self.assertEqual("", company["website"])
        self.assertEqual("", company["address"])

    def test_pipeline_prefers_company_sitemap_when_available(self) -> None:
        sitemap_xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://www.pasonacareer.jp/company/80204433/</loc></url>
        </urlset>
        """
        company_html = """
        <html><body>
          <h1>株式会社三菱ＵＦＪ銀行 の中途採用・転職・求人情報</h1>
          <table>
            <tr><th>本社所在地</th><td>東京都 千代田区丸の内１丁目４－５</td></tr>
            <tr><th>企業URL</th><td><a href="https://www.bk.mufg.jp/">https://www.bk.mufg.jp/</a></td></tr>
          </table>
        </body></html>
        """

        class _Client:
            stats = {"requests": 2, "errors": 0}

            def __init__(self) -> None:
                self.search_calls = 0

            def fetch_company_sitemap(self) -> str | None:
                return sitemap_xml

            def fetch_company_page(self, detail_url: str) -> str | None:
                _ = detail_url
                return company_html

            def fetch_search_page(self, page: int = 1, *, filters=None) -> str | None:  # noqa: ARG002
                self.search_calls += 1
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            from japan_crawler.sites.pasonacareer import pipeline as target

            original_client = target.PasonacareerClient
            target.PasonacareerClient = lambda **kwargs: _Client()  # type: ignore[assignment]
            try:
                stats = run_pipeline_list(output_dir=output_dir, request_delay=0.0, proxy="", max_pages=0, detail_workers=1)
            finally:
                target.PasonacareerClient = original_client  # type: ignore[assignment]
            self.assertEqual(1, stats["pages_done"])
            self.assertEqual(1, stats["new_companies"])
            conn = sqlite3.connect(output_dir / "pasonacareer_store.db")
            row = conn.execute("SELECT company_name, website, address FROM companies").fetchone()
            checkpoint = conn.execute(
                "SELECT last_page, total_pages, status FROM checkpoints WHERE scope = 'company_sitemap'"
            ).fetchone()
            conn.close()
            self.assertEqual(("株式会社三菱ＵＦＪ銀行", "https://www.bk.mufg.jp", "東京都 千代田区丸の内１丁目４－５"), row)
            self.assertEqual((1, 1, "done"), checkpoint)


if __name__ == "__main__":
    unittest.main()
