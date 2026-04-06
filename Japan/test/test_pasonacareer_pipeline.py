"""PasonaCareer pipeline 测试。"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.pasonacareer.pipeline import _fetch_search_page_with_retries
from japan_crawler.sites.pasonacareer.pipeline import _resolve_start_page
from japan_crawler.sites.pasonacareer.pipeline import run_pipeline_list


class PasonacareerPipelineTests(unittest.TestCase):
    def test_fetch_search_page_with_retries_eventually_succeeds(self) -> None:
        class _Client:
            def __init__(self) -> None:
                self.calls = 0

            def fetch_search_page(self, page: int) -> str | None:
                _ = page
                self.calls += 1
                return "<html>ok</html>" if self.calls >= 3 else None

        client = _Client()
        from japan_crawler.sites.pasonacareer import pipeline as target

        original_sleep = target.time.sleep
        target.time.sleep = lambda seconds: None  # type: ignore[assignment]
        try:
            html = _fetch_search_page_with_retries(client, 197)
        finally:
            target.time.sleep = original_sleep  # type: ignore[assignment]
        self.assertEqual("<html>ok</html>", html)
        self.assertEqual(3, client.calls)

    def test_resolve_start_page_resumes_stale_done_checkpoint(self) -> None:
        self.assertEqual(197, _resolve_start_page({"last_page": 196, "total_pages": 997, "status": "done"}))

    def test_resolve_start_page_skips_true_done_checkpoint(self) -> None:
        self.assertIsNone(_resolve_start_page({"last_page": 997, "total_pages": 997, "status": "done"}))

    def test_pipeline_keeps_running_checkpoint_when_next_page_fetch_fails(self) -> None:
        class _Client:
            stats = {"requests": 2, "errors": 1}

            def fetch_search_page(self, page: int = 1) -> str | None:
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


if __name__ == "__main__":
    unittest.main()
