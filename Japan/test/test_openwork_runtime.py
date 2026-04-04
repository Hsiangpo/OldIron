"""OpenWork 运行时回退测试。"""

from __future__ import annotations

import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.openwork.client import OpenworkClient
from japan_crawler.sites.openwork.pipeline import _wait_for_list_page_html
from japan_crawler.sites.openwork.pipeline import _load_company_details
from japan_crawler.sites.openwork.browser_profile import OpenworkBrowserBlocked


class _FailingBrowser:
    def fetch_html(self, *, url: str, ready_selector: str) -> str:  # noqa: ARG002
        raise RuntimeError("browser blocked")


class _ManualAuthBrowser:
    def __init__(self) -> None:
        self.fetch_calls = 0
        self.auth_calls = 0

    def fetch_html(self, *, url: str, ready_selector: str) -> str:  # noqa: ARG002
        self.fetch_calls += 1
        if self.fetch_calls == 1:
            raise OpenworkBrowserBlocked("need auth")
        return "<html>ok</html>"

    def prepare_manual_auth(self) -> None:
        self.auth_calls += 1


class _FakeClient:
    def __init__(self) -> None:
        self.browser_primary = False
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def fetch_detail_page(self, detail_url: str) -> str:
        _ = detail_url
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.05)
        with self.lock:
            self.active -= 1
        return """
        <html><body>
        <a class="noLink v-m">株式会社リクルート</a>
        <table class="definitionList-wiki jsDefinitionList w-100p">
          <tr><th>業界</th><td><ul><li>情報サービス、リサーチ</li></ul></td></tr>
          <tr><th>URL</th><td><a href="https://www.recruit.co.jp/">https://www.recruit.co.jp/</a></td></tr>
          <tr><th>所在地</th><td>東京都千代田区丸の内1-9-2</td></tr>
          <tr><th>代表者</th><td>代表取締役社長 牛田 圭一</td></tr>
        </table>
        </body></html>
        """


class _RetryListClient:
    def __init__(self, responses: list[str | None]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def fetch_list_page(self, page: int) -> str | None:
        _ = page
        self.calls += 1
        return self._responses.pop(0) if self._responses else None


class OpenworkRuntimeTests(unittest.TestCase):
    def test_browser_response_failure_returns_none(self) -> None:
        client = OpenworkClient.__new__(OpenworkClient)
        client._browser_client = _FailingBrowser()
        client._browser_lock = threading.Lock()
        client._browser_mode = False
        client._browser_notice_logged = False
        client._request_count = 0
        client._captcha_api_key = ""
        client._manual_auth_attempted = False

        result = client._browser_response("https://www.openwork.jp/company.php?m_id=test")
        self.assertIsNone(result)

    def test_browser_response_retries_once_after_manual_auth(self) -> None:
        browser = _ManualAuthBrowser()
        client = OpenworkClient.__new__(OpenworkClient)
        client._browser_client = browser
        client._browser_lock = threading.Lock()
        client._browser_mode = False
        client._browser_notice_logged = False
        client._request_count = 0
        client._captcha_api_key = ""
        client._manual_auth_attempted = False

        result = client._browser_response("https://www.openwork.jp/company.php?m_id=test")
        self.assertIsNotNone(result)
        self.assertEqual(1, browser.auth_calls)
        self.assertEqual(2, browser.fetch_calls)

    def test_load_company_details_parallelizes_when_browser_not_primary(self) -> None:
        client = _FakeClient()
        cards = [
            {"company_id": "1", "company_name": "A", "industry": "IT", "detail_url": "/company.php?m_id=1"},
            {"company_id": "2", "company_name": "B", "industry": "IT", "detail_url": "/company.php?m_id=2"},
            {"company_id": "3", "company_name": "C", "industry": "IT", "detail_url": "/company.php?m_id=3"},
        ]
        results = _load_company_details(client, cards, detail_workers=3)
        self.assertEqual(3, len(results))
        self.assertGreaterEqual(client.max_active, 2)

    def test_wait_for_list_page_html_retries_until_success(self) -> None:
        client = _RetryListClient([None, None, "<html>ok</html>"])
        with patch("japan_crawler.sites.openwork.pipeline.time.sleep", return_value=None):
            html = _wait_for_list_page_html(client, 9)
        self.assertEqual("<html>ok</html>", html)
        self.assertEqual(3, client.calls)


if __name__ == "__main__":
    unittest.main()
