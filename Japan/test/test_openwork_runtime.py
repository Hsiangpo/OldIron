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
from japan_crawler.sites.openwork.client import OpenworkPageNotFound
from japan_crawler.sites.openwork.pipeline import _plan_list_scopes
from japan_crawler.sites.openwork.pipeline import _wait_for_list_page_html
from japan_crawler.sites.openwork.pipeline import _load_company_details
from japan_crawler.sites.openwork.browser_profile import OpenworkBrowserBlocked


class _FailingBrowser:
    def __init__(self) -> None:
        self.fetch_calls = 0

    def fetch_html(self, *, url: str, ready_selector: str) -> str:  # noqa: ARG002
        self.fetch_calls += 1
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


class _NotFoundListClient:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_list_page(self, page: int, *, field: str = "", pref: str = "", src_str: str = "", ct: str = "") -> str | None:  # noqa: ARG002
        self.calls += 1
        raise OpenworkPageNotFound(f"page={page}")


class _ScopePlanClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, str]] = []

    def fetch_list_page(self, page: int, *, field: str = "", pref: str = "", src_str: str = "", ct: str = "") -> str | None:  # noqa: ARG002
        self.calls.append((page, field, pref))
        if page != 1:
            return None
        if not field and not pref:
            return """
            <html><body>
            <a href="/company_list?field=0023&sort=1">IT</a>
            <a href="/company_list?field=0067&sort=1">Retail</a>
            <a href="/company_list?pref=13&sort=1">Tokyo</a>
            <a href="/company_list?pref=27&sort=1">Osaka</a>
            </body></html>
            """
        if field == "0023" and not pref:
            return "<html><body><div>1,200 件中 1～50件を表示</div></body></html>"
        if field == "0067" and not pref:
            return "<html><body><div>180 件中 1～50件を表示</div></body></html>"
        if field == "0023" and pref == "13":
            return "<html><body><div>320 件中 1～50件を表示</div></body></html>"
        if field == "0023" and pref == "27":
            return "<html><body><div>90 件中 1～50件を表示</div></body></html>"
        return "<html><body><div>0 件中 0～0件を表示</div></body></html>"


class _FieldScopedPrefClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, str]] = []

    def fetch_list_page(self, page: int, *, field: str = "", pref: str = "", src_str: str = "", ct: str = "") -> str | None:  # noqa: ARG002
        self.calls.append((page, field, pref))
        if page != 1:
            return None
        if not field and not pref:
            return """
            <html><body>
            <a href="/company_list?field=0001&sort=1">制造</a>
            <a href="/company_list?pref=13&sort=1">东京</a>
            <a href="/company_list?pref=27&sort=1">大阪</a>
            <a href="/company_list?pref=40&sort=1">福冈</a>
            </body></html>
            """
        if field == "0001" and not pref:
            return """
            <html><body>
            <div>1,086 件中 1～50件を表示</div>
            <a href="/company_list?field=0001&pref=13&sort=1">东京</a>
            <a href="/company_list?field=0001&pref=27&sort=1">大阪</a>
            </body></html>
            """
        if field == "0001" and pref == "13":
            return "<html><body><div>819 件中 1～50件を表示</div></body></html>"
        if field == "0001" and pref == "27":
            return "<html><body><div>120 件中 1～50件を表示</div></body></html>"
        raise AssertionError(f'unexpected filters: field={field}, pref={pref}')


class _StaticResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


class _StaticSession:
    def __init__(self, response: _StaticResponse) -> None:
        self._response = response

    def get(self, url: str, params=None, timeout: int = 30):  # noqa: ANN001, ARG002
        return self._response


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

    def test_browser_response_does_not_enter_manual_auth_fallback(self) -> None:
        browser = _ManualAuthBrowser()
        client = OpenworkClient.__new__(OpenworkClient)
        client._browser_client = browser
        client._browser_lock = threading.Lock()
        client._browser_mode = False
        client._browser_notice_logged = False
        client._request_count = 0
        client._captcha_api_key = ""

        result = client._browser_response("https://www.openwork.jp/company.php?m_id=test")
        self.assertIsNone(result)
        self.assertEqual(0, browser.auth_calls)
        self.assertEqual(1, browser.fetch_calls)

    def test_get_with_retry_does_not_auto_fallback_to_browser_on_403(self) -> None:
        browser = _FailingBrowser()
        client = OpenworkClient.__new__(OpenworkClient)
        client._browser_client = browser
        client._browser_lock = threading.Lock()
        client._browser_mode = False
        client._browser_notice_logged = False
        client._captcha_notice_logged = False
        client._captcha_api_key = "2cc-key"
        client._proxy_url = ""
        client._proxy_cooldown_until = 0
        client._request_count = 0
        client._error_count = 0
        client._max_retries = 1
        client._request_modes = lambda: [False]
        client._session = lambda use_proxy: _StaticSession(_StaticResponse(403, "forbidden"))  # noqa: ARG005
        client._polite_delay = lambda: None
        client._sleep_backoff = lambda *args: None
        client._disable_proxy_temporarily = lambda reason: None  # noqa: ARG005
        client._solve_captcha_if_possible = lambda **kwargs: None

        result = client._get_with_retry("https://www.openwork.jp/company_list")
        self.assertIsNone(result)
        self.assertEqual(0, browser.fetch_calls)

    def test_get_with_retry_does_not_auto_fallback_to_browser_on_captcha_page(self) -> None:
        browser = _FailingBrowser()
        client = OpenworkClient.__new__(OpenworkClient)
        client._browser_client = browser
        client._browser_lock = threading.Lock()
        client._browser_mode = False
        client._browser_notice_logged = False
        client._captcha_notice_logged = False
        client._captcha_api_key = "2cc-key"
        client._proxy_url = ""
        client._proxy_cooldown_until = 0
        client._request_count = 0
        client._error_count = 0
        client._max_retries = 1
        client._request_modes = lambda: [False]
        client._session = lambda use_proxy: _StaticSession(_StaticResponse(200, "<title>画像認証</title>"))  # noqa: ARG005
        client._polite_delay = lambda: None
        client._sleep_backoff = lambda *args: None
        client._disable_proxy_temporarily = lambda reason: None  # noqa: ARG005
        client._solve_captcha_if_possible = lambda **kwargs: None

        result = client._get_with_retry("https://www.openwork.jp/company_list")
        self.assertIsNone(result)
        self.assertEqual(0, browser.fetch_calls)

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

    def test_wait_for_list_page_html_respects_custom_retry_rounds(self) -> None:
        client = _RetryListClient([None, None, "<html>ok</html>"])
        with patch("japan_crawler.sites.openwork.pipeline.time.sleep", return_value=None):
            html = _wait_for_list_page_html(client, 9, max_rounds=2)
        self.assertIsNone(html)
        self.assertEqual(2, client.calls)

    def test_wait_for_list_page_html_treats_404_scope_as_empty_without_retry(self) -> None:
        client = _NotFoundListClient()
        with patch("japan_crawler.sites.openwork.pipeline.time.sleep", return_value=None):
            html = _wait_for_list_page_html(client, 1, field="0001", pref="2", max_rounds=40)
        self.assertIn("0 件中", html or "")
        self.assertEqual(1, client.calls)

    def test_plan_list_scopes_splits_large_field_by_pref(self) -> None:
        client = _ScopePlanClient()
        scopes = _plan_list_scopes(client, client.fetch_list_page(1) or "")
        scope_keys = [scope.key for scope in scopes]
        self.assertIn("company_list:field=0067", scope_keys)
        self.assertIn("company_list:field=0023&pref=13", scope_keys)
        self.assertIn("company_list:field=0023&pref=27", scope_keys)
        self.assertNotIn("company_list:field=0023", scope_keys)

    def test_plan_list_scopes_prefers_field_specific_pref_codes(self) -> None:
        client = _FieldScopedPrefClient()
        scopes = _plan_list_scopes(client, client.fetch_list_page(1) or "")
        scope_keys = [scope.key for scope in scopes]
        self.assertIn("company_list:field=0001&pref=13", scope_keys)
        self.assertIn("company_list:field=0001&pref=27", scope_keys)
        self.assertNotIn("company_list:field=0001&pref=40", scope_keys)


if __name__ == "__main__":
    unittest.main()
