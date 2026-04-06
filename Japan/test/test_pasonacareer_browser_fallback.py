from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from japan_crawler.sites.pasonacareer.browser_auth import PasonacareerPersistentBrowser
from japan_crawler.sites.pasonacareer.client import PasonacareerClient


class PasonacareerBrowserFallbackTests(unittest.TestCase):
    def test_browser_disables_itself_after_asyncio_loop_sync_api_error(self) -> None:
        browser = PasonacareerPersistentBrowser(user_data_dir=Path("tmp/pasona-browser-test"))
        browser._page_handle = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError(
                "It looks like you are using Playwright Sync API inside the asyncio loop. "
                "Please use the Async API instead."
            )
        )
        try:
            result = browser.fetch_job_page("/job/1")
            self.assertIsNone(result)
            self.assertTrue(browser.disabled)
            self.assertIsNone(browser.fetch_job_page("/job/2"))
        finally:
            browser.close()

    def test_client_disables_browser_auth_after_sync_api_loop_error(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser_auth_lock = threading.Lock()
        client._browser_cookie_header = ""
        client._browser_user_agent = ""
        client._browser_auth_expires_at = 0.0
        client._browser_auth_disabled = False
        client._proxy_url = ""
        client._local = threading.local()
        with patch(
            "japan_crawler.sites.pasonacareer.client.fetch_browser_auth",
            side_effect=RuntimeError(
                "Playwright Sync API is unavailable inside the current asyncio loop"
            ),
        ):
            refreshed = client._refresh_browser_auth("https://www.pasonacareer.jp/search/jl/")
        self.assertFalse(refreshed)
        self.assertTrue(client._browser_auth_disabled)

    def test_fetch_search_page_uses_protocol_without_browser_first(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser = type(
            "_Browser",
            (),
            {"fetch_search_page": lambda self, page: (_ for _ in ()).throw(AssertionError("browser should not be used"))},
        )()
        client._request_count = 0
        client._error_count = 0

        class _Response:
            status_code = 200
            text = "<html>ok</html>"

        client._get_with_retry = lambda url, params=None: _Response()  # noqa: ARG005

        html = client.fetch_search_page(3)
        self.assertEqual("<html>ok</html>", html)

    def test_fetch_job_page_uses_protocol_before_browser(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser = type(
            "_Browser",
            (),
            {"fetch_job_page": lambda self, detail_url: (_ for _ in ()).throw(AssertionError("browser should not be used"))},
        )()
        client._request_count = 0
        client._error_count = 0

        class _Response:
            status_code = 200
            text = "<html>job</html>"

        client._get_with_retry = lambda url, params=None, **kwargs: _Response()  # noqa: ARG005

        html = client.fetch_job_page("/job/1/")
        self.assertEqual("<html>job</html>", html)

    def test_fetch_search_page_falls_back_to_browser_after_protocol_failure(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser = type(
            "_Browser",
            (),
            {"fetch_search_page": lambda self, page: f"<html>browser-{page}</html>"},
        )()
        client._request_count = 0
        client._error_count = 0
        client._fetch_with_browser = lambda label, action: action()  # noqa: ARG005
        client._get_with_retry = lambda url, params=None: None  # noqa: ARG005

        html = client.fetch_search_page(197)
        self.assertEqual("<html>browser-197</html>", html)

    def test_browser_primary_is_disabled_for_parallel_detail_fetch(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser = object()
        self.assertFalse(client.browser_primary)

    def test_fetch_job_page_falls_back_to_browser_after_fast_fail(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._browser = type(
            "_Browser",
            (),
            {"fetch_job_page": lambda self, detail_url: "<html>browser</html>"},
        )()
        client._request_count = 0
        client._error_count = 0
        client._fetch_with_browser = lambda label, action: action()  # noqa: ARG005
        client._get_with_retry = lambda url, **kwargs: None  # noqa: ARG005

        html = client.fetch_job_page("/job/1/")
        self.assertEqual("<html>browser</html>", html)


if __name__ == "__main__":
    unittest.main()
