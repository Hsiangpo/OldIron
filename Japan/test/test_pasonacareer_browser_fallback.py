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


if __name__ == "__main__":
    unittest.main()
