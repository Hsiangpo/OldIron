"""PasonaCareer 客户端测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from japan_crawler.sites.pasonacareer.client import PasonacareerClient


class PasonacareerClientTests(unittest.TestCase):
    def test_fetch_search_page_uses_protocol_params_only(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._request_count = 0
        client._error_count = 0

        class _Response:
            status_code = 200
            text = "<html>ok</html>"

        captured: dict[str, object] = {}

        def _fake_get(url, params=None, **kwargs):  # noqa: ANN001, ARG001
            captured["url"] = url
            captured["params"] = params
            return _Response()

        client._get_with_retry = _fake_get  # type: ignore[method-assign]

        html = client.fetch_search_page(3, filters={"f[s3][]": "pm210", "f[s1][]": "jb100"})
        self.assertEqual("<html>ok</html>", html)
        self.assertEqual(
            {
                "utf8": "✓",
                "f[f]": "1",
                "f[q]": "",
                "f[s3][]": "pm210",
                "f[s1][]": "jb100",
                "page": "3",
            },
            captured["params"],
        )

    def test_fetch_search_page_returns_none_without_browser_fallback(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._request_count = 0
        client._error_count = 0
        client._get_with_retry = lambda url, params=None, **kwargs: None  # noqa: ARG005
        self.assertIsNone(client.fetch_search_page(197, filters={"f[s3][]": "pm210"}))

    def test_fetch_job_page_returns_none_without_browser_fallback(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        client._request_count = 0
        client._error_count = 0
        client._get_with_retry = lambda url, **kwargs: None  # noqa: ARG005
        self.assertIsNone(client.fetch_job_page("/job/1/"))

    def test_browser_primary_is_always_false(self) -> None:
        client = PasonacareerClient.__new__(PasonacareerClient)
        self.assertFalse(client.browser_primary)


if __name__ == "__main__":
    unittest.main()
