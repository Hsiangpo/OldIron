import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandBrowserCookieTests(unittest.TestCase):
    def test_build_cookie_header_filters_non_dnb_domains(self) -> None:
        from england_crawler.dnb.browser_cookie import _build_cookie_header

        cookies = [
            {"name": "ak_bmsc", "value": "abc", "domain": ".dnb.com", "path": "/", "expires": -1},
            {"name": "bm_sv", "value": "def", "domain": "www.dnb.com", "path": "/", "expires": -1},
            {"name": "sid", "value": "zzz", "domain": ".google.com", "path": "/", "expires": -1},
        ]

        header = _build_cookie_header(cookies)

        self.assertIn("ak_bmsc=abc", header)
        self.assertIn("bm_sv=def", header)
        self.assertNotIn("sid=zzz", header)

    def test_resolve_dnb_cookie_header_updates_env_file(self) -> None:
        from england_crawler.dnb.browser_cookie import resolve_dnb_cookie_header

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / ".env"
            env_path.write_text("DNB_COOKIE_HEADER=old=value\n", encoding="utf-8")
            with patch("england_crawler.dnb.browser_cookie.fetch_live_dnb_cookie_header", return_value="new=value; bm_sv=1"):
                with patch.dict(os.environ, {"DNB_COOKIE_HEADER": "old=value"}, clear=False):
                    header = resolve_dnb_cookie_header(project_root=root)

            self.assertEqual("new=value; bm_sv=1", header)
            content = env_path.read_text(encoding="utf-8")
            self.assertIn("DNB_COOKIE_HEADER=new=value; bm_sv=1", content)

    def test_cookie_provider_refreshes_on_force(self) -> None:
        from england_crawler.dnb.browser_cookie import DnbCookieProvider

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            values = iter(["first=value", "second=value"])
            with patch("england_crawler.dnb.browser_cookie.resolve_dnb_cookie_header", side_effect=lambda **_: next(values)):
                provider = DnbCookieProvider(project_root=root, min_refresh_seconds=3600)
                first = provider.get(force_refresh=True)
                second = provider.get(force_refresh=False)
                third = provider.get(force_refresh=True)

        self.assertEqual("first=value", first)
        self.assertEqual("first=value", second)
        self.assertEqual("second=value", third)


if __name__ == "__main__":
    unittest.main()
