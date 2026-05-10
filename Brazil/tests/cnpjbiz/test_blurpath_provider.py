"""Blurpath 浏览器态提供器测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.blurpath_provider import BlurpathBrowserProvider
from brazil_crawler.sites.cnpjbiz.proxy_pool import ProxyPoolConfig
from brazil_crawler.sites.cnpjbiz.proxy_pool import fetch_blurpath_candidates


class BlurpathProviderTests(unittest.TestCase):
    def test_fetch_blurpath_candidates_builds_auth_proxy_urls(self) -> None:
        with patch(
            "brazil_crawler.sites.cnpjbiz.proxy_pool.BlurpathBrowserProvider.fetch_bundle",
            return_value=type(
                "Bundle",
                (),
                {
                    "username": "user1",
                    "password": "pass1",
                    "white_proxies": ["1.1.1.1:80", "2.2.2.2:81"],
                },
            )(),
        ):
            values = fetch_blurpath_candidates(
                ProxyPoolConfig(
                    blurpath_enabled=True,
                    blurpath_cdp_url="http://127.0.0.1:9222",
                    scheme="http",
                )
            )
        self.assertEqual(
            [
                "http://user1:pass1@1.1.1.1:80",
                "http://user1:pass1@2.2.2.2:81",
            ],
            values,
        )

    def test_browser_provider_bundle_uses_runtime_and_api_helpers(self) -> None:
        provider = BlurpathBrowserProvider("http://127.0.0.1:9222")
        with patch.object(provider, "_fetch_auth_token", return_value="token123"), \
             patch.object(provider, "_fetch_first_account", return_value={"username": "u1", "password1": "p1"}), \
             patch.object(provider, "_fetch_white_proxies", return_value=["1.1.1.1:80"]):
            bundle = provider.fetch_bundle()
        self.assertEqual("token123", bundle.token)
        self.assertEqual("u1", bundle.username)
        self.assertEqual("p1", bundle.password)
        self.assertEqual(["1.1.1.1:80"], bundle.white_proxies)
