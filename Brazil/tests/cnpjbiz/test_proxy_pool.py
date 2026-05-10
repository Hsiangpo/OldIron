"""CNPJ Biz 代理池测试。"""

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

from brazil_crawler.sites.cnpjbiz.proxy_pool import CnpjBizProxyPool
from brazil_crawler.sites.cnpjbiz.proxy_pool import ProxyPoolConfig
from brazil_crawler.sites.cnpjbiz.proxy_pool import _parse_proxy_feed_payload
from brazil_crawler.sites.cnpjbiz.proxy_pool import _normalize_proxy_lines


class ProxyPoolTests(unittest.TestCase):
    def test_parse_proxy_feed_payload_handles_error_json(self) -> None:
        self.assertEqual([], _parse_proxy_feed_payload('{"ret":1,"msg":""}'))

    def test_normalize_proxy_lines_adds_scheme_and_dedupes(self) -> None:
        values = _normalize_proxy_lines("1.1.1.1:80\n1.1.1.1:80\nhttp://2.2.2.2:81\n", "http")
        self.assertEqual(["http://1.1.1.1:80", "http://2.2.2.2:81"], values)

    def test_proxy_pool_rotates_candidates(self) -> None:
        pool = CnpjBizProxyPool(ProxyPoolConfig(feed_url="https://example.com/feed", scheme="http"))
        with patch(
            "brazil_crawler.sites.cnpjbiz.proxy_pool._fetch_proxy_candidates",
            return_value=["http://1.1.1.1:80", "http://2.2.2.2:81"],
        ):
            self.assertEqual("http://1.1.1.1:80", pool.current_proxy())
            self.assertEqual("http://2.2.2.2:81", pool.rotate_proxy())
            self.assertEqual("http://2.2.2.2:81", pool.current_proxy())
