"""桥接代理辅助函数测试。"""

from __future__ import annotations

import unittest

from indonesia_crawler.proxy.bridge import (
    _build_connect_request,
    _build_upstream_request,
    _parse_pre_proxy_url,
)


class TestProxyBridge(unittest.TestCase):
    """覆盖桥接代理的协议拼装逻辑。"""

    def test_parse_pre_proxy_url_should_keep_remote_dns_flag(self) -> None:
        config = _parse_pre_proxy_url("socks5h://demo:secret@127.0.0.1:7897")
        self.assertIsNotNone(config)
        assert config is not None
        self.assertEqual(config.scheme, "socks5h")
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 7897)
        self.assertTrue(config.remote_dns)
        self.assertEqual(config.username, "demo")
        self.assertEqual(config.password, "secret")

    def test_build_upstream_request_should_override_proxy_headers(self) -> None:
        payload = _build_upstream_request(
            "GET /hello HTTP/1.1",
            {
                "Host": "example.com",
                "Proxy-Authorization": "Basic old",
                "Proxy-Connection": "Keep-Alive",
            },
            "Basic new",
            "proxy.local:8080",
            b"",
        )
        text = payload.decode("iso-8859-1")
        self.assertIn("GET http://example.com/hello HTTP/1.1", text)
        self.assertIn("Host: proxy.local:8080", text)
        self.assertIn("Proxy-Authorization: Basic new", text)
        self.assertIn("Connection: close", text)
        self.assertNotIn("Basic old", text)
        self.assertNotIn("Proxy-Connection", text)

    def test_build_connect_request_should_include_proxy_auth(self) -> None:
        payload = _build_connect_request(
            "ahu.go.id:443",
            "HTTP/1.1",
            "proxy.local:8080",
            "Basic token",
        )
        text = payload.decode("iso-8859-1")
        self.assertIn("CONNECT ahu.go.id:443 HTTP/1.1", text)
        self.assertIn("Host: proxy.local:8080", text)
        self.assertIn("Proxy-Authorization: Basic token", text)


if __name__ == "__main__":
    unittest.main()
