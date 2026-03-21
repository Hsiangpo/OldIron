"""代理池配置与轮换测试。"""

from __future__ import annotations

import os
import tempfile
import unittest
from urllib.parse import unquote, urlsplit

from indonesia_crawler.proxy.pool import build_proxy_pool_from_env


class _EnvGuard:
    """测试期间临时覆盖环境变量。"""

    def __init__(self, updates: dict[str, str]) -> None:
        self.updates = updates
        self._backup: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self.updates.items():
            self._backup[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, exc_type, exc, tb) -> None:
        for key, old_value in self._backup.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


class TestProxyPool(unittest.TestCase):
    """覆盖代理池构建、熔断与 sid 拼接。"""

    def test_static_mode_should_build_single_proxy(self) -> None:
        with _EnvGuard(
            {
                "AHU_PROXY_MODE": "static",
                "AHU_PROXY_URL": "http://user:pass@127.0.0.1:8080",
            }
        ):
            pool = build_proxy_pool_from_env(prefix="AHU")
            self.assertIsNotNone(pool)
            self.assertTrue(pool.enabled)
            lease = pool.acquire()
            self.assertIsNotNone(lease)
            assert lease is not None
            self.assertEqual(lease.label, "127.0.0.1:8080")
            self.assertIn("127.0.0.1:8080", lease.proxy_url)

    def test_pool_mode_should_failover_on_failure(self) -> None:
        with _EnvGuard(
            {
                "AHU_PROXY_MODE": "pool",
                "AHU_PROXY_LIST": "host-a:1001:user_a:pass_a;host-b:1002:user_b:pass_b",
                "AHU_PROXY_COOLDOWN_SECONDS": "120",
            }
        ):
            pool = build_proxy_pool_from_env(prefix="AHU")
            self.assertIsNotNone(pool)
            first = pool.acquire()
            self.assertIsNotNone(first)
            assert first is not None
            pool.mark_failure(first.endpoint_id)
            second = pool.acquire()
            self.assertIsNotNone(second)
            assert second is not None
            self.assertNotEqual(first.endpoint_id, second.endpoint_id)
            self.assertEqual(second.label, "host-b:1002")

    def test_sid_enabled_should_append_sid_and_ttl(self) -> None:
        with _EnvGuard(
            {
                "AHU_PROXY_MODE": "static",
                "AHU_PROXY_URL": "http://demo-region-US:secret@proxy.local:8081",
                "AHU_PROXY_SID_ENABLED": "true",
                "AHU_PROXY_SID_TTL_MINUTES": "3",
                "AHU_PROXY_SID_LENGTH": "10",
            }
        ):
            pool = build_proxy_pool_from_env(prefix="AHU")
            self.assertIsNotNone(pool)
            lease = pool.acquire()
            self.assertIsNotNone(lease)
            assert lease is not None
            username = unquote(urlsplit(lease.proxy_url).username or "")
            self.assertIn("-sid-", username)
            self.assertIn("-t-3", username)

    def test_pool_mode_should_load_from_file(self) -> None:
        content = "# comment\nhost-1:2001:user1:pass1\nhost-2:2002\n"
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as fp:
            fp.write(content)
            temp_path = fp.name
        self.addCleanup(lambda: os.path.exists(temp_path) and os.remove(temp_path))

        with _EnvGuard(
            {
                "AHU_PROXY_MODE": "pool",
                "AHU_PROXY_LIST_FILE": temp_path,
                "AHU_PROXY_DEFAULT_SCHEME": "socks5",
            }
        ):
            pool = build_proxy_pool_from_env(prefix="AHU")
            self.assertIsNotNone(pool)
            self.assertTrue(pool.enabled)
            lease = pool.acquire()
            self.assertIsNotNone(lease)
            assert lease is not None
            self.assertTrue(lease.proxy_url.startswith("socks5://"))


if __name__ == "__main__":
    unittest.main()
