"""ahu 客户端重试策略测试。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import indonesia_crawler.ahu.client as client_mod
from indonesia_crawler.proxy import ProxyLease


class _FakeResponse:
    """简化响应对象。"""

    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """可控的会话桩。"""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)

    def get(self, url: str, **kwargs) -> _FakeResponse:  # noqa: ANN003
        if not self.responses:
            raise RuntimeError("no more fake responses")
        return self.responses.pop(0)

    def close(self) -> None:
        return None


class _FakePool:
    """最小代理池桩。"""

    enabled = True

    def __init__(self) -> None:
        self.failures = 0
        self.successes = 0

    def acquire(self):  # noqa: ANN001,ANN202
        return ProxyLease(endpoint_id=0, proxy_url="http://127.0.0.1:7897", label="127.0.0.1:7897")

    def mark_failure(self, endpoint_id: int) -> int:  # noqa: ARG002
        self.failures += 1
        return 30

    def mark_success(self, endpoint_id: int) -> None:  # noqa: ARG002
        self.successes += 1


class TestAhuClient(unittest.TestCase):
    """覆盖 429 与代理失败的判定。"""

    def test_rate_limit_should_not_mark_proxy_failure(self) -> None:
        client = client_mod.AhuClient(
            rate_config=client_mod.AhuRateLimitConfig(
                request_delay=0,
                timeout=5,
                max_retries=2,
                retry_backoff=1,
                rate_limit_wait=1,
            )
        )
        pool = _FakePool()
        client.proxy_pool = pool
        client.session = _FakeSession([_FakeResponse(429), _FakeResponse(200, "ok")])
        try:
            with patch.object(client_mod.time, "sleep", return_value=None):
                response = client._request_with_retry("get", "https://ahu.go.id/test", timeout=5)
        finally:
            client.close()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(pool.failures, 0)
        self.assertEqual(pool.successes, 1)

    def test_rate_limit_should_pause_after_exhaustion(self) -> None:
        client = client_mod.AhuClient(
            rate_config=client_mod.AhuRateLimitConfig(
                request_delay=0,
                timeout=5,
                max_retries=2,
                retry_backoff=1,
                rate_limit_wait=1,
            )
        )
        client.proxy_pool = _FakePool()
        client.session = _FakeSession([_FakeResponse(429), _FakeResponse(429)])
        try:
            with patch.object(client_mod.time, "sleep", return_value=None):
                with self.assertRaises(client_mod.AhuRateLimitError) as ctx:
                    client._request_with_retry("get", "https://ahu.go.id/test", timeout=5)
        finally:
            client.close()
        self.assertGreaterEqual(ctx.exception.retry_after, 30)


if __name__ == "__main__":
    unittest.main()
