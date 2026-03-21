"""indonesiayp 客户端重试与代理参数测试。"""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import indonesia_crawler.indonesiayp.client as client_mod


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


class _FakeResponse:
    """简化响应对象。"""

    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """可注入返回序列的会话桩。"""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.closed = False

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise RuntimeError("fake session has no response")
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


class TestIndonesiaYpClient(unittest.TestCase):
    """覆盖 403 重试与代理参数透传。"""

    def test_should_retry_403_and_rotate_session(self) -> None:
        created: list[_FakeSession] = []

        def _factory(*, impersonate: str) -> _FakeSession:  # noqa: ARG001
            responses = [_FakeResponse(403)] if len(created) == 0 else [_FakeResponse(200, "<html>ok</html>")]
            session = _FakeSession(responses)
            created.append(session)
            return session

        with _EnvGuard(
            {
                "IYP_MAX_RETRIES": "2",
                "IYP_403_WAIT": "2",
            }
        ):
            with (
                patch.object(client_mod.cffi_requests, "Session", side_effect=_factory),
                patch.object(client_mod.time, "sleep", return_value=None),
                patch.object(client_mod.random, "uniform", return_value=0.0),
            ):
                client = client_mod.IndonesiaYpClient(
                    rate_config=client_mod.RateLimitConfig(
                        min_delay=0,
                        max_delay=0,
                        long_rest_interval=0,
                        long_rest_seconds=0,
                    )
                )
                html = client.get_html("/company/1")
                self.assertEqual(html, "<html>ok</html>")
                self.assertEqual(len(created), 2)
                self.assertTrue(created[0].closed)
                client.close()
                self.assertTrue(created[1].closed)

    def test_should_attach_proxy_when_configured(self) -> None:
        session = _FakeSession([_FakeResponse(200, "<html>proxy</html>")])

        def _factory(*, impersonate: str) -> _FakeSession:  # noqa: ARG001
            return session

        with _EnvGuard(
            {
                "IYP_PROXY_URL": "socks5h://127.0.0.1:7897",
                "IYP_MAX_RETRIES": "1",
            }
        ):
            with (
                patch.object(client_mod.cffi_requests, "Session", side_effect=_factory),
                patch.object(client_mod.time, "sleep", return_value=None),
                patch.object(client_mod.random, "uniform", return_value=0.0),
            ):
                client = client_mod.IndonesiaYpClient(
                    rate_config=client_mod.RateLimitConfig(
                        min_delay=0,
                        max_delay=0,
                        long_rest_interval=0,
                        long_rest_seconds=0,
                    )
                )
                html = client.get_html("/company/2")
                self.assertEqual(html, "<html>proxy</html>")
                self.assertEqual(session.calls[0]["proxy"], "socks5h://127.0.0.1:7897")
                client.close()


if __name__ == "__main__":
    unittest.main()
