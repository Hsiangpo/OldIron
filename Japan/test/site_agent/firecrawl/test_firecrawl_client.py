from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from unittest.mock import Mock
from typing import Any, Dict
from pathlib import Path

from site_agent.firecrawl_client import FirecrawlClient
from site_agent.firecrawl_client import FirecrawlConfig
from site_agent.firecrawl_client import FirecrawlError
from site_agent.firecrawl_key_pool import KeyPool
from site_agent.firecrawl_key_pool import KeyPoolConfig
from site_agent.firecrawl_key_pool import KeyState


class DummyResponse:
    def __init__(
        self,
        status_code: int,
        payload: Dict[str, Any] | None = None,
        headers: Dict[str, Any] | None = None,
    ):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload


class TestFirecrawlClient(unittest.TestCase):
    def test_scrape_success(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"], KeyPoolConfig(per_key_limit=1, wait_seconds=1, shared_pool=False)
            )
            client = FirecrawlClient(pool, FirecrawlConfig(max_retries=0))
            client._session.request = Mock(
                return_value=DummyResponse(200, {"data": {"markdown": "ok"}})
            )
            result = await client.scrape("https://example.com")
            self.assertIn("data", result)

        asyncio.run(run())

    def test_unauthorized_disables_key(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"], KeyPoolConfig(per_key_limit=1, wait_seconds=1, shared_pool=False)
            )
            client = FirecrawlClient(pool, FirecrawlConfig(max_retries=0))
            client._session.request = Mock(return_value=DummyResponse(401, {}))
            with self.assertRaises(FirecrawlError):
                await client.scrape("https://example.com")
            snapshot = await pool.snapshot()
            self.assertEqual(snapshot[0].state, KeyState.DISABLED)

        asyncio.run(run())

    def test_rate_limit_sets_cooldown(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"], KeyPoolConfig(per_key_limit=1, wait_seconds=1, shared_pool=False)
            )
            client = FirecrawlClient(pool, FirecrawlConfig(max_retries=0))
            client._session.request = Mock(
                return_value=DummyResponse(429, {}, {"Retry-After": "2"})
            )
            with self.assertRaises(FirecrawlError):
                await client.scrape("https://example.com")
            snapshot = await pool.snapshot()
            self.assertEqual(snapshot[0].state, KeyState.COOLDOWN)
            self.assertIsNotNone(snapshot[0].cooldown_until)
            cooldown_until = snapshot[0].cooldown_until or 0.0
            self.assertGreater(cooldown_until, time.monotonic())

        asyncio.run(run())

    def test_payment_required_removes_key_from_file(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                key_file = Path(tmpdir) / "firecrawl_keys.txt"
                key_file.write_text("k1\n", encoding="utf-8")
                pool = KeyPool(
                    ["k1"],
                    KeyPoolConfig(per_key_limit=1, wait_seconds=1, shared_pool=False),
                    key_file_path=key_file,
                )
                client = FirecrawlClient(pool, FirecrawlConfig(max_retries=0))
                client._session.request = Mock(return_value=DummyResponse(402, {}))
                with self.assertRaises(FirecrawlError):
                    await client.scrape("https://example.com")
                with self.assertRaises(ValueError):
                    KeyPool.load_keys(key_file)
                snapshot = await pool.snapshot()
                self.assertEqual(snapshot[0].state, KeyState.DISABLED)
                self.assertEqual(snapshot[0].disabled_reason, "payment_required")

        asyncio.run(run())

    def test_extract_polling(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"], KeyPoolConfig(per_key_limit=1, wait_seconds=1, shared_pool=False)
            )
            client = FirecrawlClient(pool, FirecrawlConfig(max_retries=0))

            async def fake_request(method, path, json_body=None):
                if method == "POST" and path == "extract":
                    return {"id": "job-1"}
                if method == "GET" and path == "extract/job-1":
                    return {"status": "completed", "data": {"company_name": "Acme"}}
                return {}

            client._request = fake_request
            response = await client.extract(
                ["https://example.com"],
                prompt="Extract",
                schema={
                    "type": "object",
                    "properties": {"company_name": {"type": "string"}},
                },
            )
            self.assertEqual(response.get("status"), "completed")
            self.assertEqual(response.get("data", {}).get("company_name"), "Acme")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
