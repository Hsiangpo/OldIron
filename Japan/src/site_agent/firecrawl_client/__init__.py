from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests

from ..firecrawl_key_pool import KeyPool


class FirecrawlError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


@dataclass
class FirecrawlConfig:
    base_url: str = "https://api.firecrawl.dev/v2/"
    timeout_ms: int = 30000
    max_retries: int = 2
    only_main_content: bool = False
    poll_interval: float = 1.5
    poll_timeout: int = 120


class FirecrawlClient:
    def __init__(
        self, key_pool: KeyPool, config: FirecrawlConfig | None = None
    ) -> None:
        self._pool = key_pool
        self._config = config or FirecrawlConfig()
        self._session = requests.Session()
        self._session.headers.update({"accept": "application/json"})

    async def scrape(
        self, url: str, *, timeout_ms: int | None = None, rendered: bool = False
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "url": url,
            "formats": ["markdown", "rawHtml", "links"],
            "onlyMainContent": bool(self._config.only_main_content),
            "removeBase64Images": True,
            "timeout": int(timeout_ms or self._config.timeout_ms),
            "maxAge": 86400000,
        }
        if rendered:
            payload["waitFor"] = 1500
            payload["timeout"] = max(payload["timeout"], 60000)
        return await self._request("POST", "scrape", json_body=payload)

    async def extract(
        self, urls: list[str], *, prompt: str, schema: dict[str, Any]
    ) -> dict[str, Any]:
        payload = {
            "urls": urls,
            "prompt": prompt,
            "schema": schema,
        }
        start = await self._request("POST", "extract", json_body=payload)
        if isinstance(start, dict) and start.get("data"):
            return start
        job_id = start.get("id") if isinstance(start, dict) else None
        if not job_id:
            raise FirecrawlError("firecrawl_extract_no_id")
        return await self._poll_extract(job_id)

    async def _poll_extract(self, job_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + max(5, int(self._config.poll_timeout))
        path = f"extract/{job_id}"
        while True:
            data = await self._request("GET", path)
            status = data.get("status") if isinstance(data, dict) else None
            if status in ("completed", "success"):
                return data
            if status in ("failed", "error"):
                raise FirecrawlError("firecrawl_extract_failed")
            if time.monotonic() >= deadline:
                raise FirecrawlError("firecrawl_extract_timeout")
            await asyncio.sleep(self._config.poll_interval)

    async def _request(
        self, method: str, path: str, json_body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        retries = max(0, int(self._config.max_retries))
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            lease = await self._pool.acquire()
            key = lease.entry.key
            index = lease.entry.index
            try:
                url = urljoin(self._config.base_url, path)
                timeout_s = max(1.0, self._config.timeout_ms / 1000.0)
                headers = {"Authorization": f"Bearer {key}"}

                def _do_request() -> requests.Response:
                    return self._session.request(
                        method, url, json=json_body, headers=headers, timeout=timeout_s
                    )

                resp = await asyncio.to_thread(_do_request)
                status = int(resp.status_code)
                if status == 200:
                    await self._pool.mark_success(index)
                    return _safe_json(resp)
                if status == 401:
                    await self._pool.disable(index, "unauthorized")
                    raise FirecrawlError("firecrawl_401")
                if status == 402:
                    await self._pool.disable(index, "payment_required")
                    raise FirecrawlError("firecrawl_402")
                if status == 429:
                    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                    await self._pool.mark_rate_limited(index, retry_after=retry_after)
                    raise FirecrawlError("firecrawl_429")
                if status >= 500:
                    await self._pool.mark_failure(index)
                    raise FirecrawlError("firecrawl_5xx")
                await self._pool.mark_failure(index)
                raise FirecrawlError(f"firecrawl_http_{status}")
            except FirecrawlError as exc:
                last_error = exc
            except Exception as exc:  # pragma: no cover - network variance
                last_error = exc
                await self._pool.mark_failure(index)
            finally:
                await lease.release()

            if attempt < retries:
                await asyncio.sleep(0.5 * (attempt + 1))

        if isinstance(last_error, FirecrawlError):
            raise last_error
        raise FirecrawlError("firecrawl_request_failed")


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        return resp.json()
    except json.JSONDecodeError:
        return {}


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
