"""Firecrawl 同步客户端。"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urljoin

import requests

from .key_pool import FirecrawlKeyPool
from .key_pool import KeyLease


class FirecrawlError(RuntimeError):
    """Firecrawl 请求异常。"""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message or code)
        self.code = code
        self.retry_after = retry_after


@dataclass(slots=True)
class FirecrawlClientConfig:
    base_url: str
    timeout_seconds: float = 45.0
    max_retries: int = 2
    only_main_content: bool = True
    map_limit: int = 8000


def _safe_json(resp: requests.Response) -> dict[str, object]:
    try:
        payload = resp.json()
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_retry_after(resp: requests.Response) -> float | None:
    raw = str(resp.headers.get("Retry-After", "")).strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


class FirecrawlClient:
    """基于 Key 池的 Firecrawl 请求器。"""

    def __init__(self, *, key_pool: FirecrawlKeyPool, config: FirecrawlClientConfig) -> None:
        self._key_pool = key_pool
        self._config = config
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def map_urls(self, url: str, *, include_subdomains: bool = True) -> list[str]:
        payload = {
            "url": url,
            "limit": self._config.map_limit,
            "includeSubdomains": include_subdomains,
            "sitemap": "include",
        }
        data = self._request_json("POST", "map", json_body=payload)
        return self._extract_urls(data)

    def scrape_page(self, url: str) -> dict[str, str]:
        payload = {
            "url": url,
            "formats": ["markdown", "rawHtml"],
            "onlyMainContent": self._config.only_main_content,
            "timeout": int(max(self._config.timeout_seconds, 5.0) * 1000),
            "removeBase64Images": True,
        }
        data = self._request_json("POST", "scrape", json_body=payload)
        body = data.get("data", data)
        if not isinstance(body, dict):
            return {"url": url, "markdown": "", "raw_html": ""}
        markdown = str(body.get("markdown", "") or "")
        raw_html = str(body.get("rawHtml", "") or "")
        source_url = str(body.get("metadata", {}) or "")
        if source_url and source_url.startswith("http"):
            real_url = source_url
        else:
            real_url = url
        return {"url": real_url, "markdown": markdown, "raw_html": raw_html}

    def _extract_urls(self, payload: dict[str, object]) -> list[str]:
        candidates: list[str] = []
        for key in ("links", "urls"):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        candidates.append(item.strip())
        data = payload.get("data")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str) and item.strip():
                    candidates.append(item.strip())
        elif isinstance(data, dict):
            for key in ("links", "urls"):
                value = data.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str) and item.strip():
                            candidates.append(item.strip())
        # 中文注释：保持顺序去重。
        return list(dict.fromkeys(candidates))

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, object] | None = None,
    ) -> dict[str, object]:
        attempts = max(self._config.max_retries, 0) + 1
        last_error: Exception | None = None
        for idx in range(attempts):
            try:
                lease = self._key_pool.acquire()
            except RuntimeError as exc:
                raise FirecrawlError("firecrawl_key_unavailable", str(exc)) from exc
            try:
                return self._request_once(lease, method, path, json_body=json_body)
            except FirecrawlError as exc:
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self._key_pool.mark_failure(lease)
            finally:
                self._key_pool.release(lease)
            if idx < attempts - 1:
                time.sleep(0.6 * (idx + 1))
        if isinstance(last_error, FirecrawlError):
            raise last_error
        raise FirecrawlError("firecrawl_request_failed", str(last_error or "unknown"))

    def _request_once(
        self,
        lease: KeyLease,
        method: str,
        path: str,
        *,
        json_body: dict[str, object] | None = None,
    ) -> dict[str, object]:
        url = urljoin(self._config.base_url, path)
        headers = {"Authorization": f"Bearer {lease.key}"}
        resp = self._session.request(
            method=method,
            url=url,
            headers=headers,
            json=json_body,
            timeout=max(self._config.timeout_seconds, 5.0),
        )
        status = int(resp.status_code)
        if status == 200:
            self._key_pool.mark_success(lease)
            return _safe_json(resp)
        if status == 401:
            self._key_pool.disable(lease, "unauthorized")
            raise FirecrawlError("firecrawl_401")
        if status == 402:
            self._key_pool.disable(lease, "payment_required")
            raise FirecrawlError("firecrawl_402")
        if status == 429:
            retry_after = _parse_retry_after(resp)
            self._key_pool.mark_rate_limited(lease, retry_after)
            raise FirecrawlError("firecrawl_429", retry_after=retry_after)
        if status >= 500:
            self._key_pool.mark_failure(lease)
            raise FirecrawlError("firecrawl_5xx")
        self._key_pool.mark_failure(lease)
        raise FirecrawlError(f"firecrawl_http_{status}")
