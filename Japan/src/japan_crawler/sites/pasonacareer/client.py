"""PasonaCareer HTTP 客户端。"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from curl_cffi.requests import Session


LOGGER = logging.getLogger("pasonacareer.client")
BASE_URL = "https://www.pasonacareer.jp"
SEARCH_PATH = "/search/jl/"
_PROXY_FALLBACK_ERROR_HINTS = (
    "curl: (35)",
    "tls connect error",
    "invalid library",
    "curl: (7)",
    "failed to connect to 127.0.0.1",
)
_PROXY_FALLBACK_RESPONSE_HINTS = (
    "awswaf",
    "challenge.js",
    "verify that you're not a robot",
    "javascript is disabled",
)


class PasonacareerClient:
    """PasonaCareer 搜索结果页与职位详情页抓取客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.0,
        max_retries: int = 3,
        proxy: str | None = None,
        browser_profile_dir: str | Path | None = None,
    ) -> None:
        _ = browser_profile_dir
        self._delay = request_delay
        self._max_retries = max_retries
        self._proxy_url = str(proxy or os.getenv("HTTP_PROXY", "")).strip()
        self._local = threading.local()
        self._proxy_cooldown_until = 0.0
        if self._proxy_url:
            LOGGER.info("使用代理: %s", self._proxy_url)
        self._base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{BASE_URL}{SEARCH_PATH}",
        }
        self._request_count = 0
        self._error_count = 0

    def fetch_search_page(self, page: int = 1, *, filters: dict[str, str] | None = None) -> str | None:
        params = {"utf8": "✓", "f[f]": "1", "f[q]": ""}
        if filters:
            params.update(filters)
        if page > 1:
            params["page"] = str(page)
        response = self._get_with_retry(f"{BASE_URL}{SEARCH_PATH}", params=params)
        return response.text if response is not None else None

    def fetch_job_page(self, detail_url: str) -> str | None:
        response = self._get_with_retry(urljoin(BASE_URL, detail_url), max_retries=1, fast_fail=True)
        return response.text if response is not None else None

    def _get_with_retry(
        self,
        url: str,
        params: dict[str, str] | None = None,
        *,
        max_retries: int | None = None,
        fast_fail: bool = False,
    ) -> Any:
        retry_count = max(int(max_retries or self._max_retries), 1)
        for attempt in range(retry_count):
            for use_proxy in self._request_modes():
                try:
                    self._polite_delay()
                    response = self._session(use_proxy).get(url, params=params, timeout=30)
                    self._request_count += 1
                    if self._should_fallback_direct_from_response(response):
                        if use_proxy:
                            self._disable_proxy_temporarily(f"挑战页: {url}")
                            LOGGER.warning("代理路径触发挑战页/异常状态，回退直连：%s", url)
                            continue
                        if fast_fail:
                            return None
                        self._sleep_backoff(attempt, 4.0, 8.0, "命中挑战页")
                        break
                    if response.status_code == 200:
                        return response
                    if response.status_code == 429:
                        if fast_fail:
                            return None
                        self._sleep_backoff(attempt, 4.0, 8.0, "429 限流")
                        break
                    if response.status_code == 403:
                        self._error_count += 1
                        if fast_fail:
                            return None
                        LOGGER.error("403 禁止访问: %s", url)
                        return None
                    if response.status_code >= 500:
                        if fast_fail:
                            return None
                        self._sleep_backoff(attempt, 2.0, 5.0, f"{response.status_code} 服务端错误")
                        break
                    LOGGER.warning("HTTP %d: %s", response.status_code, url)
                    return None
                except Exception as exc:  # noqa: BLE001
                    self._error_count += 1
                    if use_proxy and self._should_fallback_direct_from_exception(exc):
                        self._disable_proxy_temporarily(f"代理异常: {url}")
                        LOGGER.warning("代理路径异常，回退直连：%s | %s", url, exc)
                        continue
                    LOGGER.warning("请求异常: %s", exc)
                    if fast_fail:
                        return None
                    self._sleep_backoff(attempt, 2.0, 4.0, "网络异常重试")
                    break
        LOGGER.error("重试耗尽: %s", url)
        return None

    def _request_modes(self) -> list[bool]:
        if not self._proxy_url:
            return [False]
        if time.time() < self._proxy_cooldown_until:
            return [False]
        return [True, False]

    def _session(self, use_proxy: bool) -> Session:
        attr = "proxy_session" if use_proxy else "direct_session"
        session = getattr(self._local, attr, None)
        if session is None:
            session = Session(impersonate="chrome120")
            if use_proxy and self._proxy_url:
                session.proxies = {"http": self._proxy_url, "https": self._proxy_url}
            session.headers.update(self._base_headers)
            setattr(self._local, attr, session)
        return session

    def _disable_proxy_temporarily(self, reason: str) -> None:
        if not self._proxy_url:
            return
        self._proxy_cooldown_until = max(self._proxy_cooldown_until, time.time() + 120)
        session = getattr(self._local, "proxy_session", None)
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass
            delattr(self._local, "proxy_session")
        LOGGER.warning("代理暂时停用 120 秒：%s", reason)

    def _should_fallback_direct_from_exception(self, exc: Exception) -> bool:
        text = str(exc or "").lower()
        return any(hint in text for hint in _PROXY_FALLBACK_ERROR_HINTS)

    def _should_fallback_direct_from_response(self, response: Any) -> bool:
        if int(getattr(response, "status_code", 0) or 0) not in {202, 403}:
            return False
        text = str(getattr(response, "text", "") or "").lower()
        return any(hint in text for hint in _PROXY_FALLBACK_RESPONSE_HINTS)

    def _polite_delay(self) -> None:
        time.sleep(self._delay + random.uniform(0.2, 0.6))

    def _sleep_backoff(self, attempt: int, base: float, jitter: float, label: str) -> None:
        wait = base * (attempt + 1) + random.uniform(0.5, jitter)
        LOGGER.warning("%s，等待 %.1fs", label, wait)
        time.sleep(wait)

    @property
    def stats(self) -> dict[str, int]:
        return {"requests": self._request_count, "errors": self._error_count}

    @property
    def browser_primary(self) -> bool:
        return False
