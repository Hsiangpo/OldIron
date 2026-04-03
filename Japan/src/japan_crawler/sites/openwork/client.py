"""OpenWork HTTP 客户端。"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from curl_cffi.requests import Session

from .browser_profile import DETAIL_READY_SELECTOR, LIST_READY_SELECTOR, OpenworkPersistentBrowser


LOGGER = logging.getLogger("openwork.client")
BASE_URL = "https://www.openwork.jp"
LIST_PATH = "/company_list"
DEFAULT_PER_PAGE = 50
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
    "画像認証",
    "<title>画像認証",
    "openwork,画像認証",
    "g-recaptcha",
)


@dataclass(slots=True)
class _HtmlResponse:
    """统一协议页与浏览器页的返回形态。"""

    status_code: int
    text: str


class OpenworkClient:
    """OpenWork 列表页与公司详情页抓取客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.2,
        max_retries: int = 3,
        proxy: str | None = None,
        browser_profile_dir: str | Path | None = None,
    ) -> None:
        self._delay = request_delay
        self._max_retries = max_retries
        self._proxy_url = str(proxy or os.getenv("HTTP_PROXY", "")).strip()
        self._local = threading.local()
        self._proxy_cooldown_until = 0.0
        self._browser_lock = threading.Lock()
        self._browser_mode = os.getenv("OPENWORK_FORCE_BROWSER", "").strip() == "1"
        self._browser_notice_logged = False
        self._browser_client = None
        if browser_profile_dir is not None:
            self._browser_client = OpenworkPersistentBrowser(
                user_data_dir=Path(browser_profile_dir),
                proxy_url=self._proxy_url,
            )
        if self._proxy_url:
            LOGGER.info("使用代理: %s", self._proxy_url)
        self._base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{BASE_URL}{LIST_PATH}",
        }
        self._request_count = 0
        self._error_count = 0

    def fetch_list_page(self, page: int = 1) -> str | None:
        """抓取公司列表页。"""
        params = {
            "field": "",
            "pref": "",
            "src_str": "",
            "sort": "1",
        }
        if page > 1:
            params["next_page"] = str(page)
        response = self._get_with_retry(f"{BASE_URL}{LIST_PATH}", params=params)
        return response.text if response is not None else None

    def fetch_detail_page(self, detail_url: str) -> str | None:
        """抓取公司详情页。"""
        absolute = urljoin(BASE_URL, detail_url)
        response = self._get_with_retry(absolute)
        return response.text if response is not None else None

    def _get_with_retry(self, url: str, params: dict[str, str] | None = None) -> Any:
        if self._browser_mode:
            return self._browser_response(url)
        for attempt in range(self._max_retries):
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
                        return self._browser_response(url)
                    if response.status_code == 200:
                        return response
                    if response.status_code == 429:
                        self._sleep_backoff(attempt, 4.0, 8.0, "429 限流")
                        break
                    if response.status_code == 403:
                        self._error_count += 1
                        LOGGER.warning("403 禁止访问，切换浏览器复用：%s", url)
                        return self._browser_response(url)
                    if response.status_code >= 500:
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
                    self._sleep_backoff(attempt, 2.0, 4.0, "网络异常重试")
                    break
        LOGGER.error("重试耗尽: %s", url)
        return None

    def _browser_response(self, url: str) -> _HtmlResponse:
        if self._browser_client is None:
            raise RuntimeError("OpenWork 浏览器 profile 未配置。")
        with self._browser_lock:
            self._browser_mode = True
            if not self._browser_notice_logged:
                LOGGER.warning("OpenWork 已切换到浏览器详情补抓模式，后续详情页会逐条处理，速度会慢一些。")
                self._browser_notice_logged = True
            html_text = self._browser_client.fetch_html(url=url, ready_selector=self._ready_selector(url))
            self._request_count += 1
            return _HtmlResponse(status_code=200, text=html_text)

    def _ready_selector(self, url: str) -> str:
        if "/company_list" in url:
            return LIST_READY_SELECTOR
        return DETAIL_READY_SELECTOR

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
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code not in {200, 202, 403}:
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
        return self._browser_client is not None and self._browser_mode

    @property
    def browser_enabled(self) -> bool:
        return self._browser_client is not None
