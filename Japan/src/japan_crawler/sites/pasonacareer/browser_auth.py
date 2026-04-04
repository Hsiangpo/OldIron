"""PasonaCareer 浏览器鉴权与持久化抓取辅助。"""

from __future__ import annotations

import atexit
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode, urljoin

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


LOGGER = logging.getLogger("pasonacareer.browser")
BASE_URL = "https://www.pasonacareer.jp"
SEARCH_PATH = "/search/jl/"
_ASYNC_LOOP_HINTS = (
    "Playwright Sync API inside the asyncio loop",
    "Playwright Sync API is unavailable inside the current asyncio loop",
)
_BROWSER_FATAL_HINTS = (
    "Executable doesn't exist",
    "Please run the following command to download new browsers",
)


@dataclass(slots=True)
class BrowserAuth:
    """浏览器探活后可复用的鉴权信息。"""

    cookie_header: str
    user_agent: str


class PasonacareerPersistentBrowser:
    """复用 PersistentContext 直接抓取真实页面 HTML。"""

    def __init__(self, user_data_dir: Path, proxy_url: str = "", timeout_ms: int = 60000) -> None:
        self._user_data_dir = Path(user_data_dir)
        self._proxy_url = str(proxy_url or "").strip()
        self._timeout_ms = timeout_ms
        self._channel = str(os.getenv("PASONACAREER_BROWSER_CHANNEL") or "chrome").strip()
        self._lock = threading.Lock()
        self._playwright = None
        self._context = None
        self._page = None
        self._disabled = False
        atexit.register(self.close)

    def fetch_search_page(self, page: int = 1) -> str | None:
        if self._disabled:
            return None
        params = {"utf8": "✓", "f[f]": "1", "f[q]": ""}
        if page > 1:
            params["page"] = str(page)
        url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
        return self._fetch_html(url, ready_kind="search")

    def fetch_job_page(self, detail_url: str) -> str | None:
        if self._disabled:
            return None
        return self._fetch_html(urljoin(BASE_URL, detail_url), ready_kind="detail")

    def close(self) -> None:
        with self._lock:
            if self._page is not None and not self._page.is_closed():
                self._page.close()
            self._page = None
            if self._context is not None:
                self._context.close()
            self._context = None
            if self._playwright is not None:
                self._playwright.stop()
            self._playwright = None

    def _fetch_html(self, url: str, ready_kind: str) -> str | None:
        with self._lock:
            try:
                page = self._page_handle()
                page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                self._wait_ready(page, ready_kind)
                html = page.content()
            except PlaywrightTimeoutError as exc:
                LOGGER.warning("浏览器访问超时: %s | %s", url, exc)
                return None
            except Exception as exc:  # noqa: BLE001
                if _should_disable_browser_for_exception(exc):
                    self._disabled = True
                    LOGGER.warning("浏览器主抓取已禁用，原因: %s", exc)
                    return None
                LOGGER.warning("浏览器访问失败: %s | %s", url, exc)
                return None
        return html if self._looks_like_real_page(html, ready_kind) else None

    def _page_handle(self):
        if self._context is None:
            self._start()
        if self._page is None or self._page.is_closed():
            self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        return self._page

    def _start(self) -> None:
        self._user_data_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = sync_playwright().start()
        launch_kwargs: dict[str, object] = {
            "user_data_dir": str(self._user_data_dir),
            "headless": True,
        }
        if self._channel:
            launch_kwargs["channel"] = self._channel
        if self._proxy_url:
            launch_kwargs["proxy"] = {"server": self._proxy_url}
        self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        LOGGER.info("PasonaCareer 启动 PersistentContext: %s", self._user_data_dir)

    def _wait_ready(self, page, ready_kind: str) -> None:
        if ready_kind == "search":
            page.locator("text=検索結果一覧").first.wait_for(timeout=self._timeout_ms)
        else:
            page.locator("h1").first.wait_for(timeout=self._timeout_ms)
        page.wait_for_timeout(800)

    def _looks_like_real_page(self, page_html: str, ready_kind: str) -> bool:
        text = str(page_html or "")
        if "challenge.js" in text or "awswaf" in text.lower():
            LOGGER.warning("浏览器页面仍是 WAF challenge，等待下次重试。")
            return False
        if ready_kind == "search":
            return "検索結果一覧" in text and "/job/" in text
        return "<h1" in text

    @property
    def disabled(self) -> bool:
        return self._disabled


def fetch_browser_auth(target_url: str, proxy_url: str = "", timeout_ms: int = 30000) -> BrowserAuth:
    """通过无头浏览器访问页面，提取 WAF 通过后的 Cookie 和 UA。"""
    launch_kwargs: dict[str, object] = {"headless": True}
    channel = str(os.getenv("PASONACAREER_BROWSER_CHANNEL") or "chrome").strip()
    if channel:
        launch_kwargs["channel"] = channel
    if proxy_url:
        launch_kwargs["proxy"] = {"server": proxy_url}

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(**launch_kwargs)
            try:
                page = browser.new_page()
                page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.locator("text=検索結果一覧").first.wait_for(timeout=timeout_ms)
                page.wait_for_timeout(3000)
                cookies = page.context.cookies(target_url)
                user_agent = str(page.evaluate("() => navigator.userAgent") or "").strip()
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        if is_sync_api_asyncio_error(exc):
            raise RuntimeError("Playwright Sync API is unavailable inside the current asyncio loop") from exc
        raise

    cookie_header = "; ".join(
        f"{cookie['name']}={cookie['value']}"
        for cookie in cookies
        if str(cookie.get("name") or "").strip() and str(cookie.get("value") or "").strip()
    )
    if not cookie_header or not user_agent:
        raise RuntimeError("浏览器鉴权未拿到有效 Cookie/UA")
    return BrowserAuth(cookie_header=cookie_header, user_agent=user_agent)


def is_sync_api_asyncio_error(exc: Exception) -> bool:
    text = str(exc or "")
    return any(hint in text for hint in _ASYNC_LOOP_HINTS)


def _should_disable_browser_for_exception(exc: Exception) -> bool:
    text = str(exc or "")
    return is_sync_api_asyncio_error(exc) or any(hint in text for hint in _BROWSER_FATAL_HINTS)
