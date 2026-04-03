"""OpenWork 浏览器 profile 复用辅助。"""

from __future__ import annotations

import atexit
import logging
import os
import re
import threading
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Error, TimeoutError, sync_playwright


LOGGER = logging.getLogger("openwork.browser")
LIST_URL = "https://www.openwork.jp/company_list?field=&pref=&sort=1&src_str="
LIST_READY_SELECTOR = "ul.testCompanyList > li"
DETAIL_READY_SELECTOR = "table.definitionList-wiki tr"
_AUTH_TEXT = "画像認証"
_DEFAULT_TIMEOUT_MS = 90000
_DEFAULT_MANUAL_WAIT_SECONDS = 600
_DETAIL_PATH_RE = re.compile(r'href="(/company\.php\?m_id=[^"]+)"')


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


class OpenworkBrowserBlocked(RuntimeError):
    """OpenWork 浏览器 profile 仍被验证码拦截时抛出。"""


class OpenworkPersistentBrowser:
    """复用固定浏览器 profile 抓取 OpenWork 页面。"""

    def __init__(
        self,
        *,
        user_data_dir: Path,
        proxy_url: str = "",
        timeout_ms: int | None = None,
        manual_wait_seconds: int | None = None,
    ) -> None:
        self._user_data_dir = Path(user_data_dir)
        self._proxy_url = str(proxy_url or "").strip()
        self._timeout_ms = int(timeout_ms or _DEFAULT_TIMEOUT_MS)
        self._manual_wait_seconds = int(manual_wait_seconds or _DEFAULT_MANUAL_WAIT_SECONDS)
        self._channel = str(os.getenv("OPENWORK_BROWSER_CHANNEL") or "chrome").strip()
        self._headless = _env_bool("OPENWORK_BROWSER_HEADLESS", False)
        self._lock = threading.Lock()
        self._playwright = None
        self._context = None
        self._page = None
        self._started_headless: bool | None = None
        atexit.register(self.close)

    def prepare_manual_auth(self, target_url: str = LIST_URL, ready_selector: str = LIST_READY_SELECTOR) -> None:
        """打开可见浏览器，让人工完成列表页和详情页验证码。"""
        with self._lock:
            self._start(headless=False)
            page = self._page_handle()
            page.goto(target_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._ensure_page_ready(
                page,
                ready_selector=ready_selector,
                target_url=target_url,
                stage_label="列表页",
            )
            detail_url = self._extract_first_detail_url(page.content())
            if not detail_url:
                self._close_no_lock()
                raise OpenworkBrowserBlocked("OpenWork 列表页已打开，但未找到可用的公司详情链接。")
            page.goto(detail_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._ensure_page_ready(
                page,
                ready_selector=DETAIL_READY_SELECTOR,
                target_url=detail_url,
                stage_label="详情页",
            )
            LOGGER.info("OpenWork 浏览器 profile 已通过列表页和详情页验证：%s", self._user_data_dir)
            self._close_no_lock()

    def _ensure_page_ready(self, page, *, ready_selector: str, target_url: str, stage_label: str) -> None:
        if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
            return
        page.bring_to_front()
        LOGGER.warning(
            "OpenWork %s 当前需要人工完成图片验证码。请在打开的 Chrome 窗口中完成验证；最多等待 %d 秒。",
            stage_label,
            self._manual_wait_seconds,
        )
        deadline = time.time() + self._manual_wait_seconds
        while time.time() < deadline:
            if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
                return
            page.wait_for_timeout(2000)
        raise OpenworkBrowserBlocked(self._build_blocker_message(target_url))

    def fetch_html(self, *, url: str, ready_selector: str) -> str:
        """使用固定 profile 抓取真实页面 HTML。"""
        with self._lock:
            self._start(headless=self._headless)
            page = self._page_handle()
            page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
                return page.content()
            if not self._started_headless:
                self._ensure_page_ready(
                    page,
                    ready_selector=ready_selector,
                    target_url=url,
                    stage_label="运行页",
                )
                return page.content()
            html_text = page.content()
            raise OpenworkBrowserBlocked(self._build_blocker_message(url))

    def close(self) -> None:
        with self._lock:
            self._close_no_lock()

    def _start(self, *, headless: bool) -> None:
        if self._context is not None and self._started_headless == headless:
            return
        self._close_no_lock()
        self._user_data_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = sync_playwright().start()
        launch_kwargs: dict[str, object] = {
            "user_data_dir": str(self._user_data_dir),
            "headless": headless,
        }
        if self._channel:
            launch_kwargs["channel"] = self._channel
        if self._proxy_url:
            launch_kwargs["proxy"] = {"server": self._proxy_url}
        self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        self._started_headless = headless
        LOGGER.info("OpenWork 启动浏览器 profile：%s | headless=%s", self._user_data_dir, headless)

    def _page_handle(self):
        if self._context is None:
            raise OpenworkBrowserBlocked("OpenWork 浏览器上下文未初始化。")
        if self._page is None or self._page.is_closed():
            self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        return self._page

    def _close_no_lock(self) -> None:
        if self._page is not None and not self._page.is_closed():
            self._page.close()
        self._page = None
        if self._context is not None:
            self._context.close()
        self._context = None
        if self._playwright is not None:
            self._playwright.stop()
        self._playwright = None
        self._started_headless = None

    def _wait_ready(self, page, ready_selector: str) -> bool:
        try:
            page.locator(ready_selector).first.wait_for(timeout=5000)
            return True
        except TimeoutError:
            return False
        except Error:
            return False

    def _looks_like_real_page(self, page_html: str) -> bool:
        text = str(page_html or "")
        lowered = text.lower()
        if _AUTH_TEXT in text:
            return False
        if "<title>403 forbidden" in lowered:
            return False
        if "g-recaptcha" in lowered:
            return False
        return True

    def _extract_first_detail_url(self, page_html: str) -> str:
        matched = _DETAIL_PATH_RE.search(str(page_html or ""))
        if matched is None:
            return ""
        return urljoin(LIST_URL, str(matched.group(1) or "").strip())

    def _build_blocker_message(self, url: str) -> str:
        return (
            "OpenWork 当前仍停在验证码/403 页面，无法继续抓取。"
            f" 目标地址: {url}。"
            " 请先执行 `cd Japan && .venv/bin/python run.py openwork auth`，"
            "在打开的 Chrome 窗口里人工完成一次图片验证码，之后再重新运行站点命令。"
        )
