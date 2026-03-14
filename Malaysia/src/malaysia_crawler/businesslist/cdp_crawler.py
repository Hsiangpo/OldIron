"""通过已打开 Chrome(CDP) 抓取 BusinessList。"""

from __future__ import annotations

import time
from dataclasses import replace
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.businesslist.parser import parse_company_page

BASE_URL = "https://www.businesslist.my"
BLOCKED_TOKENS = (
    "just a moment",
    "请稍候",
    "执行安全验证",
    "challenge-platform",
    "checking your browser",
)


class BusinessListBlockedError(RuntimeError):
    """BusinessList 被 cf/风控拦截。"""


def _normalize_website_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("www."):
        return f"https://{value}"
    if value.startswith("/"):
        return urljoin(BASE_URL, value)
    return f"https://{value}"


def _resolve_website_url(website_href: str) -> str:
    href = website_href.strip()
    if not href:
        return ""
    if href.startswith("/redir/"):
        query_value = parse_qs(urlparse(href).query).get("u", [""])[0]
        return _normalize_website_url(query_value)
    return _normalize_website_url(href)


def _is_blocked_page(*, title: str, html: str) -> bool:
    merged = f"{title}\n{html}".lower()
    return any(token in merged for token in BLOCKED_TOKENS)


def _is_company_detail_page(html: str) -> bool:
    lower = html.lower()
    return 'id="company_name"' in lower or "id='company_name'" in lower


class BusinessListCDPCrawler:
    """复用本机 Chrome 调试端口抓取公司页。"""

    def __init__(
        self,
        *,
        cdp_url: str = "http://127.0.0.1:9222",
        wait_ms: int = 400,
        block_wait_seconds: float = 12.0,
        max_block_retries: int = 2,
    ) -> None:
        self.cdp_url = cdp_url.replace("http://", "").replace("https://", "").strip()
        self.wait_ms = max(wait_ms, 0)
        self.block_wait_seconds = max(block_wait_seconds, 1.0)
        self.max_block_retries = max(max_block_retries, 0)
        self._driver: WebDriver | None = None
        self._bound_window: str = ""

    def _ensure_driver(self) -> WebDriver:
        if self._driver is not None:
            return self._driver
        options = Options()
        options.add_experimental_option("debuggerAddress", self.cdp_url)
        driver = webdriver.Chrome(options=options)
        self._driver = driver
        return driver

    def _bind_businesslist_window(self, driver: WebDriver) -> None:
        if self._bound_window:
            return
        fallback = ""
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
            url = driver.current_url.lower()
            if "businesslist.my/company/" in url:
                self._bound_window = handle
                return
            if not fallback and "businesslist.my" in url:
                fallback = handle
        if fallback:
            self._bound_window = fallback
            return
        raise RuntimeError("未发现 BusinessList 页面，请先在当前 Chrome 打开 https://www.businesslist.my/company/381082")

    def _sleep_after_nav(self) -> None:
        if self.wait_ms <= 0:
            return
        time.sleep(self.wait_ms / 1000)

    def _wait_unblocked(self, driver: WebDriver) -> bool:
        deadline = time.monotonic() + self.block_wait_seconds
        while time.monotonic() < deadline:
            if not _is_blocked_page(title=driver.title, html=driver.page_source):
                return True
            time.sleep(0.8)
        return False

    def close(self) -> None:
        if self._driver is None:
            return
        self._driver.quit()
        self._driver = None
        self._bound_window = ""

    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None:
        driver = self._ensure_driver()
        self._bind_businesslist_window(driver)
        driver.switch_to.window(self._bound_window)
        target_url = f"{BASE_URL}/company/{company_id}"

        for _ in range(self.max_block_retries + 1):
            driver.get(target_url)
            self._sleep_after_nav()
            title = driver.title
            html = driver.page_source
            if _is_blocked_page(title=title, html=html):
                if not self._wait_unblocked(driver):
                    continue
                title = driver.title
                html = driver.page_source
                if _is_blocked_page(title=title, html=html):
                    continue
            if not _is_company_detail_page(html):
                return None
            parsed = parse_company_page(html, response_url=driver.current_url)
            if parsed is None:
                return None
            return replace(parsed, website_url=_resolve_website_url(parsed.website_href))
        raise BusinessListBlockedError(f"BusinessList 公司页被拦截：company_id={company_id}")
