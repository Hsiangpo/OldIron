"""Italy DNB 列表客户端。"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from curl_cffi import CurlHttpVersion
from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright

from oldiron_core.dnb_cookie_cache import load_dnb_cookie_snapshot
from oldiron_core.dnb_cookie_cache import save_dnb_cookie_snapshot


LOGGER = logging.getLogger(__name__)
LIST_API_URL = "https://www.dnb.com/business-directory/api/companyinformation"
_REPO_ROOT = Path(__file__).resolve().parents[5]
_DNB_REQUEST_RETRIES = 4
_RETRYABLE_CURL_HINTS = (
    "curl: (92)",
    "http/2 stream",
    "internal_error",
    "curl: (35)",
    "tls connect error",
    "curl: (28)",
    "timed out",
)


@dataclass(slots=True)
class DnbListPage:
    current_page: int
    total_pages: int
    page_size: int
    country_name: str
    industry_name: str
    matched_count: int
    geos: list[dict[str, str | int]]
    records: list[dict[str, str]]


@dataclass(slots=True)
class DnbBrowserHeaders:
    user_agent: str
    sec_ch_ua: str
    sec_ch_ua_platform: str
    accept_language: str


def _to_int(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    return int(text) if text.isdigit() else 0


def _safe_text(value: object) -> str:
    return unescape(str(value or "").strip()).replace("\xa0", " ")


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _default_dnb_cookie_cache_file() -> Path:
    configured = str(os.getenv("DNB_COOKIE_CACHE_FILE", "") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return _REPO_ROOT / "output" / "cache" / "dnb_akamai_cookie.json"


def _list_page_url(
    industry_path: str,
    country_code: str,
    region_name: str = "",
    city_name: str = "",
    page_number: int = 1,
) -> str:
    path = f"https://www.dnb.com/business-directory/company-information.{industry_path}.{country_code}"
    if region_name:
        path += f".{region_name}"
    if city_name:
        path += f".{city_name}"
    url = f"{path}.html"
    if int(page_number or 1) > 1:
        return f"{url}?page={int(page_number)}"
    return url


def parse_companyinformation_payload(payload: dict[str, Any], industry_path: str) -> DnbListPage:
    """解析 DNB 列表 API 返回。"""
    geos: list[dict[str, str | int]] = []
    for item in payload.get("companyInformationGeos", []) or []:
        if not isinstance(item, dict):
            continue
        geos.append(
            {
                "name": _safe_text(item.get("name", "")),
                "href": str(item.get("href", "") or "").strip(),
                "quantity": _to_int(item.get("quantity")),
            }
        )

    records: list[dict[str, str]] = []
    for item in payload.get("companyInformationCompany", []) or []:
        if not isinstance(item, dict):
            continue
        primary_address = _as_dict(item.get("primaryAddress"))
        street_address = _as_dict(primary_address.get("streetAddress"))
        records.append(
            {
                "duns": str(item.get("duns", "") or "").strip(),
                "company_name": _safe_text(item.get("primaryName", "")),
                "country_code": str(item.get("addressCountryIsoAlphaTwoCode", "") or "").strip(),
                "country_name": _safe_text(item.get("addressCountryName", "")),
                "region": _safe_text(item.get("addressRegionNameFormatted", "")),
                "city": _safe_text(item.get("addressLocalityNameFormatted", "")),
                "postal_code": str(primary_address.get("postalCode", "") or "").strip(),
                "address": _safe_text(street_address.get("line1", "")),
                "industry_path": industry_path,
            }
        )

    return DnbListPage(
        current_page=_to_int(payload.get("currentPageNumber")),
        total_pages=_to_int(payload.get("totalPages")),
        page_size=_to_int(payload.get("pageSize")),
        country_name=_safe_text(payload.get("countryMapValue", "")),
        industry_name=_safe_text(payload.get("industryName", "")),
        matched_count=_to_int(payload.get("candidatesMatchedQuantityInt") or payload.get("candidatesMatchedQuantity")),
        geos=geos,
        records=records,
    )


class DnbBrowserCookieProvider:
    """获取 DNB 列表访问 cookie。"""

    def __init__(self, cdp_url: str = "http://127.0.0.1:9222") -> None:
        self._cdp_url = cdp_url
        self._cookie_source = str(os.getenv("DNB_COOKIE_SOURCE", "launch") or "launch").strip().lower()
        self._seed_url = str(os.getenv("DNB_COOKIE_SEED_URL", "https://www.dnb.com/") or "").strip() or "https://www.dnb.com/"
        self._launch_timeout_ms = int(float(os.getenv("DNB_COOKIE_TIMEOUT_SECONDS", "30")) * 1000)
        self._launch_wait_ms = int(float(os.getenv("DNB_COOKIE_WAIT_SECONDS", "2.5")) * 1000)
        self._snapshot_ttl_seconds = max(float(os.getenv("DNB_COOKIE_CACHE_SECONDS", "2592000")), 0.0)
        self._cache_file = _default_dnb_cookie_cache_file()
        self._snapshot_lock = threading.Lock()
        self._snapshot_cookies: list[dict[str, str]] = []
        self._snapshot_headers: DnbBrowserHeaders | None = None
        self._snapshot_expire_at = 0.0

    def fetch_cookies(self, domain_keyword: str = "dnb.com") -> list[dict[str, str]]:
        cookies, _headers = self.fetch_snapshot(domain_keyword=domain_keyword)
        return list(cookies)

    def fetch_browser_headers(self) -> DnbBrowserHeaders:
        _cookies, headers = self.fetch_snapshot()
        return headers

    def fetch_snapshot(
        self,
        domain_keyword: str = "dnb.com",
        *,
        force: bool = False,
    ) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        with self._snapshot_lock:
            now = time.time()
            if (
                not force
                and self._snapshot_headers is not None
                and self._snapshot_cookies
                and now < self._snapshot_expire_at
            ):
                return list(self._snapshot_cookies), self._snapshot_headers
            if not force:
                cached = load_dnb_cookie_snapshot(
                    self._cache_file,
                    max_age_seconds=self._snapshot_ttl_seconds,
                )
                if cached is not None:
                    cookies = list(cached.get("cookies") or [])
                    headers = self._headers_from_cache(dict(cached.get("headers") or {}))
                    if cookies and headers is not None:
                        self._snapshot_cookies = cookies
                        self._snapshot_headers = headers
                        self._snapshot_expire_at = now + self._snapshot_ttl_seconds
                        return list(cookies), headers
            cookies, headers = self._fetch_live_snapshot(domain_keyword)
            self._snapshot_cookies = list(cookies)
            self._snapshot_headers = headers
            self._snapshot_expire_at = now + self._snapshot_ttl_seconds
            save_dnb_cookie_snapshot(
                self._cache_file,
                cookies=self._snapshot_cookies,
                headers=self._headers_to_cache(headers),
            )
            return list(cookies), headers

    def _fetch_live_snapshot(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        if self._cookie_source == "cdp":
            return self._fetch_snapshot_via_cdp(domain_keyword)
        if self._cookie_source == "http":
            return self._fetch_snapshot_via_http(domain_keyword)
        try:
            return self._fetch_snapshot_via_launch(domain_keyword)
        except Exception:
            LOGGER.warning("DNB 浏览器抓 cookie 失败，回退到 HTTP 种子")
            return self._fetch_snapshot_via_http(domain_keyword)

    def _headers_from_cache(self, payload: dict[str, str]) -> DnbBrowserHeaders | None:
        user_agent = str(payload.get("user_agent", "") or "").strip()
        if not user_agent:
            return None
        return DnbBrowserHeaders(
            user_agent=user_agent,
            sec_ch_ua=str(payload.get("sec_ch_ua", "") or "").strip(),
            sec_ch_ua_platform=str(payload.get("sec_ch_ua_platform", "") or "").strip(),
            accept_language=str(payload.get("accept_language", "en-US,en;q=0.9") or "en-US,en;q=0.9").strip(),
        )

    def _headers_to_cache(self, headers: DnbBrowserHeaders) -> dict[str, str]:
        return {
            "user_agent": headers.user_agent,
            "sec_ch_ua": headers.sec_ch_ua,
            "sec_ch_ua_platform": headers.sec_ch_ua_platform,
            "accept_language": headers.accept_language,
        }

    def _fetch_snapshot_via_cdp(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(self._browser_ws_url(), timeout=10000)
            try:
                cookies: list[dict[str, str]] = []
                for context in browser.contexts:
                    for item in context.cookies():
                        if domain_keyword in str(item.get("domain", "")):
                            cookies.append(item)
                return cookies, self._build_headers_from_version(self._browser_version_payload())
            finally:
                browser.close()

    def _fetch_snapshot_via_launch(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        with sync_playwright() as playwright:
            browser = self._launch_browser(playwright)
            try:
                context = browser.new_context(ignore_https_errors=True)
                page = context.new_page()
                page.goto(self._seed_url, wait_until="domcontentloaded", timeout=self._launch_timeout_ms)
                page.wait_for_timeout(self._launch_wait_ms)
                user_agent = page.evaluate("() => navigator.userAgent")
                cookies = [
                    item
                    for item in context.cookies()
                    if domain_keyword in str(item.get("domain", ""))
                ]
                return cookies, self._build_headers_from_user_agent(user_agent)
            finally:
                browser.close()

    def _fetch_snapshot_via_http(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        session = cffi_requests.Session(impersonate="chrome124")
        session.trust_env = False
        session.headers.update({"Accept-Language": "en-US,en;q=0.9"})
        try:
            response = session.get(self._seed_url, timeout=20, verify=False)
            response.raise_for_status()
            cookies: list[dict[str, str]] = []
            for cookie in session.cookies.jar:
                if domain_keyword not in str(cookie.domain or ""):
                    continue
                cookies.append(
                    {
                        "name": cookie.name,
                        "value": cookie.value,
                        "domain": cookie.domain,
                        "path": cookie.path or "/",
                    }
                )
            if cookies:
                return cookies, self._build_headers("Mozilla/5.0", "Chromium")
        finally:
            session.close()
        raise RuntimeError("DNB HTTP cookie seed failed")

    def _launch_browser(self, playwright: Any) -> Any:
        channel = str(os.getenv("DNB_COOKIE_BROWSER_CHANNEL", "chrome") or "chrome").strip()
        headless = str(os.getenv("DNB_COOKIE_HEADLESS", "0") or "0").strip() not in {"0", "false", "False"}
        launch_args = {
            "headless": headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        try:
            return playwright.chromium.launch(channel=channel, **launch_args)
        except Exception:
            return playwright.chromium.launch(**launch_args)

    def _browser_ws_url(self) -> str:
        version = json.loads(urllib.request.urlopen(f"{self._cdp_url}/json/version", timeout=5).read().decode())
        return str(version["webSocketDebuggerUrl"])

    def _browser_version_payload(self) -> dict[str, Any]:
        return json.loads(urllib.request.urlopen(f"{self._cdp_url}/json/version", timeout=5).read().decode())

    def _build_headers_from_version(self, version: dict[str, Any]) -> DnbBrowserHeaders:
        user_agent = str(version.get("User-Agent", "") or "").strip() or "Mozilla/5.0"
        return self._build_headers_from_user_agent(user_agent)

    def _build_headers_from_user_agent(self, user_agent: str) -> DnbBrowserHeaders:
        browser_label = "Chromium"
        if "Chrome/" in user_agent:
            browser_label = "Google Chrome"
        return self._build_headers(user_agent, browser_label)

    def _build_headers(self, user_agent: str, browser_label: str) -> DnbBrowserHeaders:
        major = "124"
        if "Chrome/" in user_agent:
            major = user_agent.split("Chrome/", 1)[1].split(".", 1)[0]
        return DnbBrowserHeaders(
            user_agent=user_agent,
            sec_ch_ua=f'"Not.A/Brand";v="8", "{browser_label}";v="{major}", "Chromium";v="{major}"',
            sec_ch_ua_platform='"macOS"',
            accept_language="en-US,en;q=0.9",
        )


class DnbCompanyInformationClient:
    """带浏览器 cookie 的 DNB 列表客户端。"""

    def __init__(self, cookie_provider: DnbBrowserCookieProvider | None = None) -> None:
        self._cookie_provider = cookie_provider or DnbBrowserCookieProvider()
        self._session_lock = threading.Lock()
        self._cookies: list[dict[str, str]] = []
        self._browser_headers: DnbBrowserHeaders | None = None
        self._forced_refresh_cooldown_seconds = max(
            float(os.getenv("DNB_COOKIE_REFRESH_MIN_SECONDS", "120") or "120"),
            0.0,
        )
        self._last_forced_refresh_at = 0.0

    def close(self) -> None:
        return None

    def refresh_cookies(self, *, force: bool = True) -> bool:
        now = time.monotonic()
        with self._session_lock:
            if (
                force
                and self._last_forced_refresh_at > 0
                and now - self._last_forced_refresh_at < self._forced_refresh_cooldown_seconds
            ):
                return False
        cookies, headers = self._cookie_provider.fetch_snapshot(force=force)
        with self._session_lock:
            self._cookies = list(cookies)
            self._browser_headers = headers
            if force:
                self._last_forced_refresh_at = now
        return True

    def fetch_page(
        self,
        industry_path: str,
        page_number: int,
        country_code: str = "it",
        region_name: str = "",
        city_name: str = "",
    ) -> DnbListPage:
        payload: dict[str, object] = {
            "pageNumber": int(page_number),
            "industryPath": industry_path,
            "countryIsoTwoCode": country_code,
        }
        if str(region_name or "").strip():
            payload["regionName"] = str(region_name).strip()
        if str(city_name or "").strip():
            payload["cityName"] = str(city_name).strip()
        result = self._request_json_with_retries(
            method="POST",
            url=LIST_API_URL,
            json_payload=payload,
            timeout=30,
            headers=self._list_headers(
                _list_page_url(industry_path, country_code, region_name, city_name, int(page_number))
            ),
        )
        return parse_companyinformation_payload(result, industry_path)

    def _get_cookies(self) -> list[dict[str, str]]:
        with self._session_lock:
            if not self._cookies:
                self._cookies = self._cookie_provider.fetch_cookies()
            return list(self._cookies)

    def _get_browser_headers(self) -> DnbBrowserHeaders:
        with self._session_lock:
            if self._browser_headers is None:
                self._browser_headers = self._cookie_provider.fetch_browser_headers()
            return self._browser_headers

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(impersonate="chrome124")
        session.trust_env = False
        session.http_version = CurlHttpVersion.V2_0
        for cookie in self._get_cookies():
            domain = str(cookie.get("domain") or "www.dnb.com").lstrip(".")
            session.cookies.set(
                name=str(cookie.get("name") or ""),
                value=str(cookie.get("value") or ""),
                domain=domain,
                path=str(cookie.get("path") or "/"),
            )
        return session

    def _request_json_with_retries(
        self,
        *,
        method: str,
        url: str,
        json_payload: dict[str, object] | None,
        timeout: int,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(_DNB_REQUEST_RETRIES):
            session = self._build_session()
            try:
                response = session.request(
                    method=method,
                    url=url,
                    json=json_payload,
                    headers=headers,
                    timeout=timeout,
                    verify=False,
                )
                if response.status_code == 403 and attempt < _DNB_REQUEST_RETRIES - 1:
                    LOGGER.warning("DNB 触发 403，强制刷新 cookie 后重试")
                    self.refresh_cookies(force=True)
                    continue
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= _DNB_REQUEST_RETRIES - 1 or not _looks_retryable(exc):
                    break
                time.sleep(0.8 * (attempt + 1))
            finally:
                session.close()
        if last_error is not None:
            raise last_error
        raise RuntimeError("DNB request failed without exception")

    def _list_headers(self, referer_url: str) -> dict[str, str]:
        browser_headers = self._get_browser_headers()
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": browser_headers.accept_language,
            "Origin": "https://www.dnb.com",
            "Priority": "u=1, i",
            "Referer": referer_url,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": browser_headers.user_agent,
            "sec-ch-ua": browser_headers.sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": browser_headers.sec_ch_ua_platform,
        }


def _looks_retryable(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(hint in message for hint in _RETRYABLE_CURL_HINTS)
