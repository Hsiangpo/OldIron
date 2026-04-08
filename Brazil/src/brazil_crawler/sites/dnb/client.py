"""DNB 巴西站点协议客户端。"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.parse import urlparse

from curl_cffi import CurlHttpVersion
from curl_cffi import requests as cffi_requests
from oldiron_core.dnb_cookie_cache import load_dnb_cookie_snapshot
from oldiron_core.dnb_cookie_cache import save_dnb_cookie_snapshot
from playwright.sync_api import sync_playwright


LOGGER = logging.getLogger(__name__)
LIST_API_URL = "https://www.dnb.com/business-directory/api/companyinformation"
DETAIL_API_URL = "https://www.dnb.com/business-directory/api/companyprofile"
DETAIL_URL_TEMPLATE = (
    "https://www.dnb.com/business-directory/company-profiles.{company_name_url}.html"
)
_DNB_REQUEST_RETRIES = 4
_REPO_ROOT = Path(__file__).resolve().parents[5]
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


@dataclass(slots=True)
class DnbDetailProfile:
    company_name: str
    representative: str
    website: str
    phone: str
    address: str
    city: str
    region: str
    postal_code: str


def _to_int(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    return int(text) if text.isdigit() else 0


def _safe_text(value: object) -> str:
    return unescape(str(value or "").strip()).replace("\xa0", " ")


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _clean_masked_text(value: object) -> str:
    text = _safe_text(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")
    text = " ".join(text.split())
    compact = text.replace("?", "").replace(" ", "").strip()
    if not compact or not any(ch.isalnum() for ch in compact):
        return ""
    return text


def _normalize_website(value: object) -> str:
    website = _clean_masked_text(value)
    if not website:
        return ""
    if website.startswith(("http://", "https://")):
        return website
    return f"https://{website.lstrip('/')}"


def _first_real_contact_name(contacts: object) -> str:
    if not isinstance(contacts, list):
        return ""
    for item in contacts:
        if not isinstance(item, Mapping):
            continue
        name = _clean_masked_text(item.get("name"))
        if not name or name.lower().startswith("contact "):
            continue
        return name
    return ""


def _companyprofile_api_path(detail_url: str) -> str:
    parsed = urlparse(detail_url)
    path = parsed.path.rstrip("/")
    if path.endswith(".html"):
        path = path[:-5]
    return f"https://www.dnb.com{path}"


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
    """解析 DNB 列表 API 的返回。"""
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
        slug = str(item.get("companyNameUrl", "") or "").strip()
        detail_url = ""
        if slug:
            detail_url = DETAIL_URL_TEMPLATE.format(company_name_url=slug)
        records.append(
            {
                "duns": str(item.get("duns", "") or "").strip(),
                "company_name": _safe_text(item.get("primaryName", "")),
                "company_name_url": slug,
                "detail_url": detail_url,
                "country_code": str(item.get("addressCountryIsoAlphaTwoCode", "") or "").strip(),
                "country_name": _safe_text(item.get("addressCountryName", "")),
                "region": _safe_text(item.get("addressRegionNameFormatted", "")),
                "city": _safe_text(item.get("addressLocalityNameFormatted", "")),
                "postal_code": str(primary_address.get("postalCode", "") or "").strip(),
                "address": _safe_text(street_address.get("line1", "")),
                "sales_revenue": str(item.get("salesRevenue", "") or "").strip(),
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


def parse_companyprofile_payload(payload: dict[str, Any]) -> DnbDetailProfile:
    """解析 DNB 详情 JSON。"""
    overview = payload.get("overview") or {}
    header = payload.get("header") or {}
    header_params = header.get("companyNewHeaderParameter") or {}
    company_cookie = header_params.get("companyInformationForCookie") or {}
    contacts = payload.get("contacts") or {}
    representative = _clean_masked_text(overview.get("keyPrincipal"))
    if not representative:
        representative = _first_real_contact_name(contacts.get("contacts"))
    return DnbDetailProfile(
        company_name=_clean_masked_text(
            overview.get("primaryName")
            or header.get("companyName")
            or company_cookie.get("companyName")
        ),
        representative=representative,
        website=_normalize_website(overview.get("website") or header.get("companyWebsiteUrl")),
        phone=_clean_masked_text(overview.get("phone")),
        address=_clean_masked_text(company_cookie.get("companyAddress")),
        city=_clean_masked_text(company_cookie.get("companyCity")),
        region=_clean_masked_text(company_cookie.get("companyState")),
        postal_code=_clean_masked_text(company_cookie.get("companyZip")),
    )


class DnbBrowserCookieProvider:
    """获取 DNB cookie，默认临时启动浏览器，不依赖 9222。"""

    def __init__(self, cdp_url: str = "http://127.0.0.1:9222") -> None:
        self._cdp_url = cdp_url
        self._cookie_source = str(os.getenv("DNB_COOKIE_SOURCE", "launch") or "launch").strip().lower()
        self._seed_url = str(
            os.getenv("DNB_COOKIE_SEED_URL", "https://www.dnb.com/") or ""
        ).strip() or "https://www.dnb.com/"
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
                        return list(self._snapshot_cookies), self._snapshot_headers
            if self._cookie_source == "cdp":
                LOGGER.info("DNB 获取 cookie 快照：source=cdp force=%s", force)
                cookies, headers = self._fetch_snapshot_via_cdp(domain_keyword)
            elif self._cookie_source == "launch":
                try:
                    LOGGER.info("DNB 获取 cookie 快照：source=launch force=%s", force)
                    cookies, headers = self._fetch_snapshot_via_launch(domain_keyword)
                except Exception:
                    LOGGER.warning("DNB 浏览器抓 cookie 失败，回退到 HTTP 种子")
                    cookies, headers = self._fetch_snapshot_via_http(domain_keyword)
            else:
                LOGGER.info("DNB 获取 cookie 快照：source=http force=%s", force)
                cookies, headers = self._fetch_snapshot_via_http(domain_keyword)
            self._snapshot_cookies = list(cookies)
            self._snapshot_headers = headers
            self._snapshot_expire_at = now + self._snapshot_ttl_seconds
            save_dnb_cookie_snapshot(
                self._cache_file,
                cookies=self._snapshot_cookies,
                headers=self._headers_to_cache(headers),
            )
            return list(cookies), headers

    def fetch_browser_headers(self) -> DnbBrowserHeaders:
        _cookies, headers = self.fetch_snapshot()
        return headers

    def _headers_from_cache(self, payload: dict[str, str]) -> DnbBrowserHeaders | None:
        user_agent = str(payload.get("user_agent") or "").strip()
        sec_ch_ua = str(payload.get("sec_ch_ua") or "").strip()
        sec_ch_ua_platform = str(payload.get("sec_ch_ua_platform") or "").strip()
        accept_language = str(payload.get("accept_language") or "").strip()
        if not (user_agent and sec_ch_ua and sec_ch_ua_platform and accept_language):
            return None
        return DnbBrowserHeaders(
            user_agent=user_agent,
            sec_ch_ua=sec_ch_ua,
            sec_ch_ua_platform=sec_ch_ua_platform,
            accept_language=accept_language,
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
                        domain = str(item.get("domain", "") or "")
                        if domain_keyword in domain:
                            cookies.append(item)
                return cookies, self._build_headers_from_version(self._browser_version_payload())
            finally:
                browser.close()

    def _fetch_snapshot_via_launch(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        with sync_playwright() as playwright:
            browser = self._launch_browser(playwright)
            try:
                context = browser.new_context(locale="en-US")
                page = context.new_page()
                page.goto(self._seed_url, wait_until="domcontentloaded", timeout=self._launch_timeout_ms)
                page.wait_for_timeout(self._launch_wait_ms)
                user_agent = str(page.evaluate("() => navigator.userAgent") or "").strip()
                cookies = [
                    item
                    for item in context.cookies()
                    if domain_keyword in str(item.get("domain", "") or "")
                ]
                return cookies, self._build_headers_from_user_agent(user_agent)
            finally:
                browser.close()

    def _fetch_snapshot_via_http(self, domain_keyword: str) -> tuple[list[dict[str, str]], DnbBrowserHeaders]:
        proxy = str(os.getenv("HTTP_PROXY", "http://127.0.0.1:7897") or "").strip()
        candidates = []
        if proxy:
            candidates.append({"http": proxy, "https": proxy})
        candidates.append(None)
        last_error: Exception | None = None
        for proxies in candidates:
            for _ in range(3):
                session = cffi_requests.Session(impersonate="chrome110", proxies=proxies)
                try:
                    response = session.get(
                        "https://www.dnb.com/",
                        timeout=20,
                        allow_redirects=True,
                        http_version=CurlHttpVersion.V1_1,
                    )
                    response.raise_for_status()
                    cookies: list[dict[str, str]] = []
                    for cookie in session.cookies.jar:
                        domain = str(getattr(cookie, "domain", "") or "")
                        if domain_keyword in domain:
                            cookies.append(
                                {
                                    "name": str(getattr(cookie, "name", "") or ""),
                                    "value": str(getattr(cookie, "value", "") or ""),
                                    "domain": domain,
                                    "path": str(getattr(cookie, "path", "/") or "/"),
                                }
                            )
                    if cookies:
                        return cookies, self._build_headers(
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
                            "Chrome/110.0.0.0",
                        )
                except Exception as exc:
                    last_error = exc
                    time.sleep(1.0)
                finally:
                    session.close()
        if last_error is not None:
            raise last_error
        raise RuntimeError("DNB HTTP cookie seed failed")

    def _launch_browser(self, playwright) -> Any:
        channel = str(os.getenv("DNB_COOKIE_BROWSER_CHANNEL", "chrome") or "chrome").strip()
        headless = str(os.getenv("DNB_COOKIE_HEADLESS", "0") or "0").strip() not in {"0", "false", "False"}
        proxy = str(os.getenv("HTTP_PROXY", "http://127.0.0.1:7897") or "").strip()
        launch_args: dict[str, Any] = {"headless": headless}
        if proxy:
            launch_args["proxy"] = {"server": proxy}
        if channel:
            try:
                return playwright.chromium.launch(channel=channel, **launch_args)
            except Exception:
                return playwright.chromium.launch(**launch_args)
        return playwright.chromium.launch(**launch_args)

    def _browser_ws_url(self) -> str:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        version = json.loads(opener.open(f"{self._cdp_url}/json/version", timeout=5).read().decode())
        return str(version["webSocketDebuggerUrl"])

    def _browser_version_payload(self) -> dict[str, Any]:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return json.loads(opener.open(f"{self._cdp_url}/json/version", timeout=5).read().decode())

    def _build_headers_from_version(self, version: dict[str, Any]) -> DnbBrowserHeaders:
        user_agent = str(version.get("User-Agent") or "").strip()
        browser_label = str(version.get("Browser") or "").strip()
        return self._build_headers(user_agent, browser_label)

    def _build_headers_from_user_agent(self, user_agent: str) -> DnbBrowserHeaders:
        return self._build_headers(user_agent, "")

    def _build_headers(self, user_agent: str, browser_label: str) -> DnbBrowserHeaders:
        final_user_agent = user_agent or (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )
        major_version = self._extract_major_version(final_user_agent, browser_label)
        return DnbBrowserHeaders(
            user_agent=final_user_agent,
            sec_ch_ua=(
                f'"Chromium";v="{major_version}", '
                f'"Not-A.Brand";v="24", "Google Chrome";v="{major_version}"'
            ),
            sec_ch_ua_platform=self._detect_platform(final_user_agent),
            accept_language="en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        )

    def _extract_major_version(self, user_agent: str, browser_label: str) -> str:
        matched = re.search(r"Chrome/(\d+)", str(user_agent or ""))
        if matched is not None:
            return str(matched.group(1) or "146")
        if "/" in str(browser_label or ""):
            return str(browser_label).split("/", 1)[-1].split(".", 1)[0]
        return "146"

    def _detect_platform(self, user_agent: str) -> str:
        lowered = user_agent.lower()
        if "mac os x" in lowered:
            return '"macOS"'
        if "windows" in lowered:
            return '"Windows"'
        if "linux" in lowered:
            return '"Linux"'
        return '"macOS"'


class DnbCompanyInformationClient:
    """带浏览器 cookie 的 DNB 列表客户端。"""

    def __init__(self, cookie_provider: DnbBrowserCookieProvider | None = None) -> None:
        self._cookie_provider = cookie_provider or DnbBrowserCookieProvider()
        self._cookie_lock = threading.Lock()
        self._cookie_refresh_lock = threading.Lock()
        self._cookies: list[dict[str, str]] = []
        self._browser_headers: DnbBrowserHeaders | None = None
        self._forced_refresh_cooldown_seconds = max(
            float(os.getenv("DNB_COOKIE_REFRESH_MIN_SECONDS", "120") or "120"),
            0.0,
        )
        self._last_forced_refresh_at = 0.0

    def refresh_cookies(self, *, force: bool = True) -> bool:
        now = time.monotonic()
        with self._cookie_lock:
            now = time.monotonic()
            if self._should_skip_forced_refresh(now=now, force=force):
                return False
        with self._cookie_refresh_lock:
            with self._cookie_lock:
                now = time.monotonic()
                if self._should_skip_forced_refresh(now=now, force=force):
                    return False
            if force:
                LOGGER.warning("DNB 触发强制刷新 cookie，将重新获取浏览器快照")
            cookies, browser_headers = self._cookie_provider.fetch_snapshot(force=force)
            with self._cookie_lock:
                self._cookies = list(cookies)
                self._browser_headers = browser_headers
            if force:
                self._last_forced_refresh_at = now
            return True

    def _get_cookies(self) -> list[dict[str, str]]:
        with self._cookie_lock:
            if not self._cookies:
                self._cookies = self._cookie_provider.fetch_cookies()
            return list(self._cookies)

    def _get_browser_headers(self) -> DnbBrowserHeaders:
        with self._cookie_lock:
            if self._browser_headers is None:
                self._browser_headers = self._cookie_provider.fetch_browser_headers()
            return self._browser_headers

    def _new_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(impersonate="chrome110")
        for cookie in self._get_cookies():
            domain = str(cookie.get("domain") or "www.dnb.com").lstrip(".")
            session.cookies.set(
                str(cookie["name"]),
                str(cookie["value"]),
                domain=domain,
                path=str(cookie.get("path") or "/"),
            )
        return session

    def _is_retryable_request_error(self, exc: Exception) -> bool:
        text = str(exc or "").lower()
        return any(hint in text for hint in _RETRYABLE_CURL_HINTS)

    def _should_skip_forced_refresh(self, *, now: float, force: bool) -> bool:
        if not force:
            return False
        if self._forced_refresh_cooldown_seconds <= 0:
            return False
        if not self._cookies or self._browser_headers is None:
            return False
        elapsed = now - self._last_forced_refresh_at
        if elapsed >= self._forced_refresh_cooldown_seconds:
            return False
        remaining = self._forced_refresh_cooldown_seconds - elapsed
        LOGGER.info("DNB cookie 强制刷新仍在冷却期，跳过本次浏览器重取：remaining=%.1fs", remaining)
        return True

    def _request_json_with_retries(
        self,
        *,
        method: str,
        url: str,
        timeout: float,
        headers: dict[str, str],
        json_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(_DNB_REQUEST_RETRIES):
            session = self._new_session()
            try:
                request = session.post if method.upper() == "POST" else session.get
                kwargs: dict[str, Any] = {
                    "timeout": timeout,
                    "headers": headers,
                    "http_version": CurlHttpVersion.V1_1,
                }
                if json_payload is not None:
                    kwargs["json"] = json_payload
                response = request(url, **kwargs)
                if response.status_code == 403 and attempt < _DNB_REQUEST_RETRIES - 1:
                    self.refresh_cookies(force=True)
                    time.sleep(min(0.5 * (attempt + 1), 2.0))
                    continue
                response.raise_for_status()
                payload = response.json()
                return payload if isinstance(payload, dict) else {}
            except Exception as exc:
                last_error = exc
                if not self._is_retryable_request_error(exc):
                    raise
                if attempt < _DNB_REQUEST_RETRIES - 1:
                    time.sleep(min(1.5 * (attempt + 1), 5.0))
                    continue
            finally:
                session.close()
        if last_error is not None:
            raise last_error
        raise RuntimeError("DNB request failed without exception")

    def _detail_headers(self, detail_url: str) -> dict[str, str]:
        browser_headers = self._get_browser_headers()
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": browser_headers.accept_language,
            "Origin": "https://www.dnb.com",
            "Priority": "u=1, i",
            "Referer": detail_url,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": browser_headers.user_agent,
            "sec-ch-ua": browser_headers.sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": browser_headers.sec_ch_ua_platform,
        }

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

    def fetch_page(
        self,
        industry_path: str,
        page_number: int,
        country_code: str = "br",
        region_name: str = "",
        city_name: str = "",
    ) -> DnbListPage:
        payload = {
            "pageNumber": int(page_number),
            "industryPath": industry_path,
            "countryIsoTwoCode": country_code,
        }
        if str(region_name or "").strip():
            payload["regionName"] = str(region_name).strip()
        if str(city_name or "").strip():
            payload["cityName"] = str(city_name).strip()
        referer_url = _list_page_url(
            industry_path,
            country_code,
            region_name,
            city_name,
            int(page_number),
        )
        result = self._request_json_with_retries(
            method="POST",
            url=LIST_API_URL,
            json_payload=payload,
            timeout=30,
            headers=self._list_headers(referer_url),
        )
        return parse_companyinformation_payload(result, industry_path)

    def fetch_detail_profile(self, detail_url: str, country_code: str = "br") -> DnbDetailProfile:
        api_url = (
            f"{DETAIL_API_URL}?path={quote(_companyprofile_api_path(detail_url), safe='')}"
            f"&language=en&country={country_code}"
        )
        result = self._request_json_with_retries(
            method="GET",
            url=api_url,
            timeout=15,
            headers=self._detail_headers(detail_url),
        )
        return parse_companyprofile_payload(result)
