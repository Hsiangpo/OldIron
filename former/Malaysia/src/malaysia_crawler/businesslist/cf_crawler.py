"""BusinessList 的 cf 协议抓取器。"""

from __future__ import annotations

import json
import random
import time
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

from curl_cffi import requests as curl_requests
import requests

from malaysia_crawler.businesslist.cdp_crawler import BusinessListBlockedError
from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.businesslist.parser import parse_company_page
from malaysia_crawler.businesslist.parser import parse_redir_target

BASE_URL = "https://www.businesslist.my"
BLOCKED_TOKENS = (
    "just a moment",
    "checking your browser",
    "cf-turnstile",
    "cf_turnstile",
    "attention required!",
)


def _is_blocked_page(text: str) -> bool:
    return bool(_detect_block_reason(text))


def _detect_block_reason(text: str) -> str:
    lower = text.lower()
    # 中文注释：BusinessList 正常页也会加载 challenge-platform 相关 js，不能仅凭该关键字判定被拦截。
    if 'id="company_name"' in lower or "id='company_name'" in lower:
        return ""
    if (
        "error 1005" in lower
        or "cloudflare to restrict access" in lower
        or "has banned the autonomous system number" in lower
    ):
        return "error_1005_asn_blocked"
    if any(token in lower for token in BLOCKED_TOKENS):
        return "cf_challenge"
    if "challenge-platform" in lower and "ray id" in lower:
        return "cf_challenge"
    if "challenge-platform" in lower and "window._cf_chl_opt" in lower:
        return "cf_challenge"
    if "access denied" in lower and "ray id" in lower and "cloudflare" in lower:
        return "cf_access_denied"
    return ""


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


def _read_cookie_map(path: str) -> dict[str, str]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    content = file_path.read_text(encoding="utf-8").strip()
    if not content:
        return {}
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return _parse_cookie_header(content)
    if isinstance(payload, dict) and isinstance(payload.get("cookies"), list):
        return _cookie_list_to_map(payload["cookies"])
    if isinstance(payload, list):
        return _cookie_list_to_map(payload)
    if isinstance(payload, dict):
        return {str(k): str(v) for k, v in payload.items() if str(k).strip()}
    return {}


def _cookie_list_to_map(items: list[object]) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        value = str(item.get("value", "")).strip()
        if name:
            cookies[name] = value
    return cookies


def _parse_cookie_header(value: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for segment in value.split(";"):
        chunk = segment.strip()
        if not chunk or "=" not in chunk:
            continue
        name, raw_value = chunk.split("=", 1)
        name = name.strip()
        if name:
            cookies[name] = raw_value.strip()
    return cookies


def _cookie_map_to_header(cookies: dict[str, str]) -> str:
    parts = [f"{name}={value}" for name, value in cookies.items() if name and value]
    return "; ".join(parts)


class BusinessListCFCrawler:
    """使用 cloudscraper 进行 BusinessList 协议抓取。"""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        delay_min: float = 0.2,
        delay_max: float = 0.6,
        max_retries: int = 3,
        backoff_base: float = 1.7,
        user_agent: str = "",
        cookies_file: str = "",
        proxy_url: str = "",
        use_system_proxy: bool = False,
    ) -> None:
        self.timeout = max(timeout, 1.0)
        self.delay_min = max(delay_min, 0.0)
        self.delay_max = max(delay_max, self.delay_min)
        self.max_retries = max(max_retries, 1)
        self.backoff_base = max(backoff_base, 1.2)
        self.user_agent = user_agent.strip()
        self.cookies_file = cookies_file.strip()
        self.proxy_url = proxy_url.strip()
        self.use_system_proxy = bool(use_system_proxy)
        self.session = curl_requests.Session(
            impersonate="chrome124",
            trust_env=self.use_system_proxy,
        )
        self._cookie_mtime = 0.0
        self._cookie_signature = ""
        self._configure_session()

    def _configure_session(self) -> None:
        if self.user_agent:
            self.session.headers.update({"User-Agent": self.user_agent})
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
        )
        self._apply_proxy_settings()
        self.refresh_cookies_from_file(force=True)

    def _apply_proxy_settings(self) -> None:
        if not self.proxy_url:
            return
        proxy_map = {"http": self.proxy_url, "https": self.proxy_url}
        self.session.proxies.clear()
        self.session.proxies.update(proxy_map)

    def _sleep_jitter(self) -> None:
        if self.delay_max <= 0:
            return
        time.sleep(random.uniform(self.delay_min, self.delay_max))

    def _sleep_backoff(self, attempt: int) -> None:
        seconds = (self.backoff_base**attempt) + random.uniform(0, 0.5)
        time.sleep(seconds)

    def refresh_cookies_from_file(self, *, force: bool = False) -> bool:
        if not self.cookies_file:
            return False
        cookie_path = Path(self.cookies_file)
        if not cookie_path.exists():
            return False
        stat = cookie_path.stat()
        mtime = float(stat.st_mtime)
        if not force and mtime <= self._cookie_mtime:
            return False
        cookie_map = _read_cookie_map(self.cookies_file)
        if not cookie_map:
            return False
        signature = json.dumps(cookie_map, sort_keys=True, ensure_ascii=False)
        # 中文注释：即便强制刷新也要比较签名，避免“未变化却重复提示已刷新”。
        if signature == self._cookie_signature:
            self._cookie_mtime = mtime
            return False
        cookie_header = _cookie_map_to_header(cookie_map)
        if cookie_header:
            self.session.headers["Cookie"] = cookie_header
        self._cookie_mtime = mtime
        self._cookie_signature = signature
        return True

    def _request(self, url: str, *, allow_redirects: bool = True):
        for attempt in range(1, self.max_retries + 1):
            self.refresh_cookies_from_file(force=False)
            try:
                self.session.cookies.clear()
            except Exception:  # noqa: BLE001
                pass
            self._sleep_jitter()
            try:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=allow_redirects)
            except Exception as exc:  # noqa: BLE001
                if attempt >= self.max_retries:
                    raise requests.exceptions.ConnectionError(str(exc)) from exc
                self._sleep_backoff(attempt)
                continue
            if response.status_code == 404:
                return None
            if response.status_code == 429:
                if attempt >= self.max_retries:
                    raise requests.exceptions.HTTPError("HTTP 429", response=response)
                self._sleep_backoff(attempt)
                continue
            text = response.text
            block_reason = _detect_block_reason(text)
            if block_reason:
                self.refresh_cookies_from_file(force=True)
                if attempt >= self.max_retries:
                    raise BusinessListBlockedError(
                        f"BusinessList 被拦截：{block_reason} status={response.status_code}"
                    )
                self._sleep_backoff(attempt)
                continue
            response.raise_for_status()
            return response
        raise RuntimeError("BusinessList 请求重试耗尽")

    def resolve_website_url(self, website_href: str) -> str:
        href = website_href.strip()
        if not href:
            return ""
        if href.startswith("/redir/"):
            # 中文注释：优先从 redir 链接 query 中直接提取目标域名，避免触发 /redir 页的 cf 挑战。
            direct_target = parse_qs(urlparse(href).query).get("u", [""])[0].strip()
            if direct_target:
                return _normalize_website_url(direct_target)
            redir_url = urljoin(BASE_URL, href)
            response = self._request(redir_url, allow_redirects=False)
            if response is None:
                return ""
            target = parse_redir_target(response.text)
            if not target:
                return ""
            return _normalize_website_url(target)
        return _normalize_website_url(href)

    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None:
        url = f"{BASE_URL}/company/{company_id}"
        response = self._request(url, allow_redirects=True)
        if response is None:
            return None
        parsed = parse_company_page(response.text, response_url=str(response.url))
        if parsed is None:
            return None
        website = self.resolve_website_url(parsed.website_href)
        return replace(parsed, website_url=website)

    def close(self) -> None:
        self.session.close()
