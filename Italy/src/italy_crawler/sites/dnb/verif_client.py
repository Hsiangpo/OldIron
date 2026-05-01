"""Verif 协议客户端。"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
import urllib.request
from urllib.parse import quote
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright


LOGGER = logging.getLogger(__name__)
LOGIN_STATE_NAME = "login_state.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7"
ENV_COOKIE_HEADER = "VERIF_COOKIE_HEADER"
ENV_USER_AGENT = "VERIF_USER_AGENT"
ENV_XSRF_TOKEN = "VERIF_XSRF_TOKEN"
ENV_ACCEPT_LANGUAGE = "VERIF_ACCEPT_LANGUAGE"
ENV_CDP_URL = "VERIF_CDP_URL"
ENV_COOKIE_SOURCE = "VERIF_COOKIE_SOURCE"
ENV_CDP_TIMEOUT_SECONDS = "VERIF_CDP_TIMEOUT_SECONDS"
ENV_COOKIE_CACHE_SECONDS = "VERIF_COOKIE_CACHE_SECONDS"
ENV_COOKIE_REFRESH_MIN_SECONDS = "VERIF_COOKIE_REFRESH_MIN_SECONDS"
REQUIRED_COOKIE_KEYS = ("cf_clearance", "XSRF-TOKEN")
_WEBSITE_LABELS = ("website", "web site")
_REPRESENTATIVE_LABELS = (
    "most senior leader",
    "managing partner",
    "chief executive officer",
    "legal representative",
    "president",
    "owner",
    "director",
)


class VerifChallengeError(RuntimeError):
    """Verif challenge 未通过。"""


@dataclass(slots=True)
class RuntimeLoginState:
    cookie_header: str
    user_agent: str
    xsrf_token: str
    accept_language: str


@dataclass(slots=True)
class VerifCompanyMatch:
    company_name: str
    representative: str
    website: str
    company_url: str
    search_url: str


def normalize_company_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def pick_best_company_hit(hits: list[dict[str, Any]], target_company_name: str) -> dict[str, Any] | None:
    target_key = normalize_company_key(target_company_name)
    best_hit: dict[str, Any] | None = None
    best_score = -1
    for hit in hits:
        company_name = str(
            hit.get("companyName")
            or hit.get("companyNameOriginal")
            or hit.get("primaryName")
            or hit.get("primaryNameOriginal")
            or ""
        ).strip()
        label_key = normalize_company_key(company_name)
        if not label_key:
            continue
        score = 0
        if label_key == target_key and target_key:
            score += 1000
        if target_key and target_key in label_key:
            score += 200
        if label_key and label_key in target_key:
            score += 100
        score += len(set(_tokenize_name(label_key)) & set(_tokenize_name(target_key))) * 10
        if str(hit.get("operatingStatusDescription", "")).strip().lower() == "active":
            score += 20
        if score > best_score:
            best_score = score
            best_hit = hit
    return best_hit


def pick_best_company_link(items: list[tuple[str, str]], target_company_name: str) -> tuple[str, str] | None:
    target_key = normalize_company_key(target_company_name)
    best_item: tuple[str, str] | None = None
    best_score = -1
    for label, href in items:
        label_key = normalize_company_key(label)
        if not href:
            continue
        score = 0
        if label_key == target_key and target_key:
            score += 1000
        if target_key and target_key in label_key:
            score += 200
        if label_key and label_key in target_key:
            score += 100
        score += len(set(_tokenize_name(label_key)) & set(_tokenize_name(target_key))) * 10
        if score > best_score:
            best_score = score
            best_item = (label.strip(), href.strip())
    return best_item


def build_company_url(company_name: str, company_id: str) -> str:
    slug = _slugify_company_name(company_name)
    return f"https://www.verif.com/en/company/{slug}-{company_id}/"


def extract_company_fields_from_text(page_text: str) -> tuple[str, str]:
    lines = _normalize_page_lines(page_text)
    website = _extract_value_after_labels(lines, _WEBSITE_LABELS, normalize=_normalize_website)
    representative = _extract_value_after_labels(lines, _REPRESENTATIVE_LABELS, normalize=_normalize_person)
    return website, representative


class VerifClient:
    """使用 pc 重放 Verif 搜索与公司页请求。"""

    def __init__(
        self,
        *,
        output_dir: Path,
        proxy_url: str,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._state_path = self._output_dir / "session" / LOGIN_STATE_NAME
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._cookie_provider = VerifCookieProvider(self._state_path)
        self._state = load_runtime_login_state(self._state_path)
        if not _has_required_cookies(self._state.cookie_header):
            self._state = self._cookie_provider.fetch_snapshot(force=False)
        self._proxy = str(proxy_url or "").strip()
        self._session = _build_session(self._proxy, self._state.user_agent)

    def close(self) -> None:
        self._save_state()
        self._session.close()

    def search_company(self, company_name: str) -> VerifCompanyMatch | None:
        query = str(company_name or "").strip()
        if not query:
            return None
        self._wait_for_verif_window()
        hits = self._search_company_hits(query)
        if not hits:
            return None
        hit = pick_best_company_hit(hits, query)
        if hit is None:
            return None
        matched_name = str(
            hit.get("companyName")
            or hit.get("companyNameOriginal")
            or hit.get("primaryName")
            or hit.get("primaryNameOriginal")
            or query
        ).strip() or query
        company_id = str(hit.get("id") or hit.get("_id") or hit.get("worldbaseId") or "").strip()
        if not company_id:
            return None
        company_url = build_company_url(matched_name, company_id)
        page_text = self._fetch_company_page_text(company_url, query)
        website, representative = extract_company_fields_from_text(page_text)
        return VerifCompanyMatch(
            company_name=matched_name,
            representative=representative,
            website=website,
            company_url=company_url,
            search_url=_search_url(query),
        )

    def _search_company_hits(self, query: str) -> list[dict[str, Any]]:
        url = _search_api_url(query)
        response = self._request_with_refresh(
            url=url,
            headers=_build_api_headers(self._state, referer=_search_url(query)),
        )
        self._save_state()
        payload = response.json()
        hits = payload.get("hits") if isinstance(payload, dict) else None
        return [item for item in hits or [] if isinstance(item, dict)]

    def _fetch_company_page_text(self, company_url: str, query: str) -> str:
        response = self._request_with_refresh(
            url=company_url,
            headers=_build_page_headers(self._state, referer=_search_url(query)),
        )
        self._save_state()
        return response.text

    def _request_with_refresh(self, *, url: str, headers: dict[str, str]):
        response = self._session.get(url, headers=headers, timeout=30, verify=False)
        if response.status_code not in {403, 429}:
            response.raise_for_status()
            return response
        self._trigger_verif_cooldown(response.status_code)
        self._wait_for_verif_window()
        self._state = self._cookie_provider.fetch_snapshot(force=True)
        retry_headers = dict(headers)
        if "x-xsrf-token" in retry_headers:
            retry_headers["x-xsrf-token"] = self._state.xsrf_token
        retry_headers["cookie"] = self._state.cookie_header
        retry_headers["user-agent"] = self._state.user_agent
        if "accept-language" in retry_headers:
            retry_headers["accept-language"] = self._state.accept_language
        response = self._session.get(url, headers=retry_headers, timeout=30, verify=False)
        if response.status_code in {403, 429}:
            self._trigger_verif_cooldown(response.status_code)
            raise VerifChallengeError(f"Verif blocked after cookie refresh: status={response.status_code}")
        response.raise_for_status()
        return response

    def _save_state(self) -> None:
        save_runtime_login_state(self._state_path, self._state)

    def _wait_for_verif_window(self) -> None:
        while True:
            with VerifCookieProvider._GLOBAL_LOCK:
                block_until = VerifCookieProvider._GLOBAL_BLOCK_UNTIL_MONOTONIC
            now = time.monotonic()
            if now >= block_until:
                return None
            time.sleep(min(block_until - now, 1.5))

    def _trigger_verif_cooldown(self, status_code: int) -> None:
        cooldown_seconds = 20.0 if int(status_code) == 403 else 45.0
        with VerifCookieProvider._GLOBAL_LOCK:
            now = time.monotonic()
            target = now + cooldown_seconds
            if target > VerifCookieProvider._GLOBAL_BLOCK_UNTIL_MONOTONIC:
                VerifCookieProvider._GLOBAL_BLOCK_UNTIL_MONOTONIC = target


def load_runtime_login_state(state_path: Path) -> RuntimeLoginState:
    stored = _read_state_file(state_path)
    cookie_header = str(os.getenv(ENV_COOKIE_HEADER, "") or stored.get("cookie_header") or "").strip()
    xsrf_token = str(os.getenv(ENV_XSRF_TOKEN, "") or stored.get("xsrf_token") or "").strip()
    if not xsrf_token:
        xsrf_token = parse_cookie_header(cookie_header).get("XSRF-TOKEN", "")
    return RuntimeLoginState(
        cookie_header=cookie_header,
        user_agent=str(os.getenv(ENV_USER_AGENT, "") or stored.get("user_agent") or DEFAULT_USER_AGENT).strip(),
        xsrf_token=xsrf_token,
        accept_language=str(os.getenv(ENV_ACCEPT_LANGUAGE, "") or stored.get("accept_language") or DEFAULT_ACCEPT_LANGUAGE).strip(),
    )


def save_runtime_login_state(state_path: Path, state: RuntimeLoginState) -> None:
    payload = {
        "cookie_header": state.cookie_header,
        "user_agent": state.user_agent,
        "xsrf_token": state.xsrf_token,
        "accept_language": state.accept_language,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_cookie_header(cookie_text: str) -> dict[str, str]:
    results: dict[str, str] = {}
    for chunk in str(cookie_text or "").split(";"):
        item = str(chunk or "").strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        clean_key = str(key or "").strip()
        clean_value = str(value or "").strip()
        if clean_key and clean_value:
            results[clean_key] = clean_value
    return results


def _ensure_required_cookies(cookie_header: str) -> None:
    if _has_required_cookies(cookie_header):
        return
    cookies = parse_cookie_header(cookie_header)
    missing = [key for key in REQUIRED_COOKIE_KEYS if not str(cookies.get(key) or "").strip()]
    raise RuntimeError(
        "Verif 缺少可用登录态。"
        f" 至少需要这些 cookie：{', '.join(missing)}。"
        f" 请刷新 {Path('output/dnb/session') / LOGIN_STATE_NAME} 或 .env 里的 {ENV_COOKIE_HEADER}。"
    )


def _has_required_cookies(cookie_header: str) -> bool:
    cookies = parse_cookie_header(cookie_header)
    return all(str(cookies.get(key) or "").strip() for key in REQUIRED_COOKIE_KEYS)


def _read_state_file(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Verif 登录态文件解析失败：%s", state_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _build_session(proxy: str, user_agent: str) -> cffi_requests.Session:
    proxies = {"http": proxy, "https": proxy} if proxy else {}
    session = cffi_requests.Session(impersonate="chrome136", proxies=proxies)
    session.trust_env = False
    session.headers.update(
        {
            "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
            "User-Agent": user_agent or DEFAULT_USER_AGENT,
        }
    )
    return session


def _build_api_headers(state: RuntimeLoginState, *, referer: str) -> dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": state.accept_language or DEFAULT_ACCEPT_LANGUAGE,
        "cookie": state.cookie_header,
        "priority": "u=1, i",
        "referer": referer,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": state.user_agent or DEFAULT_USER_AGENT,
        "x-xsrf-token": state.xsrf_token,
    }


def _build_page_headers(state: RuntimeLoginState, *, referer: str) -> dict[str, str]:
    return {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": state.accept_language or DEFAULT_ACCEPT_LANGUAGE,
        "cookie": state.cookie_header,
        "priority": "u=0, i",
        "referer": referer,
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "upgrade-insecure-requests": "1",
        "user-agent": state.user_agent or DEFAULT_USER_AGENT,
    }


def _search_url(query: str) -> str:
    return f"https://www.verif.com/en/searchResult/?search={quote(str(query or '').strip())}&country=IT"


def _search_api_url(query: str) -> str:
    return (
        "https://www.verif.com/back-api/search"
        f"?query={quote(str(query or '').strip())}"
        "&isoCode=IT&resultType=organization&pageNumber=1&sortOrder=score&locale=en"
    )


def _slugify_company_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^A-Za-z0-9]+", "-", normalized).strip("-")
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized.upper() or "COMPANY"


def _extract_value_after_labels(lines: list[str], labels: tuple[str, ...], *, normalize) -> str:
    lowered_labels = tuple(label.lower() for label in labels)
    for index, line in enumerate(lines):
        lowered = line.lower()
        for label in lowered_labels:
            if lowered == label:
                for offset in range(1, 4):
                    if index + offset >= len(lines):
                        break
                    candidate = normalize(lines[index + offset])
                    if candidate:
                        return candidate
            if lowered.startswith(label):
                tail = line[len(label):].strip(" :-|")
                candidate = normalize(tail)
                if candidate:
                    return candidate
    return ""


def _normalize_website(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = f"https://{text}"
    parsed = urlparse(text)
    if not parsed.netloc:
        return ""
    return f"https://{parsed.netloc.lower()}{parsed.path or ''}".rstrip("/")


def _normalize_person(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    text = re.sub(r"^(mr|mrs|ms|dr)\s+", "", text, flags=re.I)
    if not text or len(text) > 160:
        return ""
    if any(ch.isdigit() for ch in text):
        return ""
    return text


def _tokenize_name(value: str) -> list[str]:
    return [item for item in re.split(r"[^a-z0-9]+", str(value or "").lower()) if item]


class VerifCookieProvider:
    """从 9222 浏览器提取 Verif 运行态。"""

    _GLOBAL_LOCK = threading.Lock()
    _GLOBAL_STATE_BY_KEY: dict[str, RuntimeLoginState] = {}
    _GLOBAL_EXPIRE_AT_BY_KEY: dict[str, float] = {}
    _GLOBAL_LAST_REFRESH_AT_BY_KEY: dict[str, float] = {}
    _GLOBAL_BLOCK_UNTIL_MONOTONIC = 0.0

    def __init__(self, state_path: Path) -> None:
        self._state_path = state_path
        self._cdp_url = str(os.getenv(ENV_CDP_URL, "http://127.0.0.1:9222") or "").strip() or "http://127.0.0.1:9222"
        self._cookie_source = str(os.getenv(ENV_COOKIE_SOURCE, "cdp") or "cdp").strip().lower()
        self._cdp_timeout_ms = int(max(float(os.getenv(ENV_CDP_TIMEOUT_SECONDS, "30") or "30"), 5.0) * 1000)
        self._cache_ttl_seconds = max(float(os.getenv(ENV_COOKIE_CACHE_SECONDS, "600") or "600"), 1.0)
        self._refresh_cooldown_seconds = max(float(os.getenv(ENV_COOKIE_REFRESH_MIN_SECONDS, "90") or "90"), 0.0)

    def fetch_snapshot(self, *, force: bool) -> RuntimeLoginState:
        key = self._cache_key()
        now = time.time()
        with self._GLOBAL_LOCK:
            cached = self._GLOBAL_STATE_BY_KEY.get(key)
            cached_expire_at = self._GLOBAL_EXPIRE_AT_BY_KEY.get(key, 0.0)
            last_refresh_at = self._GLOBAL_LAST_REFRESH_AT_BY_KEY.get(key, 0.0)
            if cached is not None and now < cached_expire_at:
                if not force or (now - last_refresh_at) < self._refresh_cooldown_seconds:
                    return cached
            file_state = load_runtime_login_state(self._state_path)
            if _has_required_cookies(file_state.cookie_header):
                if not force or (now - last_refresh_at) < self._refresh_cooldown_seconds:
                    self._GLOBAL_STATE_BY_KEY[key] = file_state
                    self._GLOBAL_EXPIRE_AT_BY_KEY[key] = now + self._cache_ttl_seconds
                    return file_state
            if self._cookie_source != "cdp":
                raise RuntimeError("Verif 当前只支持从 9222 浏览器提取 cookie。")
            state = self._fetch_snapshot_via_cdp()
            save_runtime_login_state(self._state_path, state)
            self._GLOBAL_STATE_BY_KEY[key] = state
            self._GLOBAL_EXPIRE_AT_BY_KEY[key] = now + self._cache_ttl_seconds
            self._GLOBAL_LAST_REFRESH_AT_BY_KEY[key] = now
            return state

    def _fetch_snapshot_via_cdp(self) -> RuntimeLoginState:
        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(self._browser_ws_url(), timeout=self._cdp_timeout_ms)
            try:
                cookies: list[dict[str, str]] = []
                for context in browser.contexts:
                    for item in context.cookies():
                        domain = str(item.get("domain", "") or "")
                        if "verif.com" in domain:
                            cookies.append(item)
                cookie_map = self._cookie_map_from_items(cookies)
                if not _has_required_cookies(_cookie_header_from_map(cookie_map)):
                    raise RuntimeError("9222 浏览器里没有可用的 Verif cf cookie，请先在该浏览器里通过 Verif challenge。")
                version = self._browser_version_payload()
                return RuntimeLoginState(
                    cookie_header=_cookie_header_from_map(cookie_map),
                    user_agent=str(version.get("User-Agent") or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
                    xsrf_token=str(cookie_map.get("XSRF-TOKEN") or "").strip(),
                    accept_language=DEFAULT_ACCEPT_LANGUAGE,
                )
            finally:
                browser.close()

    def _cache_key(self) -> str:
        return f"{self._state_path.resolve()}::{self._cdp_url}"

    def _browser_ws_url(self) -> str:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        version = json.loads(opener.open(f"{self._cdp_url}/json/version", timeout=5).read().decode())
        return str(version["webSocketDebuggerUrl"])

    def _browser_version_payload(self) -> dict[str, Any]:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return json.loads(opener.open(f"{self._cdp_url}/json/version", timeout=5).read().decode())

    def _cookie_map_from_items(self, items: list[dict[str, Any]]) -> dict[str, str]:
        result: dict[str, str] = {}
        for item in items:
            name = str(item.get("name", "") or "").strip()
            value = str(item.get("value", "") or "").strip()
            if name and value:
                result[name] = value
        return result


def _cookie_header_from_map(cookie_map: dict[str, str]) -> str:
    preferred = ["XSRF-TOKEN", "cf_clearance", "__cf_bm"]
    parts: list[str] = []
    for key in preferred:
        value = str(cookie_map.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    for key in sorted(cookie_map.keys()):
        if key in preferred:
            continue
        value = str(cookie_map.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    return "; ".join(parts)


def _normalize_page_lines(page_text: str) -> list[str]:
    text = str(page_text or "")
    if "<" in text and ">" in text:
        text = html.unescape(text)
        text = re.sub(r"(?is)<(script|style|template)\b[^>]*>.*?</\1>", " ", text)
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(?:p|li|td|th|div|tr|dd|dt|h[1-6]|section|article|ul|ol|table|span|a)>", "\n", text)
        text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    return [line.strip(" \t\r\n:|") for line in text.splitlines() if line.strip(" \t\r\n:|")]
