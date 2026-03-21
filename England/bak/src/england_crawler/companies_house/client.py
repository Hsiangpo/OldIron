"""Companies House 协议客户端。"""

from __future__ import annotations

import logging
import random
import re
import threading
import time
import unicodedata
from dataclasses import dataclass
from html import unescape
from urllib.parse import quote_plus
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests
from lxml import html

from england_crawler.companies_house.proxy import BlurpathProxyConfig

logger = logging.getLogger(__name__)

SEARCH_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
    "referer": "https://find-and-update.company-information.service.gov.uk/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "upgrade-insecure-requests": "1",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "navigate",
    "sec-fetch-dest": "document",
}
NEGATIVE_STATUS_HINTS = (
    "dissolved",
    "liquidation",
    "liquidated",
    "administration",
    "receivership",
    "converted / closed",
    "closed",
)
COMPANY_PATH_PATTERN = re.compile(r"/company/([^/?#]+)", flags=re.I)
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.25
DEFAULT_BLOCK_COOLDOWN_SECONDS = 10.0


class _RequestGate:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0
        self._blocked_until = 0.0

    def wait_turn(self, min_interval_seconds: float) -> None:
        delay = 0.0
        with self._lock:
            now = time.monotonic()
            target = max(self._next_allowed_at, self._blocked_until)
            if target > now:
                delay = target - now
                now = target
            self._next_allowed_at = now + max(min_interval_seconds, 0.0)
        if delay > 0:
            time.sleep(delay)

    def block_all(self, cooldown_seconds: float) -> None:
        with self._lock:
            self._blocked_until = max(
                self._blocked_until,
                time.monotonic() + max(cooldown_seconds, 0.0),
            )


_REQUEST_GATE = _RequestGate()


def _random_session_id() -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(alphabet) for _ in range(4))


@dataclass(slots=True)
class CompaniesHouseCandidate:
    company_name: str
    company_number: str
    status_text: str
    address: str
    detail_path: str

    @property
    def is_active(self) -> bool:
        lowered = self.status_text.lower()
        if any(hint in lowered for hint in NEGATIVE_STATUS_HINTS):
            return False
        return bool(lowered)


def normalize_company_name(value: str) -> str:
    """标准化公司名，用于精确匹配。"""
    text = unescape(str(value or ""))
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " AND ")
    text = text.upper()
    text = re.sub(r"[^0-9A-Z]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_search_results(page_html: str) -> list[CompaniesHouseCandidate]:
    """解析搜索结果页。"""
    root = html.fromstring(page_html or "<html></html>")
    items: list[CompaniesHouseCandidate] = []
    for node in root.xpath("//ul[@id='results']/li[contains(@class,'type-company')]"):
        company_name = _normalize_space("".join(node.xpath("./h3//a//text()")))
        detail_path = _normalize_space("".join(node.xpath("./h3//a/@href")))
        status_text = _normalize_space("".join(node.xpath("./p[contains(@class,'meta')]//text()")))
        address = _normalize_space("".join(node.xpath("./p[not(contains(@class,'meta'))][1]//text()")))
        matched = COMPANY_PATH_PATTERN.search(detail_path)
        company_number = matched.group(1).strip() if matched else ""
        if company_name and company_number and detail_path:
            items.append(
                CompaniesHouseCandidate(
                    company_name=company_name,
                    company_number=company_number,
                    status_text=status_text,
                    address=address,
                    detail_path=detail_path,
                )
            )
    return items


def select_best_candidate(
    query_name: str,
    candidates: list[CompaniesHouseCandidate],
) -> CompaniesHouseCandidate | None:
    """按“标准化精确匹配 + 活跃优先”选择最佳公司。"""
    normalized_query = normalize_company_name(query_name)
    exact_matches = [
        item
        for item in candidates
        if normalize_company_name(item.company_name) == normalized_query
    ]
    if not exact_matches:
        return None
    exact_matches.sort(key=lambda item: (not item.is_active, candidates.index(item)))
    return exact_matches[0]


def parse_first_active_director(page_html: str) -> str:
    """解析 officers 页面中的首个当前 Director。"""
    root = html.fromstring(page_html or "<html></html>")
    for node in root.xpath("//div[starts-with(@class,'appointment-') or contains(@class,' appointment-')]"):
        officer_name = _normalize_space(
            "".join(node.xpath(".//*[starts-with(@id,'officer-name')]//text()"))
        )
        role = _normalize_space(
            "".join(node.xpath(".//*[starts-with(@id,'officer-role')]//text()"))
        )
        status = _normalize_space(
            "".join(node.xpath(".//*[starts-with(@id,'officer-status-tag')]//text()"))
        )
        if officer_name and role.lower() == "director" and status.lower() == "active":
            return officer_name
    return ""


class CompaniesHouseClient:
    """封装搜索结果与 officers 页面请求。"""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        max_retries: int = 4,
        min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        block_cooldown_seconds: float = DEFAULT_BLOCK_COOLDOWN_SECONDS,
        proxy_config: BlurpathProxyConfig | None = None,
        worker_label: str = "",
    ) -> None:
        self.timeout = max(float(timeout), 5.0)
        self.max_retries = max(int(max_retries), 1)
        self.min_request_interval_seconds = max(float(min_request_interval_seconds), 0.0)
        self.block_cooldown_seconds = max(float(block_cooldown_seconds), 1.0)
        self.base_url = "https://find-and-update.company-information.service.gov.uk"
        self.proxy_config = proxy_config or BlurpathProxyConfig(False, "", 0, "", "", "GB", 10)
        self.worker_label = worker_label.strip()
        self.session_id = _random_session_id()
        self.session = self._build_session()

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(
            impersonate="chrome110",
            curl_options=self.proxy_config.build_curl_options() if self.proxy_config.enabled else None,
        )
        if self.proxy_config.enabled:
            proxy_url = self.proxy_config.build_proxy_url(self.session_id)
            session.proxies = {"http": proxy_url, "https": proxy_url}
        return session

    def _reset_session(self, *, rotate: bool) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        if rotate and self.proxy_config.enabled:
            self.session_id = _random_session_id()
        self.session = self._build_session()

    def close(self) -> None:
        self.session.close()

    def describe_proxy(self) -> str:
        if not self.proxy_config.enabled:
            return "direct"
        return f"{self.proxy_config.host}:{self.proxy_config.port}"

    def describe_preproxy(self) -> str:
        if not self.proxy_config.enabled:
            return "-"
        return self.proxy_config.describe_preproxy()

    def current_session_label(self) -> str:
        return self.session_id

    def fetch_exit_ip(self, *, strict: bool = False) -> str:
        if not self.proxy_config.enabled:
            return "direct"
        try:
            response = self.session.get(
                "http://ipinfo.io/ip",
                headers={
                    "accept": "text/plain",
                    "user-agent": SEARCH_HEADERS["user-agent"],
                },
                timeout=10,
            )
            response.raise_for_status()
            exit_ip = str(response.text or "").strip()
            if not exit_ip:
                raise RuntimeError("Blurpath 代理未返回出口 IP。")
            return exit_ip
        except Exception as exc:
            if strict:
                raise RuntimeError(f"Blurpath 代理出口探测失败：{exc}") from exc
            return f"error:{_normalize_space(exc)}"

    def probe_proxy(self) -> tuple[bool, str]:
        if not self.proxy_config.enabled:
            return True, "direct"
        try:
            return True, self.fetch_exit_ip(strict=True)
        except Exception as exc:
            return False, _normalize_space(exc)

    def _get_text(self, path_or_url: str) -> str:
        url = path_or_url
        if not path_or_url.startswith("http"):
            url = f"{self.base_url.rstrip('/')}/{path_or_url.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            _REQUEST_GATE.wait_turn(self.min_request_interval_seconds)
            response = self.session.get(url, headers=SEARCH_HEADERS, timeout=self.timeout)
            if response.status_code in {401, 407}:
                response.raise_for_status()
            if response.status_code == 403:
                _REQUEST_GATE.block_all(self.block_cooldown_seconds)
                old_session = self.session_id
                self._reset_session(rotate=True)
                if attempt >= self.max_retries:
                    response.raise_for_status()
                wait_seconds = min(self.block_cooldown_seconds + random.uniform(0.2, 0.8), 30.0)
                logger.warning(
                    "Companies House 403，worker=%s session=%s->%s proxy=%s 冷却 %.1fs 后重试：%s",
                    self.worker_label or "-",
                    old_session,
                    self.session_id,
                    self.describe_proxy(),
                    wait_seconds,
                    url,
                )
                time.sleep(wait_seconds)
                continue
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt >= self.max_retries:
                    response.raise_for_status()
                time.sleep(min((2**attempt) + random.uniform(0.1, 0.5), 15.0))
                continue
            response.raise_for_status()
            return response.text
        raise RuntimeError(f"Companies House 请求失败: {url}")

    def search_companies(self, query_name: str) -> list[CompaniesHouseCandidate]:
        """查询公司搜索结果。"""
        path = f"/search/companies?q={quote_plus(query_name)}"
        return parse_search_results(self._get_text(path))

    def fetch_first_active_director(self, company_number: str) -> str:
        """读取 officers 页面并返回首个当前 Director。"""
        path = f"/company/{company_number}/officers"
        return parse_first_active_director(self._get_text(path))
