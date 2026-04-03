"""Companies House 搜索与 officers 规则解析。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from urllib.parse import quote
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


_BASE_URL = "https://find-and-update.company-information.service.gov.uk"
_SEARCH_PATH = "/search/companies?q="
_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}
_UK_SUFFIX_TOKENS = {
    "limited",
    "ltd",
    "plc",
    "llp",
    "lp",
    "uk",
    "u.k",
}
_ENTITY_HINTS = {
    "limited",
    "ltd",
    "plc",
    "llp",
    "lp",
    "holdings",
    "group",
    "trust",
    "foundation",
    "ventures",
    "partners",
    "capital",
    "properties",
    "management",
    "investments",
    "solutions",
    "services",
    "systems",
    "company",
    "companies",
}


@dataclass(slots=True)
class CompaniesHouseSearchResult:
    company_name: str
    company_number: str
    company_url: str
    is_dissolved: bool


@dataclass(slots=True)
class CompaniesHouseOfficer:
    name: str
    role: str
    is_active: bool


@dataclass(slots=True)
class CompaniesHouseLookupResult:
    company_number: str
    company_url: str
    officers_url: str
    officer_names: list[str]
    representative: str


class CompaniesHouseClient:
    """英国 Companies House 轻量抓取客户端。"""

    def __init__(self, *, timeout_seconds: float = 20.0, proxy_url: str | None = None) -> None:
        self._session = requests.Session()
        self._session.headers.update(_DEFAULT_HEADERS)
        proxy = str(proxy_url or os.getenv("HTTP_PROXY") or "http://127.0.0.1:7897").strip()
        if proxy:
            self._session.proxies.update({"http": proxy, "https": proxy})
        self._timeout = max(float(timeout_seconds), 5.0)

    def close(self) -> None:
        self._session.close()

    def lookup_company(self, company_name: str) -> CompaniesHouseLookupResult:
        query = str(company_name or "").strip()
        if not query:
            return CompaniesHouseLookupResult("", "", "", [], "")
        search_url = f"{_BASE_URL}{_SEARCH_PATH}{quote(query)}"
        response = self._session.get(search_url, timeout=self._timeout)
        response.raise_for_status()
        search_results = parse_search_results(response.text, _BASE_URL)
        matched = choose_best_search_result(query, search_results)
        if matched is None:
            return CompaniesHouseLookupResult("", "", "", [], "")
        officers_url = f"{matched.company_url}/officers"
        officer_entries = self._fetch_current_officers(officers_url)
        officer_names = select_representative_names(officer_entries)
        representative = "; ".join(officer_names)
        return CompaniesHouseLookupResult(
            company_number=matched.company_number,
            company_url=matched.company_url,
            officers_url=officers_url,
            officer_names=officer_names,
            representative=representative,
        )

    def _fetch_current_officers(self, officers_url: str) -> list[CompaniesHouseOfficer]:
        entries: list[CompaniesHouseOfficer] = []
        page_urls = self._collect_page_urls(officers_url)
        for page_url in page_urls:
            response = self._session.get(page_url, timeout=self._timeout)
            response.raise_for_status()
            entries.extend(parse_officers_page(response.text))
        return entries

    def _collect_page_urls(self, officers_url: str) -> list[str]:
        response = self._session.get(officers_url, timeout=self._timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        urls = [officers_url]
        for link in soup.select(".pagination a, .govuk-pagination a"):
            href = str(link.get("href") or "").strip()
            if not href:
                continue
            full_url = urljoin(_BASE_URL, href)
            if full_url not in urls:
                urls.append(full_url)
        return urls


def parse_search_results(html: str, base_url: str) -> list[CompaniesHouseSearchResult]:
    """解析 Companies House 搜索结果页。"""
    soup = BeautifulSoup(str(html or ""), "html.parser")
    results: list[CompaniesHouseSearchResult] = []
    for item in soup.select("#results li"):
        link = item.find("a", href=re.compile(r"^/company/[A-Z0-9]+$", re.I))
        if link is None:
            continue
        company_url = urljoin(base_url, str(link.get("href") or "").strip())
        matched = re.search(r"/company/([A-Z0-9]+)$", company_url, re.I)
        if matched is None:
            continue
        text = " ".join(item.get_text(" ", strip=True).split())
        results.append(
            CompaniesHouseSearchResult(
                company_name=" ".join(link.get_text(" ", strip=True).split()),
                company_number=matched.group(1),
                company_url=company_url,
                is_dissolved="dissolved" in text.lower(),
            )
        )
    return results


def choose_best_search_result(
    company_name: str,
    results: list[CompaniesHouseSearchResult],
) -> CompaniesHouseSearchResult | None:
    """按英国站点约定选择最合适的 Companies House 结果。"""
    query_norm = normalize_company_name(company_name)
    query_core = strip_uk_suffixes(query_norm)
    candidates: list[tuple[tuple[int, int, int], CompaniesHouseSearchResult]] = []
    for result in results:
        result_norm = normalize_company_name(result.company_name)
        result_core = strip_uk_suffixes(result_norm)
        score = _match_score(query_norm, query_core, result_norm, result_core, result.is_dissolved)
        if score is not None:
            candidates.append((score, result))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    top_score = candidates[0][0]
    top_results = [result for score, result in candidates if score == top_score]
    if len(top_results) != 1:
        return None
    return top_results[0]


def parse_officers_page(html: str) -> list[CompaniesHouseOfficer]:
    """解析单页 officers 列表。"""
    soup = BeautifulSoup(str(html or ""), "html.parser")
    results: list[CompaniesHouseOfficer] = []
    for item in soup.select("div.appointments-list > div[class^='appointment-']"):
        name_tag = item.select_one("h2.heading-medium")
        if name_tag is None:
            continue
        name = " ".join(name_tag.get_text(" ", strip=True).split())
        role_tag = item.select_one("dd[id^='officer-role-']")
        status_tag = item.select_one("span.status-tag")
        role = " ".join(role_tag.get_text(" ", strip=True).split()) if role_tag else ""
        status = " ".join(status_tag.get_text(" ", strip=True).split()).lower() if status_tag else ""
        results.append(
            CompaniesHouseOfficer(
                name=name,
                role=role,
                is_active=(not status) or ("active" in status and "resigned" not in status),
            )
        )
    return results


def select_representative_names(officers: list[CompaniesHouseOfficer]) -> list[str]:
    """按约定从 officers 中选择 representative 名字列表。"""
    current_names = _dedupe_names(
        [str(officer.name or "").strip() for officer in officers if officer.is_active and str(officer.name or "").strip()]
    )
    if not current_names:
        return []
    human_names = [name for name in current_names if is_human_officer_name(name)]
    return human_names or current_names


def normalize_company_name(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def strip_uk_suffixes(value: str) -> str:
    tokens = [token for token in normalize_company_name(value).split() if token]
    while tokens and tokens[-1] in _UK_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens)


def is_human_officer_name(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = normalize_company_name(text)
    if any(hint in normalized.split() for hint in _ENTITY_HINTS):
        return False
    if any(ch.isdigit() for ch in normalized):
        return False
    if "," in text:
        surname, _, rest = text.partition(",")
        return bool(normalize_company_name(surname)) and len(normalize_company_name(rest).split()) >= 1
    return 2 <= len(normalized.split()) <= 6


def _match_score(
    query_norm: str,
    query_core: str,
    result_norm: str,
    result_core: str,
    is_dissolved: bool,
) -> tuple[int, int, int] | None:
    active_bonus = 1 if not is_dissolved else 0
    if result_norm == query_norm:
        return (300, active_bonus, -abs(len(result_norm) - len(query_norm)))
    if query_core and result_core and result_core == query_core:
        return (250, active_bonus, -abs(len(result_core) - len(query_core)))
    if _is_clear_containment(query_core, result_core):
        return (200, active_bonus, -abs(len(result_core) - len(query_core)))
    return None


def _is_clear_containment(left: str, right: str) -> bool:
    a = str(left or "").strip()
    b = str(right or "").strip()
    if not a or not b:
        return False
    shorter = a if len(a) <= len(b) else b
    longer = b if len(a) <= len(b) else a
    if len(shorter) < 8:
        return False
    return shorter in longer


def _dedupe_names(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned
