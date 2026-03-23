"""Proff HTTP 客户端与页面解析。"""

from __future__ import annotations

import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from html import unescape
from typing import Any

import requests

from denmark_crawler.sites.proff.models import ProffCompany


NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    flags=re.S,
)
LOGGER = logging.getLogger(__name__)
FILTER_GROUPS = {
    "countrypart": "countrypart",
    "municipality": "municipality",
    "postplace": "postplace",
    "industry": "navindustrynames",
}
TASK_KEY_FILTER_DELIMITER = "||filter="
TASK_KEY_INDUSTRY_DELIMITER = "||industry="


def clean_text(value: object) -> str:
    """清洗字符串。"""
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def clean_email(value: object) -> str:
    """清洗邮箱字段。"""
    text = clean_text(value).lower()
    return text if "@" in text else ""


def build_address(item: dict[str, Any]) -> str:
    """从 Proff 搜索项中拼接地址。"""
    values: list[str] = []
    direct_keys = (
        "address",
        "streetAddress",
        "visitingAddress",
        "postalAddress",
        "formattedAddress",
    )
    for key in direct_keys:
        value = item.get(key)
        if isinstance(value, str):
            text = clean_text(value)
            if text and text not in values:
                values.append(text)
        elif isinstance(value, dict):
            values.extend(_collect_address_from_dict(value))
    for key in ("streetName", "streetNumber", "postalCode", "postCode", "postPlace", "city"):
        text = clean_text(item.get(key))
        if text and text not in values:
            values.append(text)
    return clean_text(", ".join(values))


def _collect_address_from_dict(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in (
        "addressLine1",
        "addressLine2",
        "streetAddress",
        "streetName",
        "streetNumber",
        "postalCode",
        "postCode",
        "postPlace",
        "city",
        "municipality",
    ):
        text = clean_text(payload.get(key))
        if text and text not in values:
            values.append(text)
    return values


def build_task_key(query: str, filter_text: str = "", *, industry: str = "") -> str:
    """构建搜索任务 key。"""
    query_text = clean_text(query)
    filter_value = clean_text(filter_text)
    industry_value = clean_text(industry)
    task_key = query_text
    if filter_value:
        task_key = f"{task_key}{TASK_KEY_FILTER_DELIMITER}{filter_value}" if task_key else f"filter={filter_value}"
    if industry_value:
        task_key = f"{task_key}{TASK_KEY_INDUSTRY_DELIMITER}{industry_value}" if task_key else f"industry={industry_value}"
    return task_key


def parse_task_key(task_key: str) -> tuple[str, str, str]:
    """解析搜索任务 key。"""
    text = clean_text(task_key)
    industry = ""
    filter_text = ""
    query_text = text
    if TASK_KEY_INDUSTRY_DELIMITER in query_text:
        query_text, industry = query_text.split(TASK_KEY_INDUSTRY_DELIMITER, 1)
    elif query_text.startswith("industry="):
        return "", "", clean_text(query_text.split("industry=", 1)[1])
    if TASK_KEY_FILTER_DELIMITER in query_text:
        query_text, filter_text = query_text.split(TASK_KEY_FILTER_DELIMITER, 1)
    elif query_text.startswith("filter="):
        return "", clean_text(query_text.split("filter=", 1)[1]), clean_text(industry)
    return clean_text(query_text), clean_text(filter_text), clean_text(industry)


def extract_next_data(text: str) -> dict[str, Any]:
    """从 HTML 提取 __NEXT_DATA__ JSON。"""
    match = NEXT_DATA_RE.search(str(text or ""))
    if match is None:
        raise RuntimeError("Proff 页面缺少 __NEXT_DATA__，无法解析搜索结果。")
    payload = json.loads(match.group(1))
    return payload if isinstance(payload, dict) else {}


def parse_search_page(text: str, *, query: str, page: int, source_url: str) -> tuple[list[ProffCompany], int, int]:
    """解析 Proff HTML 搜索页。"""
    payload = extract_next_data(text)
    companies_root = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("hydrationData", {})
        .get("searchStore", {})
        .get("companies", {})
    )
    return parse_search_payload(companies_root, query=query, page=page, source_url=source_url)


def parse_search_payload(payload: dict[str, Any], *, query: str, page: int, source_url: str) -> tuple[list[ProffCompany], int, int]:
    """解析 Proff 搜索 JSON。"""
    items = payload.get("companies", [])
    hits = int(payload.get("hits", 0) or 0)
    pages = int(payload.get("pages", 0) or 0)
    rows: list[ProffCompany] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        orgnr = clean_text(item.get("orgnr"))
        company_name = clean_text(item.get("name") or item.get("legalName"))
        if not orgnr or not company_name:
            continue
        contact = item.get("contactPerson") if isinstance(item.get("contactPerson"), dict) else {}
        rows.append(
            ProffCompany(
                orgnr=orgnr,
                company_name=company_name,
                representative=clean_text(contact.get("name")),
                representative_role=clean_text(contact.get("role")),
                address=build_address(item),
                homepage=clean_text(item.get("homePage")),
                email=clean_email(item.get("email")),
                phone=clean_text(item.get("phone")),
                source_query=query,
                source_page=page,
                source_url=source_url,
                raw_payload=item,
            )
        )
    return rows, hits, pages


def extract_filter_values(payload: dict[str, Any], filter_type: str) -> list[tuple[str, int]]:
    """从 filters 中提取某个 filterType 的候选值。"""
    out: list[tuple[str, int]] = []
    target_type = FILTER_GROUPS.get(str(filter_type or "").strip().lower(), str(filter_type or "").strip().lower())
    for group in payload.get("filters", []) or []:
        if not isinstance(group, dict):
            continue
        if clean_text(group.get("filterType")).lower() != target_type:
            continue
        for item in group.get("filters", []) or []:
            if not isinstance(item, dict):
                continue
            name = clean_text(item.get("displayName"))
            hits = int(item.get("hits", 0) or 0)
            if name and not any(existing[0] == name for existing in out):
                out.append((name, hits))
    return out


class ProffClient:
    """Proff 请求客户端。"""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        proxy_url: str,
        min_interval_seconds: float,
    ) -> None:
        self.base_url = str(base_url or "").strip()
        self.api_url = "https://www.proff.dk/api/search"
        self.timeout_seconds = max(float(timeout_seconds), 5.0)
        self.min_interval_seconds = max(float(min_interval_seconds), 0.0)
        self._session = requests.Session()
        self._session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
                ),
                "referer": "https://www.proff.dk/branches%C3%B8g?q=ApS",
            }
        )
        proxy = str(proxy_url or "").strip()
        if proxy:
            self._session.proxies.update({"http": proxy, "https": proxy})
        self._lock = threading.Lock()
        self._last_request_at = 0.0

    def fetch_search_page(self, *, query: str, page: int) -> tuple[list[ProffCompany], int, int]:
        """抓取并解析搜索页。优先走 API。"""
        search_term, filter_text, industry = parse_task_key(query)
        payload, source_url = self.fetch_search_payload(
            search_term=search_term,
            filter_text=filter_text,
            industry=industry,
            page=page,
        )
        return parse_search_payload(payload, query=query, page=page, source_url=source_url)

    def fetch_search_payload(
        self,
        *,
        search_term: str,
        filter_text: str,
        industry: str,
        page: int,
    ) -> tuple[dict[str, Any], str]:
        """抓取 Proff API 搜索 JSON。"""
        params: list[tuple[str, object]] = [("page", max(int(page or 1), 1))]
        if clean_text(search_term):
            params.append(("name", clean_text(search_term)))
        if clean_text(industry):
            params.append(("industry", clean_text(industry)))
        if filter_text:
            params.append(("filter", clean_text(filter_text)))
        params.append(("highlight", "registerListing.alternativeNames.name"))
        with self._lock:
            wait_seconds = self.min_interval_seconds - (time.monotonic() - self._last_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            response = self._session.get(self.api_url, params=params, timeout=self.timeout_seconds)
            self._last_request_at = time.monotonic()
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            raise RuntimeError(f"Proff API 非 JSON 响应：{content_type}")
        payload = response.json()
        return payload if isinstance(payload, dict) else {}, str(response.url)

    def discover_search_task_keys(self, queries: list[str], *, max_results_per_segment: int) -> list[str]:
        """为查询词生成更深分段任务。"""
        task_keys: list[str] = []
        for raw_query in queries:
            query = clean_text(raw_query)
            if not query:
                continue
            try:
                region_values = self.fetch_filter_values(query, "", "countrypart")
            except Exception:
                task_keys.append(build_task_key(query))
                continue
            if not region_values:
                task_keys.append(build_task_key(query))
                continue
            for region_name, _region_hits in region_values:
                region_filter = f"countrypart:{region_name}"
                try:
                    municipality_values = self.fetch_filter_values(query, region_filter, "municipality")
                except Exception:
                    task_keys.append(build_task_key(query, region_filter))
                    continue
                if not municipality_values:
                    task_keys.append(build_task_key(query, region_filter))
                    continue
                for municipality_name, municipality_hits in municipality_values:
                    municipality_filter = f"municipality:{municipality_name}"
                    if municipality_hits <= max_results_per_segment:
                        task_keys.append(build_task_key(query, municipality_filter))
                        continue
                    try:
                        postplace_values = self.fetch_filter_values(query, municipality_filter, "postplace")
                    except Exception:
                        task_keys.append(build_task_key(query, municipality_filter))
                        continue
                    if not postplace_values:
                        task_keys.append(build_task_key(query, municipality_filter))
                        continue
                    for postplace_name, _postplace_hits in postplace_values:
                        postplace_filter = f"postplace:{postplace_name}"
                        try:
                            postplace_payload, _url = self.fetch_search_payload(
                                search_term=query,
                                filter_text=postplace_filter,
                                industry="",
                                page=1,
                            )
                        except Exception:
                            task_keys.append(build_task_key(query, municipality_filter))
                            continue
                        postplace_hits = int(postplace_payload.get("hits", 0) or 0)
                        if postplace_hits <= max_results_per_segment:
                            task_keys.append(build_task_key(query, postplace_filter))
                            continue
                        industry_values = extract_filter_values(postplace_payload, "industry")
                        if not industry_values:
                            task_keys.append(build_task_key(query, postplace_filter))
                            continue
                        for industry_name, _industry_hits in industry_values:
                            task_keys.append(build_task_key("", postplace_filter, industry=industry_name))
        unique: list[str] = []
        for task_key in task_keys:
            if task_key not in unique:
                unique.append(task_key)
        return unique

    def fetch_industry_catalog(self) -> list[str]:
        """从 brancher 页面提取行业目录。"""
        with self._lock:
            wait_seconds = self.min_interval_seconds - (time.monotonic() - self._last_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            response = self._session.get("https://www.proff.dk/brancher", timeout=self.timeout_seconds)
            self._last_request_at = time.monotonic()
        response.raise_for_status()
        values: list[str] = []
        for match in re.finditer(r'/branches%C3%B8g\?q=([^\"&<>]+)', response.text):
            value = clean_text(requests.utils.unquote(match.group(1)).replace("+", " "))
            if value and value not in values:
                values.append(value)
        return values

    def _make_planning_session(self) -> requests.Session:
        """为并发规划创建独立 HTTP session（无全局锁）。"""
        session = requests.Session()
        session.headers.update(dict(self._session.headers))
        if self._session.proxies:
            session.proxies.update(dict(self._session.proxies))
        return session

    def _fetch_payload_with_session(
        self, session: requests.Session, *, industry: str, filter_text: str,
    ) -> dict[str, Any]:
        """用指定 session 发请求（规划专用，不走全局锁）。"""
        params: list[tuple[str, object]] = [("page", 1)]
        if clean_text(industry):
            params.append(("industry", clean_text(industry)))
        if clean_text(filter_text):
            params.append(("filter", clean_text(filter_text)))
        params.append(("highlight", "registerListing.alternativeNames.name"))
        response = session.get(self.api_url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _plan_single_industry(
        self, session: requests.Session, industry: str, max_results_per_segment: int,
    ) -> list[str]:
        """规划单个行业的搜索任务分段。"""
        industry_keys: list[str] = []
        try:
            payload = self._fetch_payload_with_session(session, industry=industry, filter_text="")
        except Exception:
            return []
        hits = int(payload.get("hits", 0) or 0)
        if hits <= 0:
            return []
        if hits <= max_results_per_segment:
            return [build_task_key("", "", industry=industry)]
        municipalities = extract_filter_values(payload, "municipality")
        if not municipalities:
            return [build_task_key("", "", industry=industry)]
        for municipality_name, municipality_hits in municipalities:
            municipality_filter = f"municipality:{municipality_name}"
            if municipality_hits <= max_results_per_segment:
                industry_keys.append(build_task_key("", municipality_filter, industry=industry))
                continue
            try:
                muni_payload = self._fetch_payload_with_session(
                    session, industry=industry, filter_text=municipality_filter,
                )
            except Exception:
                industry_keys.append(build_task_key("", municipality_filter, industry=industry))
                continue
            postplaces = extract_filter_values(muni_payload, "postplace")
            if not postplaces:
                industry_keys.append(build_task_key("", municipality_filter, industry=industry))
                continue
            for postplace_name, _postplace_hits in postplaces:
                industry_keys.append(build_task_key("", f"postplace:{postplace_name}", industry=industry))
        return industry_keys

    def discover_max_coverage_task_keys(
        self,
        *,
        max_results_per_segment: int,
        skip_industries: set[str] | None = None,
        on_industry_done: object | None = None,
        planning_workers: int = 1,
    ) -> list[str]:
        """按行业目录做最大覆盖分段。支持断点续跑和并发规划。"""
        _skip = skip_industries or set()
        industries = self.fetch_industry_catalog()
        todo = [ind for ind in industries if ind not in _skip]
        if _skip:
            LOGGER.info("Proff 规划：总行业=%d，跳过已规划=%d，待规划=%d", len(industries), len(_skip), len(todo))
        if not todo:
            LOGGER.info("Proff 规划：所有行业已完成，无需规划")
            return []
        all_keys: list[str] = []
        keys_lock = threading.Lock()
        done_count = [0]  # 用列表做可变计数器
        workers = max(1, min(int(planning_workers), 32))

        def _process(industry: str) -> None:
            session = self._make_planning_session()
            try:
                industry_keys = self._plan_single_industry(session, industry, max_results_per_segment)
            finally:
                session.close()
            with keys_lock:
                all_keys.extend(industry_keys)
                done_count[0] += 1
                if done_count[0] == 1 or done_count[0] % 25 == 0 or done_count[0] == len(todo):
                    LOGGER.info("Proff 极限分段规划：%d/%d（并发=%d）", done_count[0], len(todo), workers)
            # 回调保存断点（store 自带线程锁）
            if callable(on_industry_done):
                on_industry_done(industry, industry_keys)

        if workers == 1:
            for industry in todo:
                _process(industry)
        else:
            LOGGER.info("Proff 并发规划启动：workers=%d，待规划行业=%d", workers, len(todo))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_process, todo))
        unique: list[str] = []
        for task_key in all_keys:
            if task_key not in unique:
                unique.append(task_key)
        return unique

    def fetch_filter_values(self, search_term: str, filter_text: str, filter_type: str) -> list[tuple[str, int]]:
        """抓取指定筛选层级的候选值。"""
        payload, _source_url = self.fetch_search_payload(
            search_term=search_term,
            filter_text=filter_text,
            industry="",
            page=1,
        )
        return extract_filter_values(payload, filter_type)
