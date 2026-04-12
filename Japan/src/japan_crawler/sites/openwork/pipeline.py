"""OpenWork Pipeline 1 — 列表页 + 公司详情页抓取。"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import DEFAULT_PER_PAGE, OpenworkClient, OpenworkPageNotFound
from .parser import (
    parse_accessible_pages,
    parse_company_cards,
    parse_company_detail,
    parse_field_codes,
    parse_pref_codes,
    parse_total_results,
)
from .store import OpenworkStore


LOGGER = logging.getLogger("openwork.pipeline")
_LIST_PAGE_RETRY_SECONDS = 15
_LIST_PAGE_RETRY_ROUNDS = 40
_ROOT_SCOPE = "company_list_root"
_LIST_PAGE_CAP = 10
_KEYWORD_SCOPE_RETRY_ROUNDS = 3
_EMPTY_SCOPE_HTML = "<html><body><div>0 件中 0～0件を表示</div></body></html>"
_COMPANY_PREFIXES = (
    "一般社団法人",
    "一般財団法人",
    "公益社団法人",
    "公益財団法人",
    "特定非営利活動法人",
    "地方独立行政法人",
    "独立行政法人",
    "社会保険労務士法人",
    "司法書士法人",
    "行政書士法人",
    "社会福祉法人",
    "医療法人社団",
    "医療法人財団",
    "弁護士法人",
    "税理士法人",
    "学校法人",
    "宗教法人",
    "監査法人",
    "医療法人",
    "株式会社",
    "有限会社",
    "合同会社",
    "合名会社",
    "合資会社",
    "NPO法人",
)
_ASCII_PREFIX_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9&+._-]{1,7}")
_LEADING_SYMBOLS_RE = re.compile(r"^[\s\u3000「」『』【】（）()［］\[\]<>〈〉《》・･･'\"“”‘’]+")


@dataclass(frozen=True)
class ListScope:
    key: str
    label: str
    field: str
    pref: str
    src_str: str
    total_results: int
    total_pages: int
    first_html: str
    truncated: bool = False


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.2,
    proxy: str | None = None,
    max_pages: int = 0,
    detail_workers: int = 12,
) -> dict[str, int]:
    """执行 Pipeline 1。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = OpenworkStore(output_dir / "openwork_store.db")
    client = OpenworkClient(request_delay=request_delay, proxy=proxy)
    root_html = _wait_for_list_page_html(client, 1)
    if root_html is None:
        return _build_pipeline_stats(store, client, 0, 0)
    remaining_budget = max_pages if max_pages > 0 else None
    pages_done = 0
    new_companies = 0
    scope_count = 0
    for scope in _iter_list_scopes(client, root_html):
        scope_count += 1
        if remaining_budget is not None and remaining_budget <= 0:
            break
        stats = _run_scope(
            store=store,
            client=client,
            scope=scope,
            detail_workers=detail_workers,
            page_budget=remaining_budget,
        )
        pages_done += stats["pages_done"]
        new_companies += stats["new_companies"]
        if remaining_budget is not None:
            remaining_budget = max(remaining_budget - stats["pages_done"], 0)
    if scope_count <= 0:
        LOGGER.warning("OpenWork 未生成任何分段范围，本轮保留断点退出。")
        return _build_pipeline_stats(store, client, 0, 0)
    LOGGER.info("OpenWork 本轮已规划范围：%d 个", scope_count)
    return _build_pipeline_stats(store, client, pages_done, new_companies)


def _build_pipeline_stats(
    store: OpenworkStore,
    client: OpenworkClient,
    pages_done: int,
    new_companies: int,
) -> dict[str, int]:
    return {
        "pages_done": pages_done,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _plan_list_scopes(client: OpenworkClient, root_html: str) -> list[ListScope]:
    return list(_iter_list_scopes(client, root_html))


def _iter_list_scopes(client: OpenworkClient, root_html: str):
    field_codes = parse_field_codes(root_html)
    pref_codes = parse_pref_codes(root_html)
    if not field_codes:
        yield _build_scope(_ROOT_SCOPE, "全部", "", "", "", root_html)
        return
    for index, field_code in enumerate(field_codes, start=1):
        field_html = _wait_for_list_page_html(client, 1, field=field_code)
        if field_html is None:
            LOGGER.warning("OpenWork 分段规划失败，行业 %s 本轮跳过。", field_code)
            continue
        total_results = parse_total_results(field_html)
        if total_results <= 0:
            continue
        scope = _build_scope(
            _build_scope_key(field_code, "", ""),
            f"行业 {field_code}",
            field_code,
            "",
            "",
            field_html,
        )
        if not scope.truncated:
            yield scope
            continue
        field_pref_codes = parse_pref_codes(field_html) or pref_codes
        if index <= 5 or index % 10 == 0 or index == len(field_codes):
            LOGGER.info("OpenWork 范围过大，按都道府県继续拆分：行业 %s | %d 家", field_code, total_results)
        yield from _plan_pref_scopes(client, field_code, field_pref_codes)


def _plan_pref_scopes(client: OpenworkClient, field_code: str, pref_codes: list[str]) -> list[ListScope]:
    scopes: list[ListScope] = []
    for pref_code in pref_codes:
        pref_html = _wait_for_list_page_html(client, 1, field=field_code, pref=pref_code)
        if pref_html is None:
            LOGGER.warning("OpenWork 分段规划失败，行业 %s / 地区 %s 本轮跳过。", field_code, pref_code)
            continue
        total_results = parse_total_results(pref_html)
        if total_results <= 0:
            continue
        scope = _build_scope(
            _build_scope_key(field_code, pref_code, ""),
            f"行业 {field_code} / 地区 {pref_code}",
            field_code,
            pref_code,
            "",
            pref_html,
        )
        scopes.append(scope)
        if scope.truncated:
            LOGGER.warning(
                "OpenWork 范围仍被站点截断：行业 %s / 地区 %s | %d 家，仅能抓取前 %d 页",
                field_code,
                pref_code,
                total_results,
                scope.total_pages,
            )
            keyword_scopes = _plan_keyword_scopes(client, scope)
            if keyword_scopes:
                LOGGER.info(
                    "OpenWork 截断范围追加关键词拆分：行业 %s / 地区 %s | 新增 %d 个关键词范围",
                    field_code,
                    pref_code,
                    len(keyword_scopes),
                )
                scopes.extend(keyword_scopes)
    return scopes


def _plan_keyword_scopes(client: OpenworkClient, parent_scope: ListScope) -> list[ListScope]:
    cards = _collect_scope_cards(client, parent_scope)
    keywords = _build_scope_keywords(cards)
    scopes: list[ListScope] = []
    seen_keys: set[str] = set()
    for keyword in keywords:
        keyword_html = _wait_for_list_page_html(
            client,
            1,
            field=parent_scope.field,
            pref=parent_scope.pref,
            src_str=keyword,
            max_rounds=_KEYWORD_SCOPE_RETRY_ROUNDS,
        )
        if keyword_html is None:
            continue
        keyword_scope = _build_scope(
            _build_scope_key(parent_scope.field, parent_scope.pref, keyword),
            f"{parent_scope.label} / 关键词 {keyword}",
            parent_scope.field,
            parent_scope.pref,
            keyword,
            keyword_html,
        )
        if keyword_scope.total_results <= 0:
            continue
        if keyword_scope.key in seen_keys:
            continue
        seen_keys.add(keyword_scope.key)
        if keyword_scope.truncated:
            LOGGER.warning(
                "OpenWork 关键词范围仍较大：行业 %s / 地区 %s / 关键词 %s | %d 家，仅先抓取前 %d 页",
                parent_scope.field,
                parent_scope.pref,
                keyword,
                keyword_scope.total_results,
                keyword_scope.total_pages,
            )
        scopes.append(keyword_scope)
    return scopes


def _collect_scope_cards(client: OpenworkClient, scope: ListScope) -> list[dict[str, str]]:
    cards_by_id: dict[str, dict[str, str]] = {}
    current_html = scope.first_html
    for page in range(1, scope.total_pages + 1):
        if page > 1:
            current_html = _wait_for_list_page_html(
                client,
                page,
                field=scope.field,
                pref=scope.pref,
                src_str=scope.src_str,
                max_rounds=_KEYWORD_SCOPE_RETRY_ROUNDS,
            )
            if current_html is None:
                break
        for card in parse_company_cards(current_html):
            company_id = str(card.get("company_id", "") or "").strip()
            if company_id and company_id not in cards_by_id:
                cards_by_id[company_id] = card
    return list(cards_by_id.values())


def _build_scope_keywords(cards: list[dict[str, str]]) -> list[str]:
    keywords: list[str] = []
    seen: set[str] = set()
    for card in cards:
        keyword = _build_keyword_seed(str(card.get("company_name", "") or ""))
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        keywords.append(keyword)
    return keywords


def _build_keyword_seed(company_name: str) -> str:
    normalized = _normalize_company_name(company_name)
    if not normalized:
        return ""
    for prefix in _COMPANY_PREFIXES:
        if normalized.startswith(prefix):
            suffix = _normalize_company_name(normalized[len(prefix) :])
            if suffix:
                return f"{prefix}{suffix[:1]}"
            return prefix
    ascii_match = _ASCII_PREFIX_RE.match(normalized)
    if ascii_match is not None:
        return ascii_match.group(0)[:4]
    return normalized[:2]


def _normalize_company_name(value: str) -> str:
    text = _LEADING_SYMBOLS_RE.sub("", str(value or "").strip())
    return re.sub(r"\s+", "", text)


def _build_scope(key: str, label: str, field: str, pref: str, src_str: str, first_html: str) -> ListScope:
    total_results = parse_total_results(first_html)
    accessible_pages = parse_accessible_pages(total_results, DEFAULT_PER_PAGE, _LIST_PAGE_CAP)
    return ListScope(
        key=key,
        label=label,
        field=field,
        pref=pref,
        src_str=src_str,
        total_results=total_results,
        total_pages=accessible_pages,
        first_html=first_html,
        truncated=total_results > accessible_pages * DEFAULT_PER_PAGE,
    )


def _build_scope_key(field: str, pref: str, src_str: str) -> str:
    parts: list[str] = []
    if field:
        parts.append(f"field={field}")
    if pref:
        parts.append(f"pref={pref}")
    if src_str:
        parts.append(f"src_str={src_str}")
    return "company_list" if not parts else "company_list:" + "&".join(parts)


def _run_scope(
    *,
    store: OpenworkStore,
    client: OpenworkClient,
    scope: ListScope,
    detail_workers: int,
    page_budget: int | None,
) -> dict[str, int]:
    checkpoint = store.get_checkpoint(scope.key)
    start_page = _resolve_start_page(checkpoint, scope.total_pages)
    if start_page is None:
        return {"pages_done": 0, "new_companies": 0}
    current_html = (
        scope.first_html
        if start_page == 1
        else _wait_for_list_page_html(
            client,
            start_page,
            field=scope.field,
            pref=scope.pref,
            src_str=scope.src_str,
        )
    )
    if current_html is None:
        return {"pages_done": 0, "new_companies": 0}
    pages_done = 0
    new_companies = 0
    current_page = start_page
    completed = False
    while current_page <= scope.total_pages:
        if page_budget is not None and pages_done >= page_budget:
            break
        cards = parse_company_cards(current_html)
        new_companies += _fetch_and_store_details(store, client, cards, detail_workers)
        pages_done += 1
        store.update_checkpoint(scope.key, current_page, scope.total_pages, "running")
        if _should_log_scope_page(current_page, scope.total_pages):
            LOGGER.info("%s 第 %d/%d 页：解析 %d 家", scope.label, current_page, scope.total_pages, len(cards))
        current_page += 1
        if current_page > scope.total_pages:
            completed = True
            break
        current_html = _wait_for_list_page_html(
            client,
            current_page,
            field=scope.field,
            pref=scope.pref,
            src_str=scope.src_str,
        )
        if current_html is None:
            LOGGER.warning("%s 第 %d 页获取失败，保留断点", scope.label, current_page)
            break
    final_page = min(current_page - 1, scope.total_pages)
    store.update_checkpoint(scope.key, final_page, scope.total_pages, "done" if completed else "running")
    return {"pages_done": pages_done, "new_companies": new_companies}


def _resolve_start_page(checkpoint: dict[str, Any] | None, total_pages: int) -> int | None:
    if checkpoint is None:
        return 1
    last_page = int(checkpoint.get("last_page", 0) or 0)
    status = str(checkpoint.get("status", "") or "").strip().lower()
    if status == "done" and last_page >= total_pages:
        return None
    return min(last_page + 1, total_pages)


def _should_log_scope_page(current_page: int, total_pages: int) -> bool:
    return current_page <= 2 or current_page == total_pages or current_page % 10 == 0


def _wait_for_list_page_html(
    client: OpenworkClient,
    page: int,
    *,
    field: str = "",
    pref: str = "",
    src_str: str = "",
    ct: str = "",
    max_rounds: int = _LIST_PAGE_RETRY_ROUNDS,
) -> str | None:
    total_rounds = max(int(max_rounds or 1), 1)
    for attempt in range(1, total_rounds + 1):
        page_html = _fetch_list_page(client, page, field=field, pref=pref, src_str=src_str, ct=ct)
        if page_html is not None:
            return page_html
        if attempt >= total_rounds:
            break
        LOGGER.warning(
            "OpenWork 列表页暂不可用：page=%d field=%s pref=%s | 第 %d/%d 次等待 %ds 后重试",
            page,
            field or "-",
            pref or "-",
            attempt,
            total_rounds,
            _LIST_PAGE_RETRY_SECONDS,
        )
        time.sleep(_LIST_PAGE_RETRY_SECONDS)
    return None


def _fetch_list_page(
    client: OpenworkClient,
    page: int,
    *,
    field: str,
    pref: str,
    src_str: str,
    ct: str,
) -> str | None:
    try:
        return client.fetch_list_page(page, field=field, pref=pref, src_str=src_str, ct=ct)
    except OpenworkPageNotFound:
        return _EMPTY_SCOPE_HTML
    except TypeError:
        try:
            return client.fetch_list_page(page)
        except OpenworkPageNotFound:
            return _EMPTY_SCOPE_HTML


def _fetch_and_store_details(
    store: OpenworkStore,
    client: OpenworkClient,
    cards: list[dict[str, str]],
    detail_workers: int,
) -> int:
    if not cards:
        return 0
    companies = _load_company_details(client, cards, detail_workers)
    return store.upsert_companies(companies)


def _load_company_details(
    client: OpenworkClient,
    cards: list[dict[str, str]],
    detail_workers: int,
) -> list[dict[str, str]]:
    if detail_workers <= 1 or client.browser_primary:
        results: list[dict[str, str]] = []
        total = len(cards)
        for index, card in enumerate(cards, start=1):
            results.append(_fetch_company_detail(client, card))
            if index == 1 or index == total or index % 5 == 0:
                LOGGER.info("OpenWork 详情进度：%d/%d", index, total)
        return results
    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="openwork-detail") as executor:
        futures = {executor.submit(_fetch_company_detail, client, card): card for card in cards}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _fetch_company_detail(client: OpenworkClient, card: dict[str, str]) -> dict[str, str]:
    html_text = client.fetch_detail_page(card["detail_url"])
    if not html_text:
        LOGGER.warning("OpenWork 详情页抓取失败，先保留列表页信息：%s", card["detail_url"])
        return {
            "company_id": card["company_id"],
            "company_name": card["company_name"],
            "representative": "",
            "website": "",
            "address": "",
            "industry": card["industry"],
            "detail_url": card["detail_url"],
        }
    detail = parse_company_detail(html_text or "")
    return {
        "company_id": card["company_id"],
        "company_name": detail["company_name"] or card["company_name"],
        "representative": detail["representative"],
        "website": detail["website"],
        "address": detail["address"],
        "industry": detail["industry"] or card["industry"],
        "detail_url": card["detail_url"],
    }
