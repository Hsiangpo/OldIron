"""OpenWork Pipeline 1 — 列表页 + 公司详情页抓取。"""

from __future__ import annotations

import logging
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
_EMPTY_SCOPE_HTML = "<html><body><div>0 件中 0～0件を表示</div></body></html>"


@dataclass(frozen=True)
class ListScope:
    key: str
    label: str
    field: str
    pref: str
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
        yield _build_scope(_ROOT_SCOPE, "全部", "", "", root_html)
        return
    for index, field_code in enumerate(field_codes, start=1):
        field_html = _wait_for_list_page_html(client, 1, field=field_code)
        if field_html is None:
            LOGGER.warning("OpenWork 分段规划失败，行业 %s 本轮跳过。", field_code)
            continue
        total_results = parse_total_results(field_html)
        if total_results <= 0:
            continue
        scope = _build_scope(_build_scope_key(field_code, ""), f"行业 {field_code}", field_code, "", field_html)
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
            _build_scope_key(field_code, pref_code),
            f"行业 {field_code} / 地区 {pref_code}",
            field_code,
            pref_code,
            pref_html,
        )
        if scope.truncated:
            LOGGER.warning(
                "OpenWork 范围仍被站点截断：行业 %s / 地区 %s | %d 家，仅能抓取前 %d 页",
                field_code,
                pref_code,
                total_results,
                scope.total_pages,
            )
        scopes.append(scope)
    return scopes


def _build_scope(key: str, label: str, field: str, pref: str, first_html: str) -> ListScope:
    total_results = parse_total_results(first_html)
    accessible_pages = parse_accessible_pages(total_results, DEFAULT_PER_PAGE, _LIST_PAGE_CAP)
    return ListScope(
        key=key,
        label=label,
        field=field,
        pref=pref,
        total_results=total_results,
        total_pages=accessible_pages,
        first_html=first_html,
        truncated=total_results > accessible_pages * DEFAULT_PER_PAGE,
    )


def _build_scope_key(field: str, pref: str) -> str:
    parts: list[str] = []
    if field:
        parts.append(f"field={field}")
    if pref:
        parts.append(f"pref={pref}")
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
    current_html = scope.first_html if start_page == 1 else _wait_for_list_page_html(client, start_page, field=scope.field, pref=scope.pref)
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
        current_html = _wait_for_list_page_html(client, current_page, field=scope.field, pref=scope.pref)
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
