"""PasonaCareer Pipeline 1 — 搜索结果页 + 职位详情页抓取。"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .client import PasonacareerClient
from .parser import (
    parse_filter_options,
    parse_job_cards,
    parse_job_detail,
    parse_total_pages,
    parse_total_results,
)
from .store import PasonacareerStore


LOGGER = logging.getLogger("pasonacareer.pipeline")
_SEARCH_PAGE_RETRY_LIMIT = 5
_SEARCH_PAGE_CAP = 196
_LOCATION_FILTER_NAME = "f[s3][]"
_JOB_FILTER_NAME = "f[s1][]"


@dataclass(frozen=True)
class SearchScope:
    key: str
    label: str
    filters: dict[str, str]
    total_pages: int
    first_html: str


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str | None = None,
    max_pages: int = 0,
    detail_workers: int = 12,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    store = PasonacareerStore(output_dir / "pasonacareer_store.db")
    client = PasonacareerClient(request_delay=request_delay, proxy=proxy)
    first_html = _fetch_search_page_with_retries(client, 1, {})
    if first_html is None:
        return _build_pipeline_stats(store, client, 0, 0)

    total_pages = parse_total_pages(first_html)
    total_results = parse_total_results(first_html)
    LOGGER.info("PasonaCareer 搜索：预计 %d 条职位，%d 页", total_results, total_pages)
    if max_pages > 0 or total_pages <= _SEARCH_PAGE_CAP:
        scope = SearchScope("job_list", "全部", {}, total_pages, first_html)
        stats = _run_scope(store, client, scope, detail_workers, max_pages=max_pages)
        return _build_pipeline_stats(store, client, stats["pages_done"], stats["new_companies"])

    scopes = _plan_search_scopes(client, first_html)
    if not scopes:
        LOGGER.warning("PasonaCareer 未生成任何协议分段，本轮保留断点退出。")
        return _build_pipeline_stats(store, client, 0, 0)
    LOGGER.info("PasonaCareer 协议分段完成：%d 个范围", len(scopes))
    pages_done = 0
    new_companies = 0
    for scope in scopes:
        stats = _run_scope(store, client, scope, detail_workers)
        pages_done += stats["pages_done"]
        new_companies += stats["new_companies"]
    return _build_pipeline_stats(store, client, pages_done, new_companies)


def _build_pipeline_stats(
    store: PasonacareerStore,
    client: PasonacareerClient,
    pages_done: int,
    new_companies: int,
) -> dict[str, int]:
    return {
        "pages_done": pages_done,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _plan_search_scopes(client: PasonacareerClient, base_html: str) -> list[SearchScope]:
    location_options = parse_filter_options(base_html, _LOCATION_FILTER_NAME)
    job_options = parse_filter_options(base_html, _JOB_FILTER_NAME)
    locations = [item for item in location_options if _is_location_scope(item)]
    job_roots = [item for item in job_options if _is_job_root_scope(item)]
    scopes: list[SearchScope] = []
    for location in locations:
        filters = {_LOCATION_FILTER_NAME: str(location["value"])}
        label = str(location["label"] or location["value"])
        html_text = _fetch_search_page_with_retries(client, 1, filters)
        if html_text is None:
            LOGGER.warning("PasonaCareer 分段规划失败，地区 %s 本轮跳过。", label)
            continue
        total_results = parse_total_results(html_text)
        if total_results <= 0:
            continue
        total_pages = parse_total_pages(html_text)
        if total_pages <= _SEARCH_PAGE_CAP:
            scopes.append(_build_scope(filters, label, total_pages, html_text))
            continue
        scopes.extend(_plan_large_location_scopes(client, filters, label, job_roots))
    return scopes


def _plan_large_location_scopes(
    client: PasonacareerClient,
    location_filters: dict[str, str],
    location_label: str,
    job_roots: list[dict[str, str | bool]],
) -> list[SearchScope]:
    scopes: list[SearchScope] = []
    for job_root in job_roots:
        filters = dict(location_filters)
        filters[_JOB_FILTER_NAME] = str(job_root["value"])
        html_text = _fetch_search_page_with_retries(client, 1, filters)
        if html_text is None:
            LOGGER.warning("PasonaCareer 分段规划失败，范围 %s / %s 本轮跳过。", location_label, job_root["label"])
            continue
        total_results = parse_total_results(html_text)
        if total_results <= 0:
            continue
        total_pages = parse_total_pages(html_text)
        label = f"{location_label} / {job_root['label']}"
        if total_pages > _SEARCH_PAGE_CAP:
            LOGGER.warning("PasonaCareer 分段后仍超过页上限：%s | %d 页", label, total_pages)
        scopes.append(_build_scope(filters, label, total_pages, html_text))
    return scopes


def _build_scope(filters: dict[str, str], label: str, total_pages: int, first_html: str) -> SearchScope:
    return SearchScope(
        key=_build_scope_key(filters),
        label=label,
        filters=dict(filters),
        total_pages=total_pages,
        first_html=first_html,
    )


def _build_scope_key(filters: dict[str, str]) -> str:
    if not filters:
        return "job_list"
    parts = [f"{key}={value}" for key, value in sorted(filters.items())]
    return "job_list:" + "&".join(parts)


def _is_location_scope(option: dict[str, str | bool]) -> bool:
    return bool(
        str(option.get("value", "")).startswith("pm")
        and not bool(option.get("is_virtual"))
    )


def _is_job_root_scope(option: dict[str, str | bool]) -> bool:
    return bool(
        str(option.get("value", "")).startswith("jb")
        and not str(option.get("parent_value", ""))
        and not bool(option.get("is_virtual"))
    )


def _run_scope(
    store: PasonacareerStore,
    client: PasonacareerClient,
    scope: SearchScope,
    detail_workers: int,
    *,
    max_pages: int = 0,
) -> dict[str, int]:
    total_pages = scope.total_pages if max_pages <= 0 else min(scope.total_pages, max_pages)
    checkpoint = store.get_checkpoint(scope.key)
    start_page = _resolve_start_page(checkpoint, discovered_total_pages=total_pages)
    if start_page is None:
        return {"pages_done": 0, "new_companies": 0}
    if start_page > total_pages:
        store.update_checkpoint(scope.key, total_pages, total_pages, "done")
        return {"pages_done": 0, "new_companies": 0}
    current_html = scope.first_html if start_page == 1 else _fetch_search_page_with_retries(client, start_page, scope.filters)
    if current_html is None:
        if checkpoint is not None and start_page > 1:
            store.update_checkpoint(scope.key, start_page - 1, total_pages, "running")
        return {"pages_done": 0, "new_companies": 0}

    pages_done = 0
    new_companies = 0
    current_page = start_page
    completed = False
    while current_page <= total_pages:
        cards = parse_job_cards(current_html)
        new_companies += _fetch_and_store_details(store, client, cards, detail_workers)
        pages_done += 1
        store.update_checkpoint(scope.key, current_page, total_pages, "running")
        if _should_log_scope_page(current_page, total_pages):
            LOGGER.info("%s 第 %d/%d 页：解析 %d 条职位", scope.label, current_page, total_pages, len(cards))
        current_page += 1
        if current_page > total_pages:
            completed = True
            break
        current_html = _fetch_search_page_with_retries(client, current_page, scope.filters)
        if current_html is None:
            LOGGER.warning("%s 第 %d 页获取失败，保留断点", scope.label, current_page)
            break
    final_page = min(current_page - 1, total_pages)
    store.update_checkpoint(scope.key, final_page, total_pages, "done" if completed else "running")
    return {"pages_done": pages_done, "new_companies": new_companies}


def _should_log_scope_page(current_page: int, total_pages: int) -> bool:
    return current_page <= 3 or current_page % 20 == 0 or current_page == total_pages


def _resolve_start_page(
    checkpoint: dict[str, int | str] | None,
    *,
    discovered_total_pages: int = 0,
) -> int | None:
    if checkpoint is None:
        return 1
    last_page = int(checkpoint.get("last_page", 0) or 0)
    checkpoint_total = int(checkpoint.get("total_pages", 0) or 0)
    total_pages = discovered_total_pages or checkpoint_total
    status = str(checkpoint.get("status", "") or "").strip().lower()
    if status == "done" and total_pages > 0 and last_page >= total_pages:
        return None
    if status in {"running", "done"} and last_page > 0:
        return last_page + 1
    return 1


def _fetch_search_page_with_retries(
    client: PasonacareerClient,
    page: int,
    filters: dict[str, str],
) -> str | None:
    for attempt in range(1, _SEARCH_PAGE_RETRY_LIMIT + 1):
        html_text = client.fetch_search_page(page, filters=filters)
        if html_text is not None:
            return html_text
        if attempt >= _SEARCH_PAGE_RETRY_LIMIT:
            break
        wait = min(10, attempt * 2)
        LOGGER.warning("第 %d 页获取失败，第 %d/%d 次重试，%ds 后继续", page, attempt, _SEARCH_PAGE_RETRY_LIMIT, wait)
        time.sleep(wait)
    return None


def _fetch_and_store_details(
    store: PasonacareerStore,
    client: PasonacareerClient,
    cards: list[dict[str, str]],
    detail_workers: int,
) -> int:
    if not cards:
        return 0
    companies = _load_job_details(client, cards, detail_workers)
    return store.upsert_companies(companies)


def _load_job_details(
    client: PasonacareerClient,
    cards: list[dict[str, str]],
    detail_workers: int,
) -> list[dict[str, str]]:
    if detail_workers <= 1 or client.browser_primary:
        return [_fetch_job_detail(client, card) for card in cards]
    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="pasona-detail") as executor:
        futures = {executor.submit(_fetch_job_detail, client, card): card for card in cards}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _fetch_job_detail(client: PasonacareerClient, card: dict[str, str]) -> dict[str, str]:
    html_text = client.fetch_job_page(card["detail_url"])
    detail = parse_job_detail(html_text or "")
    return {
        "company_name": detail["company_name"] or card["company_name"],
        "representative": detail["representative"],
        "website": detail["website"],
        "address": detail["address"],
        "detail_url": card["detail_url"],
        "source_job_url": card["detail_url"],
    }
