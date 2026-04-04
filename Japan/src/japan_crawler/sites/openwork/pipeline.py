"""OpenWork Pipeline 1 — 列表页 + 公司详情页抓取。"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import DEFAULT_PER_PAGE, OpenworkClient
from .parser import parse_company_cards, parse_company_detail, parse_total_pages, parse_total_results
from .store import OpenworkStore


LOGGER = logging.getLogger("openwork.pipeline")
_CHECKPOINT_SCOPE = "company_list"
_LIST_PAGE_RETRY_SECONDS = 15
_LIST_PAGE_RETRY_ROUNDS = 40


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
    client = OpenworkClient(
        request_delay=request_delay,
        proxy=proxy,
    )

    checkpoint = store.get_checkpoint(_CHECKPOINT_SCOPE)
    start_page = checkpoint["last_page"] + 1 if checkpoint and checkpoint["status"] == "running" else 1
    if max_pages > 0 and start_page > max_pages:
        LOGGER.warning("OpenWork 断点页 %d 超出测试页上限 %d，回退到第 1 页", start_page, max_pages)
        start_page = 1
    first_html = _wait_for_list_page_html(client, start_page)
    if first_html is None and start_page > 1:
        LOGGER.warning("OpenWork 断点页 %d 获取失败，回退到第 1 页重建断点", start_page)
        start_page = 1
        first_html = _wait_for_list_page_html(client, start_page)
    if first_html is None:
        return {"pages_done": 0, "new_companies": 0, "total_companies": store.get_company_count(), **client.stats}
    total_pages = parse_total_pages(first_html, DEFAULT_PER_PAGE)
    total_count = parse_total_results(first_html)
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    LOGGER.info("OpenWork 列表：预计 %d 家公司，%d 页", total_count, total_pages)

    pages_done = 0
    new_total = 0
    current_html = first_html
    current_page = start_page
    while current_page <= total_pages:
        cards = parse_company_cards(current_html)
        new_total += _fetch_and_store_details(store, client, cards, detail_workers)
        pages_done += 1
        total_pages = _expand_total_pages(current_html, total_pages, max_pages)
        store.update_checkpoint(_CHECKPOINT_SCOPE, current_page, total_pages, "running")
        if pages_done <= 3 or current_page % 20 == 0 or current_page == total_pages:
            LOGGER.info("第 %d/%d 页：解析 %d 家", current_page, total_pages, len(cards))
        current_page += 1
        if current_page > total_pages:
            break
        current_html = _wait_for_list_page_html(client, current_page)
        if current_html is None:
            LOGGER.warning("第 %d 页获取失败，保留断点", current_page)
            return {"pages_done": pages_done, "new_companies": new_total, "total_companies": store.get_company_count(), **client.stats}

    store.update_checkpoint(_CHECKPOINT_SCOPE, total_pages, total_pages, "done")
    return {
        "pages_done": pages_done,
        "new_companies": new_total,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _expand_total_pages(page_html: str, current_total: int, max_pages: int) -> int:
    total_pages = max(current_total, parse_total_pages(page_html, DEFAULT_PER_PAGE))
    if max_pages > 0:
        return min(total_pages, max_pages)
    return total_pages


def _wait_for_list_page_html(client: OpenworkClient, page: int) -> str | None:
    for attempt in range(1, _LIST_PAGE_RETRY_ROUNDS + 1):
        page_html = client.fetch_list_page(page)
        if page_html is not None:
            return page_html
        if attempt >= _LIST_PAGE_RETRY_ROUNDS:
            break
        LOGGER.warning(
            "OpenWork 列表页 %d 暂不可用，第 %d/%d 次等待 %ds 后重试",
            page,
            attempt,
            _LIST_PAGE_RETRY_ROUNDS,
            _LIST_PAGE_RETRY_SECONDS,
        )
        time.sleep(_LIST_PAGE_RETRY_SECONDS)
    return None


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
