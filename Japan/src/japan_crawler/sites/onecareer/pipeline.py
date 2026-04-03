"""OneCareer Pipeline 1 — 分类页列表 + 公司详情抓取。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import OnecareerClient
from .parser import parse_business_categories, parse_company_cards, parse_company_detail, parse_total_pages
from .store import OnecareerStore


LOGGER = logging.getLogger("onecareer.pipeline")


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str | None = None,
    max_categories: int = 0,
    max_pages: int = 0,
    detail_workers: int = 12,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    store = OnecareerStore(output_dir / "onecareer_store.db")
    client = OnecareerClient(request_delay=request_delay, proxy=proxy)

    index_html = client.fetch_index_page()
    if index_html is None:
        return {"categories_done": 0, "new_companies": 0, "total_companies": store.get_company_count(), **client.stats}

    categories = parse_business_categories(index_html)
    if max_categories > 0:
        categories = categories[:max_categories]
    LOGGER.info("OneCareer 分类数：%d", len(categories))

    categories_done = 0
    new_total = 0
    for category in categories:
        category_id = category["category_id"]
        scope = f"business_category:{category_id}"
        checkpoint = store.get_checkpoint(scope)
        start_page = checkpoint["last_page"] + 1 if checkpoint and checkpoint["status"] == "running" else 1
        first_html = client.fetch_category_page(category_id, start_page)
        if first_html is None:
            continue
        total_pages = parse_total_pages(first_html)
        if max_pages > 0:
            total_pages = min(total_pages, max_pages)
        current_html = first_html
        current_page = start_page
        while current_page <= total_pages:
            cards = parse_company_cards(current_html)
            new_total += _fetch_and_store_details(store, client, cards, detail_workers)
            store.update_checkpoint(scope, current_page, total_pages, "running")
            if current_page <= 3 or current_page % 20 == 0 or current_page == total_pages:
                LOGGER.info("分类 %s 第 %d/%d 页：%d 家", category_id, current_page, total_pages, len(cards))
            current_page += 1
            if current_page > total_pages:
                break
            current_html = client.fetch_category_page(category_id, current_page)
            if current_html is None:
                break
        store.update_checkpoint(scope, min(current_page - 1, total_pages), total_pages, "done")
        categories_done += 1
    return {
        "categories_done": categories_done,
        "new_companies": new_total,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _fetch_and_store_details(store: OnecareerStore, client: OnecareerClient, cards: list[dict[str, str]], detail_workers: int) -> int:
    if not cards:
        return 0
    companies = _load_company_details(client, cards, detail_workers)
    return store.upsert_companies(companies)


def _load_company_details(client: OnecareerClient, cards: list[dict[str, str]], detail_workers: int) -> list[dict[str, str]]:
    if detail_workers <= 1:
        return [_fetch_company_detail(client, card) for card in cards]
    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="onecareer-detail") as executor:
        futures = {executor.submit(_fetch_company_detail, client, card): card for card in cards}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _fetch_company_detail(client: OnecareerClient, card: dict[str, str]) -> dict[str, str]:
    html_text = client.fetch_detail_page(card["detail_url"])
    detail = parse_company_detail(html_text or "")
    return {
        "company_id": card["company_id"],
        "company_name": detail["company_name"] or card["company_name"],
        "representative": detail["representative"],
        "website": detail["website"],
        "address": detail["address"],
        "industry": card["industry"],
        "detail_url": card["detail_url"],
    }

