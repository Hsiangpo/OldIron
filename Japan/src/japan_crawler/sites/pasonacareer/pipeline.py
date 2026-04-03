"""PasonaCareer Pipeline 1 — 搜索结果页 + 职位详情页抓取。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import PasonacareerClient
from .parser import parse_job_cards, parse_job_detail, parse_total_pages, parse_total_results
from .store import PasonacareerStore


LOGGER = logging.getLogger("pasonacareer.pipeline")


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
    client = PasonacareerClient(
        request_delay=request_delay,
        proxy=proxy,
        browser_profile_dir=output_dir / "browser_profile",
    )

    checkpoint = store.get_checkpoint("job_list")
    start_page = checkpoint["last_page"] + 1 if checkpoint and checkpoint["status"] == "running" else 1
    if max_pages > 0 and start_page > max_pages:
        LOGGER.warning("PasonaCareer 断点页 %d 超出测试页上限 %d，回退到第 1 页", start_page, max_pages)
        start_page = 1
    first_html = client.fetch_search_page(start_page)
    if first_html is None:
        return {"pages_done": 0, "new_companies": 0, "total_companies": store.get_company_count(), **client.stats}

    total_pages = parse_total_pages(first_html)
    total_results = parse_total_results(first_html)
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    LOGGER.info("PasonaCareer 搜索：预计 %d 条职位，%d 页", total_results, total_pages)

    pages_done = 0
    new_total = 0
    current_html = first_html
    current_page = start_page
    while current_page <= total_pages:
        cards = parse_job_cards(current_html)
        new_total += _fetch_and_store_details(store, client, cards, detail_workers)
        pages_done += 1
        store.update_checkpoint("job_list", current_page, total_pages, "running")
        if current_page <= 3 or current_page % 20 == 0 or current_page == total_pages:
            LOGGER.info("第 %d/%d 页：解析 %d 条职位", current_page, total_pages, len(cards))
        current_page += 1
        if current_page > total_pages:
            break
        current_html = client.fetch_search_page(current_page)
        if current_html is None:
            LOGGER.warning("第 %d 页获取失败，保留断点", current_page)
            break

    store.update_checkpoint("job_list", min(current_page - 1, total_pages), total_pages, "done")
    return {
        "pages_done": pages_done,
        "new_companies": new_total,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _fetch_and_store_details(store: PasonacareerStore, client: PasonacareerClient, cards: list[dict[str, str]], detail_workers: int) -> int:
    if not cards:
        return 0
    companies = _load_job_details(client, cards, detail_workers)
    return store.upsert_companies(companies)


def _load_job_details(client: PasonacareerClient, cards: list[dict[str, str]], detail_workers: int) -> list[dict[str, str]]:
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
