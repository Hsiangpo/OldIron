"""mynavi Pipeline 1 — 地区职位列表页 + 职位详情页抓取。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import MynaviClient, PREF_ROUTE_GROUPS
from .parser import parse_detail_page, parse_has_next, parse_job_cards
from .store import MynaviStore


LOGGER = logging.getLogger("mynavi.pipeline")


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.2,
    proxy: str | None = None,
    max_prefs: int = 0,
    detail_workers: int = 10,
) -> dict[str, int]:
    """执行 Pipeline 1。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = MynaviStore(output_dir / "mynavi_store.db")
    client = MynaviClient(request_delay=request_delay, proxy=proxy)
    pref_codes = sorted(PREF_ROUTE_GROUPS.keys())
    if max_prefs > 0:
        pref_codes = pref_codes[:max_prefs]

    prefs_done = 0
    new_total = 0
    for pref_code in pref_codes:
        new_total += _crawl_prefecture(store, client, pref_code, detail_workers)
        prefs_done += 1

    return {
        "prefs_done": prefs_done,
        "new_companies": new_total,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _crawl_prefecture(store: MynaviStore, client: MynaviClient, pref_code: str, detail_workers: int) -> int:
    checkpoint = store.get_checkpoint(pref_code)
    start_page = checkpoint["last_page"] + 1 if checkpoint and checkpoint["status"] == "running" else 1
    current_page = start_page
    new_total = 0

    while True:
        html_text = client.fetch_list_page(pref_code, current_page)
        if html_text is None:
            LOGGER.warning("mynavi pref=%s page=%d 获取失败", pref_code, current_page)
            return new_total
        cards = parse_job_cards(html_text)
        if not cards:
            store.update_checkpoint(pref_code, max(current_page - 1, 0), "done")
            return new_total
        new_total += _fetch_and_store_details(store, client, cards, detail_workers)
        store.update_checkpoint(pref_code, current_page, "running")
        if current_page <= 2 or current_page % 20 == 0:
            LOGGER.info("mynavi pref=%s page=%d 解析 %d 条职位", pref_code, current_page, len(cards))
        if not parse_has_next(html_text):
            store.update_checkpoint(pref_code, current_page, "done")
            return new_total
        current_page += 1


def _fetch_and_store_details(
    store: MynaviStore,
    client: MynaviClient,
    cards: list[dict[str, str]],
    detail_workers: int,
) -> int:
    if detail_workers <= 1:
        return store.upsert_companies([_fetch_job_detail(client, card) for card in cards])
    rows: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(int(detail_workers or 1), 1), thread_name_prefix="mynavi-detail") as executor:
        futures = {executor.submit(_fetch_job_detail, client, card): card for card in cards}
        for future in as_completed(futures):
            rows.append(future.result())
    return store.upsert_companies(rows)


def _fetch_job_detail(client: MynaviClient, card: dict[str, str]) -> dict[str, str]:
    html_text = client.fetch_detail_page(card["detail_url"]) or ""
    detail = parse_detail_page(html_text)
    return {
        "company_name": detail["company_name"] or card["company_name"],
        "representative": detail["representative"],
        "website": detail["website"],
        "address": detail["address"] or card["address"],
        "industry": card["company_data"],
        "phone": detail["phone"],
        "detail_url": detail["source_job_url"] or card["detail_url"],
        "source_job_url": detail["source_job_url"] or card["detail_url"],
        "emails": detail["emails"],
    }

