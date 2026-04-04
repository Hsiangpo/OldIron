"""Mynavi Pipeline 1 — 五十音分组列表 + 公司详情抓取。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import MynaviClient
from .parser import parse_company_cards, parse_company_detail, parse_kana_groups, parse_total_pages
from .store import MynaviStore


LOGGER = logging.getLogger("mynavi.pipeline")


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str | None = None,
    max_groups: int = 0,
    max_pages: int = 0,
    group_workers: int = 10,
    detail_workers: int = 12,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    store = MynaviStore(output_dir / "mynavi_store.db")
    client = MynaviClient(request_delay=request_delay, proxy=proxy)

    index_html = client.fetch_index_page()
    if index_html is None:
        return {"groups_done": 0, "new_companies": 0, "total_companies": store.get_company_count(), **client.stats}

    groups = parse_kana_groups(index_html)
    if max_groups > 0:
        groups = groups[:max_groups]
    LOGGER.info("Mynavi 五十音分组数：%d", len(groups))

    groups_done, new_total = _run_group_jobs(
        groups=groups,
        max_workers=max(int(group_workers or 1), 1),
        worker_fn=lambda group: _process_group(
            store=store,
            client=client,
            group=group,
            max_pages=max_pages,
            detail_workers=detail_workers,
        ),
    )
    return {
        "groups_done": groups_done,
        "new_companies": new_total,
        "total_companies": store.get_company_count(),
        **client.stats,
    }


def _run_group_jobs(*, groups: list[dict[str, str]], max_workers: int, worker_fn) -> tuple[int, int]:
    if not groups:
        return 0, 0
    worker_count = max(1, min(int(max_workers or 1), len(groups)))
    if worker_count == 1:
        return _run_group_jobs_serial(groups=groups, worker_fn=worker_fn)
    return _run_group_jobs_parallel(groups=groups, worker_fn=worker_fn, max_workers=worker_count)


def _run_group_jobs_serial(*, groups: list[dict[str, str]], worker_fn) -> tuple[int, int]:
    groups_done = 0
    new_total = 0
    for group in groups:
        done, created = worker_fn(group)
        groups_done += done
        new_total += created
    return groups_done, new_total


def _run_group_jobs_parallel(*, groups: list[dict[str, str]], worker_fn, max_workers: int) -> tuple[int, int]:
    groups_done = 0
    new_total = 0
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mynavi-group") as executor:
        futures = {executor.submit(worker_fn, group): group for group in groups}
        for future in as_completed(futures):
            done, created = future.result()
            groups_done += done
            new_total += created
    return groups_done, new_total


def _process_group(
    *,
    store: MynaviStore,
    client: MynaviClient,
    group: dict[str, str],
    max_pages: int,
    detail_workers: int,
) -> tuple[int, int]:
    group_code = str(group["group_code"] or "").strip()
    scope = f"kana_group:{group_code}"
    checkpoint = store.get_checkpoint(scope)
    start_page = checkpoint["last_page"] + 1 if checkpoint and checkpoint["status"] == "running" else 1
    if max_pages > 0 and start_page > max_pages:
        LOGGER.warning("Mynavi 分组 %s 的断点页 %d 超出测试页上限 %d，回退到第 1 页", group_code, start_page, max_pages)
        start_page = 1
    first_html = client.fetch_list_page(group_code, start_page)
    if first_html is None:
        return 0, 0
    total_pages = parse_total_pages(first_html)
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    current_html = first_html
    current_page = start_page
    new_total = 0
    while current_page <= total_pages:
        cards = parse_company_cards(current_html)
        new_total += _fetch_and_store_details(store, client, cards, detail_workers)
        store.update_checkpoint(scope, current_page, total_pages, "running")
        if current_page <= 3 or current_page % 20 == 0 or current_page == total_pages:
            LOGGER.info("分组 %s 第 %d/%d 页：%d 家", group_code, current_page, total_pages, len(cards))
        current_page += 1
        if current_page > total_pages:
            break
        current_html = client.fetch_list_page(group_code, current_page)
        if current_html is None:
            break
    store.update_checkpoint(scope, min(current_page - 1, total_pages), total_pages, "done")
    return 1, new_total


def _fetch_and_store_details(store: MynaviStore, client: MynaviClient, cards: list[dict[str, str]], detail_workers: int) -> int:
    if not cards:
        return 0
    companies = _load_company_details(client, cards, detail_workers)
    return store.upsert_companies(companies)


def _load_company_details(client: MynaviClient, cards: list[dict[str, str]], detail_workers: int) -> list[dict[str, str]]:
    if detail_workers <= 1:
        return [_fetch_company_detail(client, card) for card in cards]
    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="mynavi-detail") as executor:
        futures = {executor.submit(_fetch_company_detail, client, card): card for card in cards}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _fetch_company_detail(client: MynaviClient, card: dict[str, str]) -> dict[str, str]:
    html_text = client.fetch_detail_page(card["detail_url"])
    detail = parse_company_detail(html_text or "")
    return {
        "company_id": card["company_id"],
        "company_name": detail["company_name"] or card["company_name"],
        "representative": detail["representative"],
        "website": detail["website"],
        "address": detail["address"] or card["address"],
        "industry": card["industry"],
        "detail_url": card["detail_url"],
    }
