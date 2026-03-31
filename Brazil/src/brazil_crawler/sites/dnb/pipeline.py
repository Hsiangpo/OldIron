"""DNB 巴西 Pipeline 1：分类/分片 + 列表 + 详情。"""

from __future__ import annotations

import logging
import threading
import time

from .catalog import build_initial_segments
from .client import DnbCompanyInformationClient
from .config import DnbBrConfig
from .store import DnbBrStore


LOGGER = logging.getLogger(__name__)
_DNB_HARD_PAGE_LIMIT = 20


def prepare_pipeline_list(*, store: DnbBrStore, config: DnbBrConfig) -> None:
    """准备 DNB 巴西 P1 的初始队列。"""
    store.requeue_running_tasks()
    repaired = store.requeue_empty_detail_tasks()
    if repaired:
        LOGGER.info("DNB 详情空结果回补：%d", repaired)
    store.seed_segments(
        build_initial_segments(
            limit=config.max_segments,
            industry_paths=config.industry_paths,
        )
    )


def run_pipeline_list(
    *,
    config: DnbBrConfig,
    store: DnbBrStore | None = None,
    auto_prepare: bool = True,
    close_store: bool = True,
) -> dict[str, int]:
    """执行 DNB 巴西 P1。"""
    own_store = store is None
    active_store = store or DnbBrStore(config.output_dir / "dnb_store.db")
    if auto_prepare:
        prepare_pipeline_list(store=active_store, config=config)
    client = DnbCompanyInformationClient()
    client.refresh_cookies()
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_segment_worker,
            args=(active_store, client, stop_event, config.max_pages_per_segment),
            name=f"dnb-segment-{i+1}",
            daemon=True,
        )
        for i in range(config.segment_workers)
    ]
    threads.extend(
        threading.Thread(
            target=_detail_worker,
            args=(active_store, client, stop_event),
            name=f"dnb-detail-{i+1}",
            daemon=True,
        )
        for i in range(config.detail_workers)
    )
    for thread in threads:
        thread.start()
    _monitor_p1(active_store, stop_event, config)
    for thread in threads:
        thread.join(timeout=2)
    progress = active_store.progress()
    summary = {
        "segments_total": progress.segment_pending + progress.segment_running,
        "companies": progress.companies_total,
        "final": progress.final_total,
    }
    if close_store and own_store:
        active_store.close()
    return summary


def _segment_worker(
    store: DnbBrStore,
    client: DnbCompanyInformationClient,
    stop_event: threading.Event,
    max_pages_per_segment: int,
) -> None:
    while not stop_event.is_set():
        task = store.claim_segment()
        if task is None:
            time.sleep(1.0)
            continue
        try:
            LOGGER.info("DNB 列表切片开始：%s", task.segment_id)
            _process_segment(store, client, task, max_pages_per_segment=max_pages_per_segment)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("DNB 列表切片失败：%s | %s", task.segment_id, exc)
            store.defer_segment(task.segment_id)


def _process_segment(store: DnbBrStore, client: DnbCompanyInformationClient, task, *, max_pages_per_segment: int) -> None:
    first_page = max(task.next_page, 1)
    LOGGER.info("DNB 列表请求：%s page=%d", task.segment_id, first_page)
    result = client.fetch_page(
        task.industry_path,
        first_page,
        task.country_iso_two_code,
        task.region_name,
        task.city_name,
    )
    store.update_segment_page(task.segment_id, first_page + 1, result.matched_count)
    if _needs_geo_split(task, result, max_pages_per_segment):
        children = _build_child_segments(task, result.geos)
        seeded = store.seed_segments(children)
        LOGGER.info(
            "DNB 分裂切片：%s -> children=%d seeded=%d matched=%d",
            task.segment_id,
            len(children),
            seeded,
            result.matched_count,
        )
        store.complete_segment(task.segment_id)
        return
    _consume_segment_pages(store, client, task, result, first_page, max_pages_per_segment)
    store.complete_segment(task.segment_id)


def _consume_segment_pages(
    store: DnbBrStore,
    client: DnbCompanyInformationClient,
    task,
    first_result,
    first_page: int,
    max_pages_per_segment: int,
) -> None:
    effective_page_limit = min(max(int(max_pages_per_segment or 1), 1), _DNB_HARD_PAGE_LIMIT)
    page = first_page
    result = first_result
    while page <= effective_page_limit:
        if not result.records:
            break
        store.upsert_companies(result.records)
        store.enqueue_detail_tasks(result.records)
        LOGGER.info(
            "DNB 列表完成：%s region=%s city=%s page=%d/%d rows=%d matched=%d",
            task.industry_path,
            task.region_name or "-",
            task.city_name or "-",
            result.current_page,
            result.total_pages,
            len(result.records),
            result.matched_count,
        )
        if page >= effective_page_limit:
            break
        page += 1
        LOGGER.info("DNB 列表请求：%s page=%d", task.segment_id, page)
        result = client.fetch_page(
            task.industry_path,
            page,
            task.country_iso_two_code,
            task.region_name,
            task.city_name,
        )
        store.update_segment_page(task.segment_id, page + 1, result.matched_count)
    _warn_if_truncated(task, result, effective_page_limit)


def _needs_geo_split(task, result, max_pages_per_segment: int) -> bool:
    if not result.geos:
        return False
    if task.city_name:
        return False
    return result.matched_count > _DNB_HARD_PAGE_LIMIT * max(result.page_size, 1)


def _build_child_segments(task, geos: list[dict[str, str | int]]) -> list[dict[str, str | int]]:
    children: list[dict[str, str | int]] = []
    for geo in geos:
        href = str(geo.get("href", "") or "").strip()
        parts = [part.strip() for part in href.split(".") if part.strip()]
        if len(parts) < 2:
            continue
        region_name = parts[1] if len(parts) >= 2 else ""
        city_name = parts[2] if len(parts) >= 3 else ""
        segment_id = f"{task.industry_path}|{task.country_iso_two_code}|{region_name}|{city_name}"
        children.append(
            {
                "segment_id": segment_id,
                "segment_type": "geo",
                "industry_path": task.industry_path,
                "country_iso_two_code": task.country_iso_two_code,
                "region_name": region_name,
                "city_name": city_name,
                "expected_count": int(geo.get("quantity") or 0),
                "next_page": 1,
                "status": "pending",
            }
        )
    return children


def _warn_if_truncated(task, result, effective_page_limit: int) -> None:
    capacity = effective_page_limit * max(result.page_size, 1)
    if result.matched_count <= capacity:
        return
    LOGGER.warning(
        "DNB 切片可能截断：segment=%s matched=%d capacity=%d totalPages=%d",
        task.segment_id,
        result.matched_count,
        capacity,
        result.total_pages,
    )


def _detail_worker(store: DnbBrStore, client: DnbCompanyInformationClient, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        task = store.claim_detail_task()
        if task is None:
            time.sleep(1.0)
            continue
        try:
            profile = client.fetch_detail_profile(task.detail_url)
            store.complete_detail_task(task.duns, profile.representative, profile.website, profile.phone)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("DNB 详情失败：%s | %s", task.duns, exc)
            store.fail_detail_task(task.duns)


def _monitor_p1(store: DnbBrStore, stop_event: threading.Event, config: DnbBrConfig) -> None:
    last_log = 0.0
    while not stop_event.is_set():
        progress = store.progress()
        now = time.monotonic()
        if now - last_log >= config.log_interval_seconds:
            LOGGER.info(
                "DNB P1 进度：segments=%d/%d detail=%d/%d companies=%d",
                progress.segment_running,
                progress.segment_pending,
                progress.detail_running,
                progress.detail_pending,
                progress.companies_total,
            )
            last_log = now
        if progress.segment_pending == 0 and progress.segment_running == 0 and progress.detail_pending == 0 and progress.detail_running == 0:
            stop_event.set()
            return
        time.sleep(config.queue_poll_interval)
