"""DNB 美国 Pipeline 1：分类/分片 + 列表 + 详情。"""

from __future__ import annotations

import logging
import threading
import time

from .catalog import build_initial_segments
from .client import DnbCompanyInformationClient
from .config import DnbUsConfig
from .store import DnbUsStore


LOGGER = logging.getLogger(__name__)


def prepare_pipeline_list(*, store: DnbUsStore, config: DnbUsConfig) -> None:
    """准备 DNB 美国 P1 的初始队列。"""
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
    config: DnbUsConfig,
    store: DnbUsStore | None = None,
    auto_prepare: bool = True,
    close_store: bool = True,
) -> dict[str, int]:
    """执行 DNB 美国 P1。"""
    own_store = store is None
    active_store = store or DnbUsStore(config.output_dir / "dnb_store.db")
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
    store: DnbUsStore,
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


def _process_segment(store: DnbUsStore, client: DnbCompanyInformationClient, task, *, max_pages_per_segment: int) -> None:
    page = max(task.next_page, 1)
    while page <= max_pages_per_segment:
        LOGGER.info("DNB 列表请求：%s page=%d", task.segment_id, page)
        result = client.fetch_page(task.industry_path, page, task.country_iso_two_code)
        store.update_segment_page(task.segment_id, page + 1, result.matched_count)
        if not result.records:
            break
        store.upsert_companies(result.records)
        store.enqueue_detail_tasks(result.records)
        LOGGER.info(
            "DNB 列表完成：%s page=%d/%d rows=%d matched=%d",
            task.industry_path,
            result.current_page,
            result.total_pages,
            len(result.records),
            result.matched_count,
        )
        if result.current_page >= result.total_pages:
            break
        page += 1
    store.complete_segment(task.segment_id)


def _detail_worker(store: DnbUsStore, client: DnbCompanyInformationClient, stop_event: threading.Event) -> None:
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


def _monitor_p1(store: DnbUsStore, stop_event: threading.Event, config: DnbUsConfig) -> None:
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
