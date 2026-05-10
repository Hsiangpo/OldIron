"""CNPJ Biz 列表与详情流水线。"""

from __future__ import annotations

import logging
import threading
import time

from .client import CnpjBizClient
from .config import CnpjBizConfig
from .selector import CnpjBizRepresentativeSelector
from .store import CnpjBizStore


LOGGER = logging.getLogger(__name__)
_START_URL = "https://cnpj.biz/empresas"


def prepare_pipeline(store: CnpjBizStore) -> None:
    store.requeue_running_tasks()
    store.requeue_failed_tasks()
    store.seed_start_page(_START_URL)


def run_pipeline_all(
    *,
    config: CnpjBizConfig,
    store: CnpjBizStore | None = None,
    auto_prepare: bool = True,
    close_store: bool = True,
) -> dict[str, int]:
    own_store = store is None
    active_store = store or CnpjBizStore(config.output_dir / "cnpjbiz_store.db")
    if auto_prepare:
        prepare_pipeline(active_store)
    client = CnpjBizClient(config)
    selector = CnpjBizRepresentativeSelector(config)
    stop_event = threading.Event()
    threads = _build_threads(active_store, client, selector, stop_event, config)
    for thread in threads:
        thread.start()
    _monitor(active_store, stop_event, config)
    for thread in threads:
        thread.join(timeout=2)
    progress = active_store.progress()
    client.close()
    selector.close()
    if close_store and own_store:
        active_store.close()
    return {
        "pages_done": progress.list_done,
        "companies": progress.companies_total,
        "final": progress.final_total,
    }


def run_pipeline_list_only(*, config: CnpjBizConfig) -> dict[str, int]:
    store = CnpjBizStore(config.output_dir / "cnpjbiz_store.db")
    prepare_pipeline(store)
    client = CnpjBizClient(config)
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_list_worker,
            args=(store, client, stop_event, config),
            daemon=True,
            name=f"cnpjbiz-list-{index + 1}",
        )
        for index in range(config.list_workers)
    ]
    for thread in threads:
        thread.start()
    while True:
        progress = store.progress()
        if progress.list_pending == 0 and progress.list_running == 0:
            stop_event.set()
            break
        time.sleep(config.queue_poll_interval)
    for thread in threads:
        thread.join(timeout=2)
    client.close()
    final_progress = store.progress()
    store.close()
    return {"pages_done": final_progress.list_done, "companies": final_progress.companies_total}


def run_pipeline_detail_only(*, config: CnpjBizConfig) -> dict[str, int]:
    store = CnpjBizStore(config.output_dir / "cnpjbiz_store.db")
    store.requeue_running_tasks()
    store.requeue_failed_tasks()
    client = CnpjBizClient(config)
    selector = CnpjBizRepresentativeSelector(config)
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_detail_worker,
            args=(store, client, selector, stop_event),
            daemon=True,
            name=f"cnpjbiz-detail-{index + 1}",
        )
        for index in range(config.detail_workers)
    ]
    for thread in threads:
        thread.start()
    while True:
        progress = store.progress()
        if progress.detail_pending == 0 and progress.detail_running == 0:
            stop_event.set()
            break
        time.sleep(config.queue_poll_interval)
    for thread in threads:
        thread.join(timeout=2)
    client.close()
    selector.close()
    final_progress = store.progress()
    store.close()
    return {"companies": final_progress.companies_total, "final": final_progress.final_total}


def _build_threads(
    store: CnpjBizStore,
    client: CnpjBizClient,
    selector: CnpjBizRepresentativeSelector,
    stop_event: threading.Event,
    config: CnpjBizConfig,
) -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    for index in range(config.list_workers):
        threads.append(
            threading.Thread(
                target=_list_worker,
                args=(store, client, stop_event, config),
                daemon=True,
                name=f"cnpjbiz-list-{index + 1}",
            )
        )
    for index in range(config.detail_workers):
        threads.append(
            threading.Thread(
                target=_detail_worker,
                args=(store, client, selector, stop_event),
                daemon=True,
                name=f"cnpjbiz-detail-{index + 1}",
            )
        )
    return threads


def _list_worker(store: CnpjBizStore, client: CnpjBizClient, stop_event: threading.Event, config: CnpjBizConfig) -> None:
    while not stop_event.is_set():
        if config.max_pages and store.progress().list_done >= config.max_pages:
            time.sleep(config.queue_poll_interval)
            continue
        task = store.claim_list_task()
        if task is None:
            time.sleep(config.queue_poll_interval)
            continue
        try:
            page = client.fetch_list_page(task.page_url)
            next_url = page.next_url
            if config.max_pages and store.progress().list_done + 1 >= config.max_pages:
                next_url = ""
            store.complete_list_task(
                task.page_url,
                task.depth,
                [_record_to_dict(record) for record in page.records],
                next_url,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz 列表页失败：url=%s error=%s", task.page_url, exc)
            store.defer_list_task(task.page_url)


def _detail_worker(
    store: CnpjBizStore,
    client: CnpjBizClient,
    selector: CnpjBizRepresentativeSelector,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        task = store.claim_detail_task()
        if task is None:
            time.sleep(1.0)
            continue
        try:
            profile = client.fetch_detail_profile(task.detail_url)
            representative = selector.choose(
                company_name=profile.company_name or task.company_name,
                cnpj=profile.cnpj or task.cnpj,
                candidates=profile.representative_candidates,
            )
            store.complete_detail_task(
                cnpj=task.cnpj,
                company_name=profile.company_name or task.company_name,
                trade_name=profile.trade_name,
                representative=representative,
                representative_candidates_json=profile.representative_candidates_json,
                emails="; ".join(profile.emails),
                phone=profile.phone,
                address=profile.address,
                city=profile.city,
                region=profile.region,
                status_text=profile.status_text,
                opened_at=profile.opened_at,
                evidence_url=profile.evidence_url or task.detail_url,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz 详情失败：cnpj=%s error=%s", task.cnpj, exc)
            store.fail_detail_task(task.cnpj)


def _monitor(store: CnpjBizStore, stop_event: threading.Event, config: CnpjBizConfig) -> None:
    last_log_at = 0.0
    while not stop_event.is_set():
        progress = store.progress()
        now = time.monotonic()
        if now - last_log_at >= config.log_interval_seconds:
            LOGGER.info(
                "CNPJ Biz 进度：list=%d/%d done=%d detail=%d/%d companies=%d final=%d",
                progress.list_running,
                progress.list_pending,
                progress.list_done,
                progress.detail_running,
                progress.detail_pending,
                progress.companies_total,
                progress.final_total,
            )
            last_log_at = now
        if config.max_pages and progress.list_done >= config.max_pages and progress.list_pending == 0 and progress.list_running == 0:
            if progress.detail_pending == 0 and progress.detail_running == 0:
                stop_event.set()
                return
        if progress.list_pending == 0 and progress.list_running == 0 and progress.detail_pending == 0 and progress.detail_running == 0:
            stop_event.set()
            return
        time.sleep(config.queue_poll_interval)


def _record_to_dict(record) -> dict[str, str]:
    return {
        "cnpj": record.cnpj,
        "company_name": record.company_name,
        "detail_url": record.detail_url,
        "city": record.city,
        "region": record.region,
        "status_text": record.status_text,
        "opened_at": record.opened_at,
    }
