"""Italy DNB pipeline。"""

from __future__ import annotations

import logging
import threading
import time

from .catalog import build_initial_segments
from .client import DnbCompanyInformationClient
from .config import ItalyDnbConfig
from .email_service import ItalyDnbEmailService
from .email_service import ItalyDnbEmailSettings
from .store import ItalyDnbStore
from .verif_client import VerifChallengeError
from .verif_client import VerifClient


LOGGER = logging.getLogger(__name__)
_DNB_HARD_PAGE_LIMIT = 20


def prepare_pipeline_list(*, store: ItalyDnbStore, config: ItalyDnbConfig) -> None:
    store.requeue_running_tasks()
    store.seed_segments(
        build_initial_segments(
            limit=config.max_segments,
            industry_paths=config.industry_paths,
        )
    )


def run_pipeline_list(
    *,
    config: ItalyDnbConfig,
    store: ItalyDnbStore | None = None,
    auto_prepare: bool = True,
    close_store: bool = True,
) -> dict[str, int]:
    own_store = store is None
    active_store = store or ItalyDnbStore(config.output_dir / "dnb_store.db")
    if auto_prepare:
        prepare_pipeline_list(store=active_store, config=config)
    client = DnbCompanyInformationClient()
    client.refresh_cookies(force=False)
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_segment_worker,
            args=(active_store, client, stop_event, config.max_pages_per_segment),
            name=f"it-dnb-segment-{index + 1}",
            daemon=True,
        )
        for index in range(config.segment_workers)
    ]
    for thread in threads:
        thread.start()
    _monitor_segment_phase(active_store, stop_event, config)
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


def run_pipeline_verif(*, store: ItalyDnbStore, config: ItalyDnbConfig, stop_event) -> None:
    client = VerifClient(
        profile_dir=config.output_dir / "session" / "verif_profile",
        proxy_url=config.proxy_url,
        timeout_seconds=config.verif_timeout_seconds,
        headless=config.verif_headless,
    )
    try:
        while not stop_event.is_set():
            task = store.claim_verif_task()
            if task is None:
                time.sleep(1.0)
                continue
            try:
                match = client.search_company(task.company_name)
                if match is None:
                    store.complete_verif_task(
                        task.duns,
                        website="",
                        representative="",
                        evidence_url="",
                    )
                    continue
                store.complete_verif_task(
                    task.duns,
                    company_name=match.company_name,
                    website=match.website,
                    representative=match.representative,
                    evidence_url=match.company_url or match.search_url,
                )
            except VerifChallengeError as exc:
                LOGGER.warning("Verif challenge 未通过：%s | %s", task.company_name, exc)
                store.fail_verif_task(task.duns)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Verif 补充失败：%s | %s", task.company_name, exc)
                store.fail_verif_task(task.duns)
    finally:
        client.close()


def run_pipeline_email(*, store: ItalyDnbStore, config: ItalyDnbConfig, stop_event) -> None:
    service = ItalyDnbEmailService(
        ItalyDnbEmailSettings(
            proxy_url=config.proxy_url,
            email_page_soft_limit=config.email_page_soft_limit,
            email_page_hard_limit=config.email_page_hard_limit,
            email_total_hard_limit=config.email_total_hard_limit,
            email_stop_same_domain_count=config.email_stop_same_domain_count,
        )
    )
    try:
        while not stop_event.is_set():
            task = store.claim_site_task()
            if task is None:
                time.sleep(1.0)
                continue
            try:
                result = service.discover_emails(task.website)
                store.complete_site_task(
                    task.duns,
                    emails=result.emails,
                    evidence_url=result.evidence_url or task.website,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("官网邮箱抓取失败：%s | %s", task.website, exc)
                store.fail_site_task(task.duns)
    finally:
        service.close()


def _segment_worker(
    store: ItalyDnbStore,
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
            LOGGER.info("DNB Italy 切片开始：%s", task.segment_id)
            _process_segment(store, client, task, max_pages_per_segment=max_pages_per_segment)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("DNB Italy 切片失败：%s | %s", task.segment_id, exc)
            store.defer_segment(task.segment_id)


def _process_segment(store: ItalyDnbStore, client: DnbCompanyInformationClient, task, *, max_pages_per_segment: int) -> None:
    first_page = max(task.next_page, 1)
    result = _fetch_segment_page(client, task, first_page)
    store.update_segment_page(task.segment_id, first_page + 1, result.matched_count)
    if _needs_geo_split(task, result):
        children = _build_child_segments(task, result.geos)
        seeded = store.seed_segments(children)
        LOGGER.info(
            "DNB Italy 分裂切片：%s -> children=%d seeded=%d matched=%d",
            task.segment_id,
            len(children),
            seeded,
            result.matched_count,
        )
        store.complete_segment(task.segment_id)
        return
    _consume_segment_pages(store, client, task, result, first_page, max_pages_per_segment)
    store.complete_segment(task.segment_id)


def _consume_segment_pages(store: ItalyDnbStore, client: DnbCompanyInformationClient, task, first_result, first_page: int, max_pages_per_segment: int) -> None:
    effective_page_limit = min(max(int(max_pages_per_segment or 1), 1), _DNB_HARD_PAGE_LIMIT)
    page = first_page
    result = first_result
    while page <= effective_page_limit:
        if not result.records:
            break
        store.upsert_companies(result.records)
        store.enqueue_verif_tasks(result.records)
        LOGGER.info(
            "DNB Italy 列表完成：industry=%s region=%s city=%s page=%d/%d rows=%d matched=%d",
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
        result = _fetch_segment_page(client, task, page)
        store.update_segment_page(task.segment_id, page + 1, result.matched_count)
    _warn_if_truncated(task, result, effective_page_limit)


def _fetch_segment_page(client: DnbCompanyInformationClient, task, page_number: int):
    return client.fetch_page(
        task.industry_path,
        page_number,
        task.country_iso_two_code,
        task.region_name,
        task.city_name,
    )


def _needs_geo_split(task, result) -> bool:
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
        children.append(
            {
                "segment_id": f"{task.industry_path}|{task.country_iso_two_code}|{region_name}|{city_name}",
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
        "DNB Italy 切片可能截断：segment=%s matched=%d capacity=%d totalPages=%d",
        task.segment_id,
        result.matched_count,
        capacity,
        result.total_pages,
    )


def _monitor_segment_phase(store: ItalyDnbStore, stop_event: threading.Event, config: ItalyDnbConfig) -> None:
    while True:
        progress = store.progress()
        if progress.segment_pending == 0 and progress.segment_running == 0:
            stop_event.set()
            break
        LOGGER.info(
            "DNB Italy P1 进度：segments=%d/%d verif=%d/%d companies=%d",
            progress.segment_running,
            progress.segment_pending,
            progress.verif_running,
            progress.verif_pending,
            progress.companies_total,
        )
        time.sleep(config.queue_poll_interval)
