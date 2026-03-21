"""丹麦 Virk 主流程。"""

from __future__ import annotations

import logging
import math
import threading
import time
from pathlib import Path

from denmark_crawler.fc_email.client import FirecrawlError
from denmark_crawler.fc_email.domain_cache import FirecrawlDomainCache
from denmark_crawler.fc_email.email_service import FirecrawlEmailService
from denmark_crawler.fc_email.email_service import FirecrawlEmailSettings
from denmark_crawler.google_maps import GoogleMapsClient
from denmark_crawler.google_maps import GoogleMapsConfig
from denmark_crawler.virk.client import VirkClient
from denmark_crawler.virk.config import VirkDenmarkConfig
from denmark_crawler.virk.models import VirkSearchCompany
from denmark_crawler.virk.store import DetailTask
from denmark_crawler.virk.store import FirecrawlTask
from denmark_crawler.virk.store import GMapTask
from denmark_crawler.virk.store import VirkDenmarkStore


logger = logging.getLogger(__name__)


def _backoff_seconds(attempt: int, cap_seconds: float) -> float:
    return min(float(2 ** max(attempt, 1)), max(cap_seconds, 1.0))


def _build_gmap_queries(task: GMapTask) -> list[str]:
    values = [
        f"{task.company_name} {task.city} Denmark".strip(),
        f"{task.company_name} {task.postal_code} Denmark".strip(),
        f"{task.company_name} Denmark".strip(),
    ]
    queries: list[str] = []
    for value in values:
        if value and value not in queries:
            queries.append(value)
    return queries


class VirkDenmarkPipelineRunner:
    """协调 Virk -> GMap -> Firecrawl。"""

    def __init__(
        self,
        *,
        config: VirkDenmarkConfig,
        client: VirkClient,
        skip_virk: bool,
        skip_gmap: bool,
        skip_firecrawl: bool,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_virk = skip_virk
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = skip_firecrawl
        self.store = VirkDenmarkStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.search_done = threading.Event()
        self._search_workers_remaining = config.search_workers
        self._search_workers_lock = threading.Lock()
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_key_pool = None
        self._firecrawl_key_pool_lock = threading.Lock()
        self._firecrawl_domain_cache = FirecrawlDomainCache(self.config.project_root / "output" / "firecrawl_cache.db")
        self._firecrawl_settings = FirecrawlEmailSettings(
            keys_inline=list(self.config.firecrawl_keys_inline or []),
            keys_file=Path(self.config.firecrawl_keys_file or (self.config.project_root / "output" / "firecrawl_keys.txt")),
            pool_db=Path(self.config.firecrawl_pool_db or (self.config.project_root / "output" / "cache" / "firecrawl_keys.db")),
            base_url=self.config.firecrawl_base_url,
            timeout_seconds=self.config.firecrawl_timeout_seconds,
            max_retries=self.config.firecrawl_max_retries,
            key_per_limit=self.config.firecrawl_key_per_limit,
            key_wait_seconds=self.config.firecrawl_key_wait_seconds,
            key_cooldown_seconds=self.config.firecrawl_key_cooldown_seconds,
            key_failure_threshold=self.config.firecrawl_key_failure_threshold,
            llm_api_key=self.config.llm_api_key,
            llm_base_url=self.config.llm_base_url,
            llm_model=self.config.llm_model,
            llm_reasoning_effort=self.config.llm_reasoning_effort,
            llm_timeout_seconds=self.config.llm_timeout_seconds,
            prefilter_limit=self.config.firecrawl_prefilter_limit,
            llm_pick_count=self.config.firecrawl_llm_pick_count,
            extract_max_urls=self.config.firecrawl_extract_max_urls,
            zero_retry_seconds=self.config.firecrawl_zero_retry_seconds,
            contact_form_retry_seconds=self.config.firecrawl_contact_form_retry_seconds,
        )

    def run(self) -> None:
        self.config.validate(skip_firecrawl=self.skip_firecrawl)
        self.store.ensure_search_seed()
        max_pages = math.ceil(self.config.max_companies / self.config.page_size) if self.config.max_companies > 0 else None
        expanded = self.store.expand_search_pages_from_known_total(
            page_size=self.config.page_size,
            max_pages=max_pages,
        )
        if expanded:
            logger.info("Virk 已按已知总量补齐搜索页队列：新增=%d", expanded)
        revived = self.store.requeue_retryable_failed_tasks()
        revived_total = sum(revived.values())
        if revived_total:
            logger.info(
                "Virk 已回收可重试失败任务：detail=%d gmap=%d firecrawl=%d",
                revived["detail_queue"],
                revived["gmap_queue"],
                revived["firecrawl_queue"],
            )
        workers: list[threading.Thread] = []
        if not self.skip_virk:
            for idx in range(self.config.search_workers):
                workers.append(threading.Thread(target=self._search_worker, name=f"Virk-Search-{idx+1}", daemon=True))
            for idx in range(self.config.detail_workers):
                workers.append(threading.Thread(target=self._detail_worker, name=f"Virk-Detail-{idx+1}", daemon=True))
        else:
            self.search_done.set()
        if not self.skip_gmap:
            for idx in range(self.config.gmap_workers):
                workers.append(threading.Thread(target=self._gmap_worker, name=f"Virk-GMap-{idx+1}", daemon=True))
        if not self.skip_firecrawl:
            for idx in range(self.config.firecrawl_workers):
                workers.append(threading.Thread(target=self._firecrawl_worker, name=f"Virk-Firecrawl-{idx+1}", daemon=True))
        for worker in workers:
            worker.start()
        try:
            self._monitor_until_done()
        finally:
            self.stop_event.set()
            for worker in workers:
                worker.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self._firecrawl_domain_cache.close()
            self.store.close()

    def _monitor_until_done(self) -> None:
        last_log = 0.0
        last_snapshot = 0.0
        last_retry = 0.0
        while not self.stop_event.is_set():
            now = time.monotonic()
            self.store.requeue_stale_running_tasks(older_than_seconds=self.config.stale_running_requeue_seconds)
            if now - last_retry >= 60.0:
                self.store.requeue_expired_firecrawl_tasks()
                last_retry = now
            if now - last_snapshot >= 30.0:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snapshot = now
            if now - last_log >= 10.0:
                stats = self.store.get_stats()
                logger.info(
                    "进度：search=%d/%d detail=%d/%d gmap=%d/%d firecrawl=%d/%d final=%d",
                    stats["search_pages_done"],
                    stats["search_pages_total"],
                    stats["companies_detail_done"],
                    stats["companies_total"],
                    stats["gmap_running"],
                    stats["gmap_pending"],
                    stats["firecrawl_running"],
                    stats["firecrawl_pending"],
                    stats["final_total"],
                )
                last_log = now
            if self.search_done.is_set() and self._queues_idle():
                return
            time.sleep(self.config.queue_poll_interval)

    def _queues_idle(self) -> bool:
        stats = self.store.get_stats()
        return (
            stats["detail_pending"]
            + stats["detail_running"]
            + stats["gmap_pending"]
            + stats["gmap_running"]
            + stats["firecrawl_pending"]
            + stats["firecrawl_running"]
        ) == 0

    def _search_worker(self) -> None:
        try:
            max_pages = math.ceil(self.config.max_companies / self.config.page_size) if self.config.max_companies > 0 else None
            while not self.stop_event.is_set():
                page_index = self.store.claim_search_page()
                if page_index is None:
                    if self.store.has_search_work():
                        time.sleep(self.config.queue_poll_interval)
                        continue
                    break
                logger.info("Virk 搜索页：%d", page_index)
                try:
                    rows, total_hits = self.client.search_companies(page_index=page_index, page_size=self.config.page_size)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Virk 搜索页失败：page=%d error=%s", page_index, exc)
                    self.store.reset_search_page(page_index)
                    time.sleep(min(_backoff_seconds(1, self.config.retry_backoff_cap_seconds), 5.0))
                    continue
                if self.config.max_companies > 0:
                    already = self.store.get_stats()["companies_total"]
                    remaining = max(self.config.max_companies - already, 0)
                    rows = rows[:remaining]
                for row in rows:
                    self.store.upsert_search_company(row)
                self.store.mark_search_page_done(page_index, total_hits=total_hits, page_size=self.config.page_size, max_pages=max_pages)
                if not rows or (max_pages is not None and page_index + 1 >= max_pages):
                    break
        finally:
            with self._search_workers_lock:
                self._search_workers_remaining -= 1
                if self._search_workers_remaining <= 0:
                    logger.info("Virk 搜索主流程完成。")
                    self.search_done.set()

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_detail_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_detail_task(task)

    def _process_detail_task(self, task: DetailTask) -> None:
        company = self.store.get_company(task.cvr) or {}
        logger.info("Virk 详情开始：%s | %s", task.cvr, str(company.get("company_name", "")).strip())
        try:
            record = self.client.fetch_company_record(self._to_search_company(company))
        except Exception as exc:  # noqa: BLE001
            attempt = task.retries + 1
            if attempt >= self.config.detail_task_max_retries:
                self.store.mark_detail_failed(cvr=task.cvr, error_text=str(exc))
                return
            self.store.defer_detail_task(
                cvr=task.cvr,
                retries=attempt,
                delay_seconds=_backoff_seconds(attempt, self.config.retry_backoff_cap_seconds),
                error_text=str(exc),
            )
            return
        self.store.upsert_detail_company(record)
        logger.info("Virk 详情完成：%s | 代表人=%s | 邮箱=%d", record.cvr, record.representative, len(record.emails))

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_gmap_task(task)

    def _process_gmap_task(self, task: GMapTask) -> None:
        logger.info("Virk GMap 开始：%s | %s", task.cvr, task.company_name)
        try:
            result = None
            for query in _build_gmap_queries(task):
                logger.info("Virk GMap 查询：%s | %s", task.cvr, query)
                candidate = self._get_gmap_client().search_company_profile(query, company_name=task.company_name)
                if candidate.website:
                    result = candidate
                    break
                if result is None:
                    result = candidate
        except Exception as exc:  # noqa: BLE001
            attempt = task.retries + 1
            if attempt >= self.config.gmap_max_retries:
                self.store.mark_gmap_failed(cvr=task.cvr, error_text=str(exc))
                return
            self.store.defer_gmap_task(
                cvr=task.cvr,
                retries=attempt,
                delay_seconds=_backoff_seconds(attempt, self.config.retry_backoff_cap_seconds),
                error_text=str(exc),
            )
            return
        self.store.mark_gmap_done(
            cvr=task.cvr,
            website=result.website if result else "",
            source="gmap" if result and result.website else "",
            phone=result.phone if result else "",
            company_name=result.company_name if result else "",
        )

    def _firecrawl_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: FirecrawlTask) -> None:
        if not task.website:
            self.store.mark_firecrawl_failed(cvr=task.cvr, error_text="缺少官网")
            return
        logger.info("Virk Firecrawl 开始：%s | 域名=%s", task.cvr, task.domain)
        try:
            result = self._get_firecrawl_service().discover_emails(
                company_name=task.company_name,
                homepage=task.website,
                domain=task.domain,
            )
        except FirecrawlError as exc:
            attempt = task.retries + 1
            delay = self._firecrawl_delay(attempt, exc)
            if attempt >= self.config.firecrawl_task_max_retries:
                self.store.mark_firecrawl_failed(cvr=task.cvr, error_text=str(exc))
                return
            self.store.defer_firecrawl_task(cvr=task.cvr, retries=attempt, delay_seconds=delay, error_text=str(exc))
            logger.warning("Virk Firecrawl 重试：%s | 域名=%s | 第%d次 | 等待=%.1fs | 原因=%s", task.cvr, task.domain, attempt, delay, exc)
            return
        self.store.mark_firecrawl_done(
            cvr=task.cvr,
            emails=result.emails,
            retry_after_seconds=self.config.firecrawl_zero_retry_seconds if not result.emails else 0.0,
        )
        logger.info("Virk Firecrawl 完成：%s | 域名=%s | 邮箱=%d", task.cvr, task.domain, len(result.emails))

    def _firecrawl_delay(self, attempt: int, exc: FirecrawlError) -> float:
        if exc.code == "firecrawl_5xx":
            return 0.0
        if exc.code == "firecrawl_429" and exc.retry_after:
            return min(float(exc.retry_after), self.config.retry_backoff_cap_seconds)
        return min(_backoff_seconds(attempt, self.config.retry_backoff_cap_seconds), self.config.retry_backoff_cap_seconds)

    def _get_gmap_client(self) -> GoogleMapsClient:
        if not hasattr(self._gmap_local, "client"):
            self._gmap_local.client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="dk"))
        return self._gmap_local.client

    def _get_firecrawl_key_pool(self):
        if self._firecrawl_key_pool is not None:
            return self._firecrawl_key_pool
        with self._firecrawl_key_pool_lock:
            if self._firecrawl_key_pool is None:
                self._firecrawl_key_pool = FirecrawlEmailService.build_key_pool(self._firecrawl_settings)
        return self._firecrawl_key_pool

    def _get_firecrawl_service(self) -> FirecrawlEmailService:
        if not hasattr(self._firecrawl_local, "service"):
            self._firecrawl_local.service = FirecrawlEmailService(
                self._firecrawl_settings,
                key_pool=self._get_firecrawl_key_pool(),
            )
        return self._firecrawl_local.service

    def _to_search_company(self, company: dict[str, object]) -> VirkSearchCompany:
        return VirkSearchCompany(
            cvr=str(company.get("cvr", "")).strip(),
            company_name=str(company.get("company_name", "")).strip(),
            address=str(company.get("address", "")).strip(),
            city=str(company.get("city", "")).strip(),
            postal_code=str(company.get("postal_code", "")).strip(),
            status=str(company.get("status", "")).strip(),
            company_form=str(company.get("company_form", "")).strip(),
            main_industry=str(company.get("main_industry", "")).strip(),
            start_date=str(company.get("start_date", "")).strip(),
            phone=str(company.get("phone", "")).strip(),
            emails=[],
        )


def run_virk_pipeline(
    *,
    config: VirkDenmarkConfig,
    client: VirkClient,
    skip_virk: bool = False,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
) -> None:
    runner = VirkDenmarkPipelineRunner(
        config=config,
        client=client,
        skip_virk=skip_virk,
        skip_gmap=skip_gmap,
        skip_firecrawl=skip_firecrawl,
    )
    runner.run()
