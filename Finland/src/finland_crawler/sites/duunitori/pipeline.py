"""Duunitori 主流程 — 列表页翻页 → 详情 → GMap → Protocol+LLM。"""

from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

from oldiron_core.fc_email.domain_cache import FirecrawlDomainCache
from oldiron_core.fc_email.email_service import (
    FirecrawlEmailService, FirecrawlEmailSettings, EmailDiscoveryResult,
)
from oldiron_core.fc_email.client import FirecrawlError
from oldiron_core.google_maps import GoogleMapsClient, GoogleMapsConfig, GoogleMapsPlaceResult
from finland_crawler.sites.duunitori.client import DuunitoriClient
from finland_crawler.sites.duunitori.config import DuunitoriConfig
from finland_crawler.sites.duunitori.store import (
    DuunitoriDetailTask, DuunitoriGMapTask, DuunitoriFirecrawlTask, DuunitoriStore,
)

try:
    from oldiron_core.protocol_crawler.client import SiteCrawlClient, SiteCrawlConfig
except ImportError:
    SiteCrawlClient = None  # type: ignore[assignment,misc]
    SiteCrawlConfig = None  # type: ignore[assignment,misc]

LOGGER = logging.getLogger(__name__)


def _retry_delay(retries: int, cap: float) -> float:
    return min(float(2 ** max(int(retries), 0)), float(cap))


def _gmap_queries(task: DuunitoriGMapTask) -> list[str]:
    queries: list[str] = []
    for value in (
        f"{task.company_name} {task.city} Finland",
        f"{task.company_name} Finland",
        task.company_name,
    ):
        text = re.sub(r"\s+", " ", str(value or "").strip())
        if text and text not in queries:
            queries.append(text)
    return queries


class DuunitoriPipelineRunner:
    """Duunitori 四阶段流水线。"""

    def __init__(
        self, *, config: DuunitoriConfig, client: DuunitoriClient,
        skip_gmap: bool = False, skip_firecrawl: bool = False,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = skip_firecrawl
        self.store = DuunitoriStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.search_done = threading.Event()
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_domain_cache = FirecrawlDomainCache(
            config.project_root / "output" / "duunitori_firecrawl_cache.db"
        )
        self._firecrawl_settings = FirecrawlEmailSettings(
            keys_inline=[], keys_file=Path(config.firecrawl_keys_file or (config.project_root / "output" / "firecrawl_keys.txt")),
            pool_db=Path(config.firecrawl_pool_db or (config.project_root / "output" / "cache" / "firecrawl_keys.db")),
            base_url=config.firecrawl_base_url, timeout_seconds=config.firecrawl_timeout_seconds,
            max_retries=config.firecrawl_max_retries, key_per_limit=config.firecrawl_key_per_limit,
            key_wait_seconds=int(config.firecrawl_key_wait_seconds),
            key_cooldown_seconds=int(config.firecrawl_key_cooldown_seconds),
            key_failure_threshold=config.firecrawl_key_failure_threshold,
            llm_api_key=config.llm_api_key, llm_base_url=config.llm_base_url,
            llm_model=config.llm_model, llm_reasoning_effort=config.llm_reasoning_effort,
            llm_timeout_seconds=config.llm_timeout_seconds,
            prefilter_limit=config.firecrawl_prefilter_limit,
            llm_pick_count=config.firecrawl_llm_pick_count,
            extract_max_urls=config.firecrawl_extract_max_urls,
            zero_retry_seconds=config.firecrawl_zero_retry_seconds,
            contact_form_retry_seconds=config.firecrawl_contact_form_retry_seconds,
            crawl_backend=config.crawl_backend,
        )

    def run(self) -> None:
        self.config.validate()
        recovered = self.store.requeue_stale_running_tasks(older_than_seconds=self.config.stale_running_requeue_seconds)
        if recovered:
            LOGGER.info("Duunitori 已回收陈旧任务：%s", recovered)
        workers = self._build_workers()
        for w in workers:
            w.start()
            if w.name.startswith("duu-email-"):
                time.sleep(0.3)

        def _bg_search() -> None:
            try:
                self._run_search()
            except Exception as exc:
                LOGGER.error("Duunitori 搜索异常：%s", exc)
            finally:
                self.search_done.set()

        threading.Thread(target=_bg_search, name="duu-search", daemon=True).start()
        try:
            self._monitor()
        finally:
            self.stop_event.set()
            for w in workers:
                w.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self.store.close()

    def _run_search(self) -> None:
        pages_done, _ = self.store.get_search_progress()
        page = max(pages_done, 1)
        LOGGER.info("Duunitori 搜索开始：从第 %d 页续跑", page)
        total_collected = 0
        while not self.stop_event.is_set():
            try:
                jobs, has_next = self.client.fetch_list_page(page)
            except Exception as exc:
                LOGGER.warning("Duunitori 列表第 %d 页失败：%s", page, exc)
                time.sleep(5)
                continue
            for job in jobs:
                self.store.upsert_job(job, page)
            total_collected += len(jobs)
            self.store.update_search_progress(total_collected, page)
            LOGGER.info("Duunitori 搜索：page=%d rows=%d total=%d", page, len(jobs), total_collected)
            if not has_next or page >= self.config.max_list_pages:
                break
            page += 1
            time.sleep(self.config.request_delay)

    def _build_workers(self) -> list[threading.Thread]:
        workers = [
            threading.Thread(target=self._detail_worker, name=f"duu-detail-{i+1}", daemon=True)
            for i in range(self.config.detail_workers)
        ]
        if not self.skip_gmap:
            workers.extend(
                threading.Thread(target=self._gmap_worker, name=f"duu-gmap-{i+1}", daemon=True)
                for i in range(self.config.gmap_workers)
            )
        if not self.skip_firecrawl:
            workers.extend(
                threading.Thread(target=self._email_worker, name=f"duu-email-{i+1}", daemon=True)
                for i in range(self.config.firecrawl_workers)
            )
        return workers

    def _monitor(self) -> None:
        last_log = 0.0
        last_snap = 0.0
        while not self.stop_event.is_set():
            now = time.monotonic()
            self.store.requeue_stale_running_tasks(older_than_seconds=self.config.stale_running_requeue_seconds)
            if now - last_snap >= 30.0:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snap = now
            if now - last_log >= 10.0:
                p = self.store.get_progress()
                LOGGER.info(
                    "D 进度：search=%d detail=%d/%d gmap=%d+%d email=%d+%d jobs=%d final=%d",
                    p.search_done_pages, p.detail_done, p.detail_total,
                    p.gmap_running, p.gmap_pending,
                    p.firecrawl_running, p.firecrawl_pending,
                    p.jobs_total, p.final_total,
                )
                last_log = now
            if self.search_done.is_set() and self._all_idle():
                return
            time.sleep(self.config.queue_poll_interval)

    def _all_idle(self) -> bool:
        p = self.store.get_progress()
        busy = p.detail_pending + p.detail_running
        if not self.skip_gmap:
            busy += p.gmap_pending + p.gmap_running
        if not self.skip_firecrawl:
            busy += p.firecrawl_pending + p.firecrawl_running
        return busy == 0

    # ---- 详情 Worker ----

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_detail_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            try:
                detail = self.client.fetch_detail(task.url)
                self.store.upsert_job_detail(task.job_id, detail)
                self.store.mark_detail_done(task.job_id)
            except Exception as exc:
                retries = task.retries + 1
                if retries >= self.config.max_task_retries:
                    self.store.mark_detail_failed(task.job_id, str(exc))
                else:
                    self.store.defer_detail_task(
                        task.job_id, retries,
                        _retry_delay(retries, self.config.retry_backoff_cap_seconds), str(exc),
                    )

    # ---- GMap Worker ----

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._do_gmap(task)

    def _do_gmap(self, task: DuunitoriGMapTask) -> None:
        try:
            result = GoogleMapsPlaceResult()
            gmap_client = self._get_gmap_client()
            for query in _gmap_queries(task):
                candidate = gmap_client.search_company_profile(query, company_name=task.company_name)
                if candidate.score >= result.score:
                    result = candidate
                if result.website:
                    break
        except Exception as exc:
            retries = task.retries + 1
            if retries >= self.config.gmap_max_retries:
                self.store.mark_gmap_failed(job_id=task.job_id, error_text=str(exc))
                return
            self.store.defer_gmap_task(
                job_id=task.job_id, retries=retries,
                delay_seconds=_retry_delay(retries, self.config.retry_backoff_cap_seconds),
                error_text=str(exc),
            )
            return
        self.store.mark_gmap_done(
            job_id=task.job_id, website=result.website,
            phone=result.phone, company_name=result.company_name,
        )

    def _get_gmap_client(self) -> GoogleMapsClient:
        client = getattr(self._gmap_local, "client", None)
        if client is None:
            client = GoogleMapsClient(GoogleMapsConfig(proxy_url=self.config.proxy_url))
            self._gmap_local.client = client
        return client

    # ---- Protocol+LLM Worker ----

    def _email_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: DuunitoriFirecrawlTask) -> None:
        if not task.website:
            self.store.mark_firecrawl_failed(job_id=task.job_id, error_text="缺少官网")
            return
        decision = self._firecrawl_domain_cache.prepare_lookup(task.domain)
        if decision.status == "done":
            self.store.mark_firecrawl_done(job_id=task.job_id, emails=decision.emails or [])
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                job_id=task.job_id, retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.config.queue_poll_interval),
                error_text="等待同域名",
            )
            return
        try:
            result = self._discover_emails(
                company_name=task.company_name, homepage=task.website, domain=task.domain,
            )
        except Exception as exc:
            self._handle_firecrawl_failure(task, exc)
            return
        self._firecrawl_domain_cache.mark_done(task.domain, result.emails)
        self.store.mark_firecrawl_done(
            job_id=task.job_id, emails=result.emails,
            representative=result.representative,
            company_name=result.company_name,
            evidence_url=result.evidence_url,
        )

    def _handle_firecrawl_failure(self, task: DuunitoriFirecrawlTask, exc: Exception) -> None:
        attempt = task.retries + 1
        delay = _retry_delay(attempt, self.config.retry_backoff_cap_seconds)
        if isinstance(exc, FirecrawlError) and getattr(exc, 'code', '') in {"firecrawl_401", "firecrawl_402"}:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(job_id=task.job_id, error_text=str(exc))
            return
        if attempt >= self.config.firecrawl_task_max_retries:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(job_id=task.job_id, error_text=str(exc))
            return
        self._firecrawl_domain_cache.defer(task.domain, delay_seconds=delay, error_text=str(exc))
        self.store.defer_firecrawl_task(
            job_id=task.job_id, retries=attempt, delay_seconds=delay, error_text=str(exc),
        )

    def _get_firecrawl_service(self) -> FirecrawlEmailService:
        service = getattr(self._firecrawl_local, "service", None)
        if service is not None:
            return service
        firecrawl_client = None
        if self.config.crawl_backend == "protocol" and SiteCrawlClient is not None:
            firecrawl_client = SiteCrawlClient(SiteCrawlConfig(
                timeout_seconds=self.config.firecrawl_timeout_seconds,
                max_retries=self.config.firecrawl_max_retries,
                proxy_url=self.config.proxy_url,
            ))
        self._firecrawl_local.service = FirecrawlEmailService(
            self._firecrawl_settings, key_pool=None, firecrawl_client=firecrawl_client,
        )
        return self._firecrawl_local.service

    def _discover_emails(self, *, company_name: str, homepage: str, domain: str) -> EmailDiscoveryResult:
        service = self._get_firecrawl_service()
        return service.discover_emails(company_name=company_name, homepage=homepage, domain=domain)


def run_duunitori_pipeline(
    config: DuunitoriConfig, client: DuunitoriClient,
    *, skip_gmap: bool = False, skip_firecrawl: bool = False,
) -> None:
    runner = DuunitoriPipelineRunner(
        config=config, client=client, skip_gmap=skip_gmap, skip_firecrawl=skip_firecrawl,
    )
    runner.run()
