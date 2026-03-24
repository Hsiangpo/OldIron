"""Työmarkkinatori 主流程 — 搜索 → 详情 → GMap → Protocol+LLM 邮箱发现。"""

from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

from finland_crawler.fc_email.domain_cache import FirecrawlDomainCache
from finland_crawler.fc_email.email_service import (
    FirecrawlEmailService, FirecrawlEmailSettings, EmailDiscoveryResult,
)
from finland_crawler.fc_email.client import FirecrawlError
from finland_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig, GoogleMapsPlaceResult
from finland_crawler.sites.tyomarkkinatori.client import TmtClient
from finland_crawler.sites.tyomarkkinatori.config import TmtConfig
from finland_crawler.sites.tyomarkkinatori.store import (
    TmtDetailTask, TmtGMapTask, TmtFirecrawlTask, TmtStore,
)

# 协议爬虫（protocol_crawler）——可选依赖
try:
    from shared.oldiron_core.protocol_crawler.client import SiteCrawlClient, SiteCrawlConfig
except ImportError:
    SiteCrawlClient = None  # type: ignore[assignment,misc]
    SiteCrawlConfig = None  # type: ignore[assignment,misc]

LOGGER = logging.getLogger(__name__)


def _retry_delay(retries: int, cap: float) -> float:
    return min(float(2 ** max(int(retries), 0)), float(cap))


def _gmap_queries(task: TmtGMapTask) -> list[str]:
    """生成 GMap 搜索关键词。"""
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


class TmtPipelineRunner:
    """协调 TMT 搜索 → 详情 → GMap → Protocol+LLM 四阶段流水线。"""

    def __init__(
        self,
        *,
        config: TmtConfig,
        client: TmtClient,
        skip_gmap: bool = False,
        skip_firecrawl: bool = False,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = skip_firecrawl
        self.store = TmtStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.search_done = threading.Event()
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_domain_cache = FirecrawlDomainCache(
            config.project_root / "output" / "tmt_firecrawl_cache.db"
        )
        self._firecrawl_settings = FirecrawlEmailSettings(
            keys_inline=[],
            keys_file=Path(config.firecrawl_keys_file or (config.project_root / "output" / "firecrawl_keys.txt")),
            pool_db=Path(config.firecrawl_pool_db or (config.project_root / "output" / "cache" / "firecrawl_keys.db")),
            base_url=config.firecrawl_base_url,
            timeout_seconds=config.firecrawl_timeout_seconds,
            max_retries=config.firecrawl_max_retries,
            key_per_limit=config.firecrawl_key_per_limit,
            key_wait_seconds=int(config.firecrawl_key_wait_seconds),
            key_cooldown_seconds=int(config.firecrawl_key_cooldown_seconds),
            key_failure_threshold=config.firecrawl_key_failure_threshold,
            llm_api_key=config.llm_api_key,
            llm_base_url=config.llm_base_url,
            llm_model=config.llm_model,
            llm_reasoning_effort=config.llm_reasoning_effort,
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
        recovered = self.store.requeue_stale_running_tasks(
            older_than_seconds=self.config.stale_running_requeue_seconds
        )
        if recovered:
            LOGGER.info("TMT 已回收陈旧运行中任务：%s", recovered)

        # 启动 workers
        workers = self._build_workers()
        for w in workers:
            w.start()
            if w.name.startswith("tmt-email-"):
                time.sleep(0.3)

        # 后台搜索
        def _bg_search() -> None:
            try:
                self._run_search()
            except Exception as exc:
                LOGGER.error("TMT 搜索异常：%s", exc)
            finally:
                self.search_done.set()

        search_thread = threading.Thread(target=_bg_search, name="tmt-search", daemon=True)
        search_thread.start()

        try:
            self._monitor()
        finally:
            self.stop_event.set()
            for w in workers:
                w.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self.store.close()

    # ---- 搜索 ----

    def _run_search(self) -> None:
        """翻页搜索所有职位。TMT API 简单翻页即可。"""
        pages_done, total_known = self.store.get_search_progress()
        page = pages_done
        LOGGER.info("TMT 搜索开始：从第 %d 页续跑", page)

        while not self.stop_event.is_set():
            try:
                jobs, total = self.client.search_jobs(
                    page_number=page,
                    page_size=self.config.search_page_size,
                )
            except Exception as exc:
                LOGGER.warning("TMT 搜索第 %d 页失败：%s，等待 5s 重试", page, exc)
                time.sleep(5)
                continue

            if not jobs:
                LOGGER.info("TMT 搜索完成：第 %d 页无数据，总计 %d 职位", page, total)
                self.store.update_search_progress(total, page)
                break

            for raw in jobs:
                posting = TmtClient.parse_search_job(raw, page)
                if posting.job_id:
                    self.store.upsert_job(posting)

            page += 1
            self.store.update_search_progress(total, page)
            LOGGER.info("TMT 搜索：page=%d rows=%d total=%d", page - 1, len(jobs), total)

            if page >= self.config.search_max_pages:
                LOGGER.info("TMT 搜索达到最大页数 %d", self.config.search_max_pages)
                break

    # ---- Workers ----

    def _build_workers(self) -> list[threading.Thread]:
        workers = [
            threading.Thread(target=self._detail_worker, name=f"tmt-detail-{i+1}", daemon=True)
            for i in range(self.config.detail_workers)
        ]
        if not self.skip_gmap:
            workers.extend(
                threading.Thread(target=self._gmap_worker, name=f"tmt-gmap-{i+1}", daemon=True)
                for i in range(self.config.gmap_workers)
            )
        if not self.skip_firecrawl:
            workers.extend(
                threading.Thread(target=self._email_worker, name=f"tmt-email-{i+1}", daemon=True)
                for i in range(self.config.firecrawl_workers)
            )
        return workers

    # ---- 监控 ----

    def _monitor(self) -> None:
        last_log = 0.0
        last_snap = 0.0
        while not self.stop_event.is_set():
            now = time.monotonic()
            self.store.requeue_stale_running_tasks(
                older_than_seconds=self.config.stale_running_requeue_seconds
            )
            if now - last_snap >= 30.0:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snap = now
            if now - last_log >= 10.0:
                p = self.store.get_progress()
                LOGGER.info(
                    "进度：search=%d/%d detail=%d/%d(%d+%d) gmap=%d+%d email=%d+%d jobs=%d final=%d",
                    p.search_done_pages, p.search_total_jobs,
                    p.detail_done, p.detail_total, p.detail_running, p.detail_pending,
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
        detail_busy = p.detail_pending + p.detail_running
        gmap_busy = 0 if self.skip_gmap else (p.gmap_pending + p.gmap_running)
        fc_busy = 0 if self.skip_firecrawl else (p.firecrawl_pending + p.firecrawl_running)
        return (detail_busy + gmap_busy + fc_busy) == 0

    # ---- 详情 Worker ----

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_detail_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            try:
                detail = self.client.fetch_job_detail(task.job_id)
                from finland_crawler.sites.tyomarkkinatori.models import TmtJobPosting
                posting = TmtJobPosting(job_id=task.job_id)
                TmtClient.enrich_with_detail(posting, detail)
                self.store.upsert_job_detail(posting)
                self.store.mark_detail_done(task.job_id)
                LOGGER.debug("TMT 详情完成：%s rep=%s email=%s",
                             task.job_id, posting.representative or "-", posting.email or "-")
            except Exception as exc:
                retries = task.retries + 1
                if retries >= self.config.max_task_retries:
                    self.store.mark_detail_failed(task.job_id, str(exc))
                    LOGGER.warning("TMT 详情放弃：%s error=%s", task.job_id, exc)
                else:
                    delay = _retry_delay(retries, self.config.retry_backoff_cap_seconds)
                    self.store.defer_detail_task(task.job_id, retries, delay, str(exc))

    # ---- GMap Worker ----

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._do_gmap(task)

    def _do_gmap(self, task: TmtGMapTask) -> None:
        LOGGER.info("TMT GMap 开始：%s | %s", task.job_id, task.company_name)
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
            LOGGER.warning("TMT GMap 异常：%s | %s | 第%d次 | %s",
                           task.job_id, task.company_name, retries, exc)
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
            source="gmap" if result.website else "",
            phone=result.phone, company_name=result.company_name,
        )
        LOGGER.info("TMT GMap 完成：%s | 官网=%s", task.job_id, result.website or "-")

    def _get_gmap_client(self) -> GoogleMapsClient:
        client = getattr(self._gmap_local, "client", None)
        if client is None:
            client = GoogleMapsClient(GoogleMapsConfig(proxy_url=self.config.proxy_url))
            self._gmap_local.client = client
        return client

    # ---- Protocol+LLM 邮箱 Worker ----

    def _email_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: TmtFirecrawlTask) -> None:
        if not task.website:
            self.store.mark_firecrawl_failed(job_id=task.job_id, error_text="缺少官网")
            return
        decision = self._firecrawl_domain_cache.prepare_lookup(task.domain)
        if decision.status == "done":
            self.store.mark_firecrawl_done(
                job_id=task.job_id, emails=decision.emails or [],
                retry_after_seconds=0.0,
            )
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                job_id=task.job_id, retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.config.queue_poll_interval),
                error_text="等待同域名查询完成",
            )
            return
        LOGGER.info("TMT 邮箱 开始：%s | 域名=%s", task.job_id, task.domain)
        try:
            result = self._discover_emails(
                company_name=task.company_name,
                homepage=task.website,
                domain=task.domain,
            )
        except (FirecrawlError, Exception) as exc:
            self._handle_firecrawl_failure(task, exc)
            return
        self._firecrawl_domain_cache.mark_done(
            task.domain, result.emails, retry_after_seconds=result.retry_after_seconds,
        )
        self.store.mark_firecrawl_done(
            job_id=task.job_id, emails=result.emails,
            representative=result.representative,
            company_name=result.company_name,
            evidence_url=result.evidence_url,
            retry_after_seconds=result.retry_after_seconds,
        )
        LOGGER.info("TMT 邮箱 完成：%s | 邮箱=%d | 代表人=%s",
                     task.job_id, len(result.emails), result.representative or "-")

    def _handle_firecrawl_failure(self, task: TmtFirecrawlTask, exc: Exception) -> None:
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
        if isinstance(exc, FirecrawlError) and getattr(exc, 'code', '') == "firecrawl_429":
            delay = max(float(getattr(exc, 'retry_after', 0) or 5.0), 5.0)
        self._firecrawl_domain_cache.defer(task.domain, delay_seconds=delay, error_text=str(exc))
        self.store.defer_firecrawl_task(
            job_id=task.job_id, retries=attempt, delay_seconds=delay, error_text=str(exc),
        )
        LOGGER.warning("TMT 邮箱 重试：%s | 第%d次 | 等待=%.1fs | 原因=%s",
                        task.job_id, attempt, delay, exc)

    def _get_firecrawl_service(self) -> FirecrawlEmailService:
        service = getattr(self._firecrawl_local, "service", None)
        if service is not None:
            return service
        firecrawl_client = None
        if self.config.crawl_backend == "protocol" and SiteCrawlClient is not None:
            LOGGER.info("TMT 邮箱 使用协议爬虫后端 (CRAWL_BACKEND=protocol)")
            firecrawl_client = SiteCrawlClient(SiteCrawlConfig(
                timeout_seconds=self.config.firecrawl_timeout_seconds,
                max_retries=self.config.firecrawl_max_retries,
                proxy_url=self.config.proxy_url,
            ))
        self._firecrawl_local.service = FirecrawlEmailService(
            self._firecrawl_settings,
            key_pool=None if self.config.crawl_backend == "protocol" else None,
            firecrawl_client=firecrawl_client,
        )
        return self._firecrawl_local.service

    def _discover_emails(self, *, company_name: str, homepage: str, domain: str) -> EmailDiscoveryResult:
        service = self._get_firecrawl_service()
        return service.discover_emails(
            company_name=company_name, homepage=homepage, domain=domain,
        )


def run_tmt_pipeline(
    config: TmtConfig,
    client: TmtClient,
    *,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
) -> None:
    """启动 TMT 完整 pipeline。"""
    runner = TmtPipelineRunner(
        config=config, client=client,
        skip_gmap=skip_gmap, skip_firecrawl=skip_firecrawl,
    )
    runner.run()
