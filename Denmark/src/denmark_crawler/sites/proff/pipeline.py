"""Proff 主流程。"""

from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

from oldiron_core.fc_email.client import FirecrawlError
from oldiron_core.fc_email.domain_cache import FirecrawlDomainCache
from oldiron_core.fc_email.email_service import FirecrawlEmailService
from oldiron_core.fc_email.email_service import FirecrawlEmailSettings
from oldiron_core.google_maps import GoogleMapsClient
from oldiron_core.google_maps import GoogleMapsConfig
from oldiron_core.google_maps import GoogleMapsPlaceResult
from denmark_crawler.sites.proff.backend_clients import GoFirecrawlService
from denmark_crawler.sites.proff.backend_clients import GoGMapClient
from denmark_crawler.sites.proff.client import ProffClient
from denmark_crawler.sites.proff.config import ProffDenmarkConfig
from denmark_crawler.sites.proff.store import FirecrawlTask
from denmark_crawler.sites.proff.store import GMapTask
from denmark_crawler.sites.proff.store import ProffStore

try:
    from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig
except ImportError:
    SiteCrawlClient = None  # type: ignore[assignment,misc]
    SiteCrawlConfig = None  # type: ignore[assignment,misc]


LOGGER = logging.getLogger(__name__)


def _retry_delay_seconds(retries: int, cap_seconds: float) -> float:
    return min(float(2 ** max(int(retries), 0)), float(cap_seconds))


def _build_gmap_queries(task: GMapTask) -> list[str]:
    queries: list[str] = []
    for value in (
        f"{task.company_name} {task.address} Denmark",
        f"{task.company_name} Denmark",
        task.company_name,
    ):
        text = re.sub(r"\s+", " ", str(value or "").strip())
        if text and text not in queries:
            queries.append(text)
    return queries


class ProffPipelineRunner:
    """协调 Proff -> GMap -> 官网爬虫(邮箱补充)。"""

    def __init__(
        self,
        *,
        config: ProffDenmarkConfig,
        client: ProffClient,
        skip_gmap: bool,
        skip_firecrawl: bool,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = skip_firecrawl
        self.store = ProffStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.search_done = threading.Event()
        self._search_workers_remaining = config.search_workers
        self._search_workers_lock = threading.Lock()
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_key_pool = None
        self._firecrawl_key_pool_lock = threading.Lock()
        self._firecrawl_domain_cache = FirecrawlDomainCache(
            self.config.project_root / "output" / "firecrawl_cache.db"
        )
        self._gmap_backend_checked = False
        self._firecrawl_backend_checked = False
        self._gmap_backend_enabled = False
        self._firecrawl_backend_enabled = False
        self._gmap_backend_lock = threading.Lock()
        self._firecrawl_backend_lock = threading.Lock()
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
            llm_api_style=self.config.llm_api_style,
            llm_timeout_seconds=self.config.llm_timeout_seconds,
            prefilter_limit=self.config.firecrawl_prefilter_limit,
            llm_pick_count=self.config.firecrawl_llm_pick_count,
            extract_max_urls=self.config.firecrawl_extract_max_urls,
            zero_retry_seconds=self.config.firecrawl_zero_retry_seconds,
            contact_form_retry_seconds=self.config.firecrawl_contact_form_retry_seconds,
            crawl_backend=self.config.crawl_backend,
        )

    def run(self) -> None:
        self.config.validate(skip_firecrawl=self.skip_firecrawl)
        self._prepare_search_tasks()
        recovered = self.store.requeue_stale_running_tasks(
            older_than_seconds=self.config.stale_running_requeue_seconds
        )
        if recovered:
            LOGGER.info("Proff 已回收陈旧运行中任务：%s", recovered)
        # 启动前批量处理：域名缓存已有结果的 pending 任务直接标 done
        cached = self._firecrawl_domain_cache.get_all_done_domains()
        batch_resolved = self.store.batch_resolve_cached_firecrawl(cached)
        if batch_resolved:
            LOGGER.info("Proff 启动预处理：批量跳过 %d 个已缓存域名的任务", batch_resolved)
        workers = self._build_workers()
        for worker in workers:
            worker.start()
            # email worker 错峰启动，避免同时请求 LLM API
            if worker.name.startswith("proff-email-"):
                time.sleep(0.1)
        try:
            self._monitor_until_done()
        finally:
            self.stop_event.set()
            for worker in workers:
                worker.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self._firecrawl_domain_cache.close()
            self.store.close()

    def _prepare_search_tasks(self) -> None:
        existing_count = self.store.search_task_count()
        use_reseed = existing_count > 0 and not self.store.has_segmented_search_tasks()
        if existing_count > 0 and not use_reseed:
            # 已有分段任务，检查规划是否跑完
            planned = self.store.get_planned_industries()
            if planned:
                LOGGER.info("Proff 已有分段任务=%d，已规划行业=%d，检查是否还有未规划行业", existing_count, len(planned))
                # 继续规划剩余行业（断点续跑）
                self.client.discover_max_coverage_task_keys(
                    max_results_per_segment=self.config.max_results_per_segment,
                    skip_industries=planned,
                    on_industry_done=self.store.mark_industry_planned,
                    planning_workers=16,
                )
                final_count = self.store.search_task_count()
                LOGGER.info("Proff 规划续跑完成：总搜索任务=%d", final_count)
            return
        if self.config.queries:
            LOGGER.info("Proff 搜索规划模式：定向 query 分段")
            task_keys = self.client.discover_search_task_keys(
                self.config.queries,
                max_results_per_segment=self.config.max_results_per_segment,
            )
            if not task_keys:
                task_keys = list(self.config.queries)
            if use_reseed:
                replaced = self.store.reseed_search_tasks(task_keys)
                LOGGER.info("Proff 已切换到 API 深分段搜索任务：segments=%s", replaced)
                return
            self.store.ensure_search_seed(task_keys)
            LOGGER.info("Proff API 深分段任务已装载：segments=%s", len(task_keys))
        else:
            LOGGER.info("Proff 搜索规划模式：极限覆盖 industry+geo 分段（增量断点）")
            planned = self.store.get_planned_industries()
            if planned:
                LOGGER.info("Proff 断点续跑：已规划行业=%d，继续剩余行业", len(planned))
            self.client.discover_max_coverage_task_keys(
                max_results_per_segment=self.config.max_results_per_segment,
                skip_industries=planned,
                on_industry_done=self.store.mark_industry_planned,
                planning_workers=16,
            )
            final_count = self.store.search_task_count()
            LOGGER.info("Proff 极限覆盖规划完成：总搜索任务=%d", final_count)

    def _build_workers(self) -> list[threading.Thread]:
        workers = [
            threading.Thread(target=self._search_worker, name=f"proff-search-{index+1}", daemon=True)
            for index in range(self.config.search_workers)
        ]
        if not self.skip_gmap:
            workers.extend(
                threading.Thread(target=self._gmap_worker, name=f"proff-gmap-{index+1}", daemon=True)
                for index in range(self.config.gmap_workers)
            )
        if not self.skip_firecrawl:
            workers.extend(
                threading.Thread(target=self._email_worker, name=f"proff-email-{index+1}", daemon=True)
                for index in range(self.config.firecrawl_workers)
            )
        return workers

    def _monitor_until_done(self) -> None:
        last_log = 0.0
        last_snapshot = 0.0
        last_retry = 0.0
        while not self.stop_event.is_set():
            now = time.monotonic()
            self.store.requeue_stale_running_tasks(
                older_than_seconds=self.config.stale_running_requeue_seconds
            )
            if now - last_retry >= 60.0:
                revived = self.store.requeue_expired_firecrawl_tasks()
                if revived:
                    LOGGER.info("Proff 邮箱补充 0结果到期，已回队列：%s", revived)
                last_retry = now
            if now - last_snapshot >= 30.0:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snapshot = now
            if now - last_log >= 10.0:
                progress = self.store.get_progress()
                LOGGER.info(
                    "进度：search=%s/%s gmap=%s/%s email=%s/%s companies=%s final=%s",
                    progress.search_done,
                    progress.search_total,
                    progress.gmap_running,
                    progress.gmap_pending,
                    progress.firecrawl_running,
                    progress.firecrawl_pending,
                    progress.companies_total,
                    progress.final_total,
                )
                last_log = now
            if self.search_done.is_set() and self._queues_idle():
                return
            time.sleep(self.config.queue_poll_interval)

    def _queues_idle(self) -> bool:
        progress = self.store.get_progress()
        gmap_busy = 0 if self.skip_gmap else (progress.gmap_pending + progress.gmap_running)
        firecrawl_busy = 0 if self.skip_firecrawl else (progress.firecrawl_pending + progress.firecrawl_running)
        return (gmap_busy + firecrawl_busy) == 0

    def _search_worker(self) -> None:
        try:
            while not self.stop_event.is_set():
                if self.config.max_companies > 0 and self.store.company_count() >= self.config.max_companies:
                    return
                task = self.store.claim_search_task()
                if task is None:
                    if self.store.has_search_work():
                        time.sleep(self.config.queue_poll_interval)
                        continue
                    return
                try:
                    rows, hits, pages = self.client.fetch_search_page(query=task.query, page=task.page)
                    direct_email_count = sum(1 for company in rows if company.email)
                    direct_deliverable_count = sum(
                        1 for company in rows if company.email and company.representative and company.company_name
                    )
                    LOGGER.info(
                        "Proff 搜索页完成：query=%s page=%s rows=%s hits=%s pages=%s 直出邮箱=%s 直出可交付=%s",
                        task.query,
                        task.page,
                        len(rows),
                        hits,
                        pages,
                        direct_email_count,
                        direct_deliverable_count,
                    )
                    for company in rows:
                        self.store.upsert_company(company)
                    self.store.mark_search_done(
                        query=task.query,
                        page=task.page,
                        total_pages=pages,
                        max_pages_per_query=self.config.max_pages_per_query,
                    )
                except Exception as exc:
                    self._handle_search_failure(task.query, task.page, task.retries + 1, exc)
        finally:
            with self._search_workers_lock:
                self._search_workers_remaining -= 1
                if self._search_workers_remaining <= 0:
                    LOGGER.info("Proff 搜索主流程完成。")
                    self.search_done.set()

    def _handle_search_failure(self, query: str, page: int, retries: int, exc: Exception) -> None:
        error_text = str(exc)
        if retries >= self.config.max_task_retries:
            LOGGER.warning("Proff 搜索页失败并放弃：query=%s page=%s error=%s", query, page, error_text)
            self.store.mark_search_failed(query=query, page=page, error_text=error_text)
            return
        delay_seconds = _retry_delay_seconds(retries, self.config.retry_backoff_cap_seconds)
        LOGGER.warning(
            "Proff 搜索页重试：query=%s page=%s retries=%s wait=%.1fs error=%s",
            query,
            page,
            retries,
            delay_seconds,
            error_text,
        )
        self.store.defer_search_task(
            query=query,
            page=page,
            retries=retries,
            delay_seconds=delay_seconds,
            error_text=error_text,
        )

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_gmap_task(task)

    def _process_gmap_task(self, task: GMapTask) -> None:
        LOGGER.info("Proff GMap 开始：%s | %s", task.orgnr, task.company_name)
        try:
            result = GoogleMapsPlaceResult()
            for query in _build_gmap_queries(task):
                LOGGER.info("Proff GMap 查询：%s | %s", task.orgnr, query)
                candidate = self._search_company_profile(query, task.company_name)
                if candidate.score >= result.score:
                    result = candidate
                if result.website:
                    break
        except Exception as exc:
            attempt = task.retries + 1
            if attempt >= self.config.gmap_max_retries:
                self.store.mark_gmap_failed(orgnr=task.orgnr, error_text=str(exc))
                return
            self.store.defer_gmap_task(
                orgnr=task.orgnr,
                retries=attempt,
                delay_seconds=_retry_delay_seconds(attempt, self.config.retry_backoff_cap_seconds),
                error_text=str(exc),
            )
            return
        self.store.mark_gmap_done(
            orgnr=task.orgnr,
            website=result.website,
            source="gmap" if result.website else "",
            phone=result.phone,
            company_name=result.company_name,
        )
        LOGGER.info("Proff GMap 完成：%s | 官网=%s", task.orgnr, result.website or "-")

    def _email_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: FirecrawlTask) -> None:
        if not task.website:
            self.store.mark_firecrawl_failed(orgnr=task.orgnr, error_text="缺少官网")
            return
        decision = self._firecrawl_domain_cache.prepare_lookup(task.domain)
        if decision.status == "done":
            if decision.emails:
                self.store.mark_firecrawl_done(
                    orgnr=task.orgnr,
                    emails=decision.emails,
                    retry_after_seconds=0.0,
                )
                LOGGER.info("Proff 邮箱补充 命中缓存：%s | 域名=%s | 邮箱=%d", task.orgnr, task.domain, len(decision.emails))
            else:
                self.store.mark_firecrawl_done(
                    orgnr=task.orgnr,
                    emails=[],
                    retry_after_seconds=0.0,
                )
                LOGGER.info("Proff 邮箱补充 命中缓存(无邮箱)：%s | 域名=%s", task.orgnr, task.domain)
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                orgnr=task.orgnr,
                retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.config.queue_poll_interval),
                error_text="等待同域名查询完成",
            )
            LOGGER.info("Proff 邮箱补充 等待同域名：%s | 域名=%s", task.orgnr, task.domain)
            return
        LOGGER.info("Proff 邮箱补充 开始：%s | 域名=%s", task.orgnr, task.domain)
        try:
            result = self._discover_emails(
                company_name=task.company_name,
                homepage=task.website,
                domain=task.domain,
                existing_representative=task.representative,
            )
        except FirecrawlError as exc:
            self._handle_firecrawl_failure(task, exc)
            return
        except Exception as exc:
            self._handle_firecrawl_failure(task, exc)
            return
        self._firecrawl_domain_cache.mark_done(
            task.domain,
            result.emails,
            retry_after_seconds=result.retry_after_seconds,
        )
        self.store.mark_firecrawl_done(
            orgnr=task.orgnr,
            emails=result.emails,
            representative=result.representative,
            company_name=result.company_name,
            evidence_url=result.evidence_url,
            retry_after_seconds=result.retry_after_seconds,
        )
        LOGGER.info("Proff 邮箱补充 完成：%s | 域名=%s | 邮箱=%d", task.orgnr, task.domain, len(result.emails))

    def _handle_firecrawl_failure(self, task: FirecrawlTask, exc: Exception) -> None:
        attempt = task.retries + 1
        delay = self._firecrawl_delay(attempt, exc)
        # 认证/配额错误——最终失败，释放域名锁
        if isinstance(exc, FirecrawlError) and exc.code in {"firecrawl_401", "firecrawl_402"}:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(orgnr=task.orgnr, error_text=str(exc))
            return
        # 达到最大重试次数——最终失败，释放域名锁
        if attempt >= self.config.firecrawl_task_max_retries:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(orgnr=task.orgnr, error_text=str(exc))
            return
        # 还有重试机会——defer 域名
        self._firecrawl_domain_cache.defer(task.domain, delay_seconds=delay, error_text=str(exc))
        self.store.defer_firecrawl_task(
            orgnr=task.orgnr,
            retries=attempt,
            delay_seconds=delay,
            error_text=str(exc),
        )
        LOGGER.warning(
            "Proff 邮箱补充 重试：%s | 域名=%s | 第%d次 | 等待=%.1fs | 原因=%s",
            task.orgnr,
            task.domain,
            attempt,
            delay,
            exc,
        )

    def _firecrawl_delay(self, attempt: int, exc: Exception) -> float:
        if isinstance(exc, FirecrawlError):
            if exc.code == "firecrawl_5xx":
                return 0.0
            if exc.code == "firecrawl_429" and exc.retry_after:
                return max(float(exc.retry_after), 5.0)
        return _retry_delay_seconds(attempt, self.config.retry_backoff_cap_seconds)

    def _get_gmap_client(self) -> GoogleMapsClient:
        if not hasattr(self._gmap_local, "client"):
            if self._use_go_gmap_backend():
                self._gmap_local.client = GoGMapClient(
                    self.config.gmap_service_url,
                    timeout_seconds=self.config.timeout_seconds,
                )
            else:
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
            firecrawl_client = None
            # 协议爬虫后端
            if self.config.crawl_backend == "protocol" and SiteCrawlClient is not None:
                LOGGER.info("使用协议爬虫后端 (CRAWL_BACKEND=protocol)")
                firecrawl_client = SiteCrawlClient(SiteCrawlConfig(
                    timeout_seconds=self.config.firecrawl_timeout_seconds,
                    max_retries=self.config.firecrawl_max_retries,
                    proxy_url=self.config.proxy_url,
                ))
            elif self._use_go_firecrawl_backend():
                firecrawl_client = GoFirecrawlService(
                    self.config.firecrawl_service_url,
                    timeout_seconds=self.config.llm_timeout_seconds,
                )
            key_pool = None if (self.config.crawl_backend == "protocol") else self._get_firecrawl_key_pool()
            self._firecrawl_local.service = FirecrawlEmailService(
                self._firecrawl_settings,
                key_pool=key_pool,
                firecrawl_client=firecrawl_client,
            )
        return self._firecrawl_local.service

    def _use_go_gmap_backend(self) -> bool:
        if not self.config.prefer_go_gmap_backend:
            return False
        with self._gmap_backend_lock:
            if self._gmap_backend_checked:
                return self._gmap_backend_enabled
            self._gmap_backend_checked = True
            try:
                client = GoGMapClient(self.config.gmap_service_url, timeout_seconds=self.config.timeout_seconds)
                health = client.health()
                self._gmap_backend_enabled = bool(health.ok)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Proff GMap Go 后端不可用，回退 Python：%s", exc)
                self._gmap_backend_enabled = False
        return self._gmap_backend_enabled

    def _use_go_firecrawl_backend(self) -> bool:
        if not self.config.prefer_go_firecrawl_backend:
            return False
        with self._firecrawl_backend_lock:
            if self._firecrawl_backend_checked:
                return self._firecrawl_backend_enabled
            self._firecrawl_backend_checked = True
            try:
                client = GoFirecrawlService(
                    self.config.firecrawl_service_url,
                    timeout_seconds=self.config.llm_timeout_seconds,
                )
                health = client.health()
                self._firecrawl_backend_enabled = bool(health.ok)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Proff 邮箱补充 Go 后端不可用，回退 Python：%s", exc)
                self._firecrawl_backend_enabled = False
            return self._firecrawl_backend_enabled

    def _search_company_profile(self, query: str, company_name: str) -> GoogleMapsPlaceResult:
        client = self._get_gmap_client()
        try:
            return client.search_company_profile(query, company_name=company_name)
        except Exception as exc:  # noqa: BLE001
            if isinstance(client, GoGMapClient):
                LOGGER.warning("Proff GMap Go 调用失败，禁用 Go 后端：%s", exc)
                self._gmap_backend_enabled = False
                # 不立刻重试，让任务回队列用 Python 客户端慢速重试
                raise
            raise

    def _discover_emails(self, *, company_name: str, homepage: str, domain: str, existing_representative: str):
        service = self._get_firecrawl_service()
        return service.discover_emails(
            company_name=company_name,
            homepage=homepage,
            domain=domain,
            existing_representative=existing_representative,
        )


def run_proff_pipeline(
    config: ProffDenmarkConfig,
    client: ProffClient,
    *,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
) -> None:
    runner = ProffPipelineRunner(
        config=config,
        client=client,
        skip_gmap=skip_gmap,
        skip_firecrawl=skip_firecrawl,
    )
    runner.run()
