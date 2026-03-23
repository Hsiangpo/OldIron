"""Virk 主流程 — 搜索 → 详情 → GMap → Protocol+LLM 邮箱发现。"""

from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

from denmark_crawler.fc_email.domain_cache import FirecrawlDomainCache
from denmark_crawler.fc_email.email_service import (
    FirecrawlEmailService, FirecrawlEmailSettings, EmailDiscoveryResult,
)
from denmark_crawler.fc_email.client import FirecrawlError
from denmark_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig, GoogleMapsPlaceResult
from denmark_crawler.sites.virk.client import VirkClient
from denmark_crawler.sites.virk.config import VirkConfig
from denmark_crawler.sites.virk.store import (
    VirkDetailTask, VirkGMapTask, VirkFirecrawlTask, VirkStore,
)

# 协议爬虫（protocol_crawler）——可选依赖
try:
    from shared.oldiron_core.protocol_crawler.client import SiteCrawlClient, SiteCrawlConfig
except ImportError:
    SiteCrawlClient = None  # type: ignore[assignment,misc]
    SiteCrawlConfig = None  # type: ignore[assignment,misc]

LOGGER = logging.getLogger(__name__)

# 搜索 API 每个分段最多返回 3000 条（3 页 × 1000）
_SEGMENT_LIMIT = 3000


def _retry_delay(retries: int, cap: float) -> float:
    return min(float(2 ** max(int(retries), 0)), float(cap))


def _gmap_queries(task: VirkGMapTask) -> list[str]:
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


class VirkPipelineRunner:
    """协调 Virk 搜索 → 详情 → GMap → Firecrawl 四阶段流水线。"""

    def __init__(
        self,
        *,
        config: VirkConfig,
        client: VirkClient,
        skip_gmap: bool = False,
        skip_firecrawl: bool = False,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = skip_firecrawl
        self.store = VirkStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.search_done = threading.Event()
        self._search_remaining = config.search_workers
        self._search_lock = threading.Lock()
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_domain_cache = FirecrawlDomainCache(
            config.project_root / "output" / "virk_firecrawl_cache.db"
        )
        self._firecrawl_settings = FirecrawlEmailSettings(
            keys_inline=list(config.firecrawl_keys_inline or []),
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
        # 先回收陈旧任务，让已有队列立刻可用
        recovered = self.store.requeue_stale_running_tasks(
            older_than_seconds=self.config.stale_running_requeue_seconds
        )
        if recovered:
            LOGGER.info("Virk 已回收陈旧运行中任务：%s", recovered)

        # 先启动所有 worker（处理已有的 detail/gmap/firecrawl 队列）
        workers = self._build_workers()
        for w in workers:
            w.start()
            # email worker 错峰启动，避免同时请求 LLM API
            if w.name.startswith("virk-email-"):
                time.sleep(0.3)

        # 在后台线程做搜索规划（可能被 429 卡很久，不阻塞已有任务）
        def _bg_plan() -> None:
            try:
                self._prepare_search_segments()
            except Exception as exc:
                LOGGER.error("Virk 搜索规划异常：%s", exc)

        plan_thread = threading.Thread(target=_bg_plan, name="virk-planner", daemon=True)
        plan_thread.start()

        try:
            self._monitor()
        finally:
            self.stop_event.set()
            for w in workers:
                w.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self.store.close()

    # ---- 搜索分段规划 ----

    def _prepare_search_segments(self) -> None:
        """按 Kommune × VirksomhedsForm 生成搜索分段（并发探测）。

        原则：每个分段总量 ≤ 3000（搜索 API 的翻页上限）。
        - 先按 Kommune 分：105 个市
        - 如果某个 Kommune 超过 3000 → 再按 Virksomhedsform 拆
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        done_segments = self.store.get_planned_segments()
        if done_segments:
            LOGGER.info("Virk 规划：已有 %d 个分段，断点续跑", len(done_segments))

        constants = self.client.fetch_constants()
        kommuner = constants.get("kommuner", [])
        virksomhedsformer = constants.get("virksomhedsformer", [])

        LOGGER.info("Virk 规划：%d 个 Kommune，%d 种公司类型，4 并发探测",
                     len(kommuner), len(virksomhedsformer))

        # 统计计数器（线程安全）
        counter_lock = threading.Lock()
        counters = {"segments": 0, "estimated": 0}

        def _plan_kommune(k: dict) -> None:
            """规划单个 Kommune 的搜索分段（含 429 重试）。"""
            kode = str(k.get("kommunekode", ""))
            navn = str(k.get("navn", ""))
            base_key = f"k{kode}"
            if base_key in done_segments:
                with counter_lock:
                    counters["segments"] += 1
                return

            def _search_with_retry(**kwargs: object) -> tuple:
                """带 429 退避重试的搜索请求。"""
                for attempt in range(6):
                    try:
                        return self.client.search_companies(**kwargs)
                    except Exception as exc:
                        if "429" in str(exc) and attempt < 5:
                            wait = 2 ** (attempt + 1)  # 2, 4, 8, 16, 32 秒
                            LOGGER.warning("Virk 规划 429 重试 %d/5：%s，等待 %ds", attempt + 1, navn, wait)
                            time.sleep(wait)
                            continue
                        raise
                return ([], 0)

            # 探测该 Kommune 总量
            _, total = _search_with_retry(kommune=[kode], page_size=10, page_index=0)

            if total == 0:
                self.store.save_segment(base_key, kode, navn, "", "", 0, 0)
                with counter_lock:
                    counters["segments"] += 1
                return

            if total <= _SEGMENT_LIMIT:
                pages = min((total + self.config.search_page_size - 1) // self.config.search_page_size,
                            self.config.search_max_pages)
                self.store.save_segment(base_key, kode, navn, "", "", total, pages)
                with counter_lock:
                    counters["segments"] += 1
                    counters["estimated"] += total
                LOGGER.info("Virk 分段：%s (%s) total=%d pages=%d", navn, kode, total, pages)
            else:
                # 需要按 Virksomhedsform 拆分
                LOGGER.info("Virk 分段拆分：%s (%s) total=%d > %d，按公司类型拆分",
                            navn, kode, total, _SEGMENT_LIMIT)
                covered = 0
                for vf in virksomhedsformer:
                    vf_kode = str(vf.get("kode", ""))
                    vf_navn = str(vf.get("vaerdi", ""))
                    sub_key = f"k{kode}_vf{vf_kode}"
                    if sub_key in done_segments:
                        with counter_lock:
                            counters["segments"] += 1
                        continue

                    _, sub_total = _search_with_retry(
                        kommune=[kode], virksomhedsform=[vf_kode],
                        page_size=10, page_index=0,
                    )
                    if sub_total == 0:
                        self.store.save_segment(sub_key, kode, navn, vf_kode, vf_navn, 0, 0)
                        with counter_lock:
                            counters["segments"] += 1
                        continue

                    pages = min(
                        (sub_total + self.config.search_page_size - 1) // self.config.search_page_size,
                        self.config.search_max_pages,
                    )
                    self.store.save_segment(sub_key, kode, navn, vf_kode, vf_navn, sub_total, pages)
                    with counter_lock:
                        counters["segments"] += 1
                        counters["estimated"] += sub_total
                    covered += sub_total
                    LOGGER.info("  %s %s (%s): total=%d pages=%d",
                                navn, vf_navn, vf_kode, sub_total, pages)

                if covered < total:
                    LOGGER.warning("Virk %s 按公司类型覆盖 %d/%d，部分公司可能遗漏",
                                   navn, covered, total)

        # 2 线程并发规划（降低并发避免 429）
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="virk-plan") as pool:
            futures = [pool.submit(_plan_kommune, k) for k in kommuner]
            for future in as_completed(futures):
                exc = future.exception()
                if exc:
                    LOGGER.error("Virk 规划某 Kommune 出错: %s", exc)

        LOGGER.info("Virk 规划完成：%d 个分段，预估 %d 家公司",
                     counters["segments"], counters["estimated"])

    # ---- Worker 构建 ----

    def _build_workers(self) -> list[threading.Thread]:
        workers = [
            threading.Thread(target=self._search_worker, name=f"virk-search-{i+1}", daemon=True)
            for i in range(self.config.search_workers)
        ]
        workers.extend(
            threading.Thread(target=self._detail_worker, name=f"virk-detail-{i+1}", daemon=True)
            for i in range(self.config.detail_workers)
        )
        if not self.skip_gmap:
            workers.extend(
                threading.Thread(target=self._gmap_worker, name=f"virk-gmap-{i+1}", daemon=True)
                for i in range(self.config.gmap_workers)
            )
        if not self.skip_firecrawl:
            workers.extend(
                threading.Thread(target=self._email_worker, name=f"virk-email-{i+1}", daemon=True)
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
                    "进度：search=%d/%d detail=%d/%d(%d+%d) gmap=%d+%d email=%d+%d companies=%d final=%d",
                    p.search_done, p.search_total,
                    p.detail_done, p.detail_total, p.detail_running, p.detail_pending,
                    p.gmap_running, p.gmap_pending,
                    p.firecrawl_running, p.firecrawl_pending,
                    p.companies_total, p.final_total,
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

    # ---- 搜索 Worker ----

    def _search_worker(self) -> None:
        try:
            while not self.stop_event.is_set():
                task = self.store.claim_search_task()
                if task is None:
                    if self.store.has_search_work():
                        time.sleep(self.config.queue_poll_interval)
                        continue
                    return
                try:
                    companies, total = self.client.search_companies(
                        kommune=[task.kommune_kode] if task.kommune_kode else None,
                        virksomhedsform=[task.virksomhedsform_kode] if task.virksomhedsform_kode else None,
                        page_index=task.page_index,
                        page_size=self.config.search_page_size,
                    )
                    for raw in companies:
                        company = VirkClient.parse_search_company(raw, task.segment_key, task.page_index)
                        self.store.upsert_company(company)
                    self.store.mark_search_done(task.segment_key, task.page_index)
                    LOGGER.info("Virk 搜索完成：%s page=%d rows=%d",
                                task.segment_key, task.page_index, len(companies))
                except Exception as exc:
                    retries = task.retries + 1
                    if retries >= self.config.max_task_retries:
                        self.store.mark_search_failed(task.segment_key, task.page_index, str(exc))
                        LOGGER.warning("Virk 搜索放弃：%s page=%d error=%s",
                                       task.segment_key, task.page_index, exc)
                    else:
                        delay = _retry_delay(retries, self.config.retry_backoff_cap_seconds)
                        self.store.defer_search_task(task.segment_key, task.page_index,
                                                    retries, delay, str(exc))
        finally:
            with self._search_lock:
                self._search_remaining -= 1
                if self._search_remaining <= 0:
                    LOGGER.info("Virk 搜索主流程完成。")
                    self.search_done.set()

    # ---- 详情 Worker ----

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_detail_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            try:
                detail = self.client.fetch_company_detail(task.cvr)
                company = self.store.get_company_for_detail(task.cvr)
                if company is None:
                    self.store.mark_detail_done(task.cvr)
                    continue
                VirkClient.enrich_with_detail(company, detail)
                self.store.upsert_company_detail(company)
                self.store.mark_detail_done(task.cvr)
                LOGGER.debug("Virk 详情完成：%s %s rep=%s email=%s",
                             task.cvr, company.company_name,
                             company.representative or "-", company.email or "-")
            except Exception as exc:
                retries = task.retries + 1
                if retries >= self.config.max_task_retries:
                    self.store.mark_detail_failed(task.cvr, str(exc))
                    LOGGER.warning("Virk 详情放弃：%s error=%s", task.cvr, exc)
                else:
                    delay = _retry_delay(retries, self.config.retry_backoff_cap_seconds)
                    self.store.defer_detail_task(task.cvr, retries, delay, str(exc))

    # ---- GMap Worker ----

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._do_gmap(task)

    def _do_gmap(self, task: VirkGMapTask) -> None:
        LOGGER.info("Virk GMap 开始：%s | %s", task.cvr, task.company_name)
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
            LOGGER.warning("Virk GMap 异常：%s | %s | 第%d次 | %s",
                           task.cvr, task.company_name, retries, exc)
            if retries >= self.config.gmap_max_retries:
                self.store.mark_gmap_failed(cvr=task.cvr, error_text=str(exc))
                return
            self.store.defer_gmap_task(
                cvr=task.cvr, retries=retries,
                delay_seconds=_retry_delay(retries, self.config.retry_backoff_cap_seconds),
                error_text=str(exc),
            )
            return
        self.store.mark_gmap_done(
            cvr=task.cvr, website=result.website,
            source="gmap" if result.website else "",
            phone=result.phone, company_name=result.company_name,
        )
        LOGGER.info("Virk GMap 完成：%s | 官网=%s", task.cvr, result.website or "-")

    def _get_gmap_client(self) -> GoogleMapsClient:
        client = getattr(self._gmap_local, "client", None)
        if client is None:
            client = GoogleMapsClient(GoogleMapsConfig(
                proxy_url=self.config.proxy_url,
            ))
            self._gmap_local.client = client
        return client

    # ---- Protocol+LLM 邮箱/代表人发现 Worker ----

    def _email_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: VirkFirecrawlTask) -> None:
        """处理单个邮箱/代表人发现任务。"""
        if not task.website:
            self.store.mark_firecrawl_failed(cvr=task.cvr, error_text="缺少官网")
            return
        # 域名缓存检查（同域名不重复爬）
        decision = self._firecrawl_domain_cache.prepare_lookup(task.domain)
        if decision.status == "done":
            self.store.mark_firecrawl_done(
                cvr=task.cvr, emails=decision.emails or [],
                retry_after_seconds=0.0,
            )
            LOGGER.info("Virk 邮箱 命中缓存：%s | 域名=%s | 邮箱=%d",
                        task.cvr, task.domain, len(decision.emails or []))
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                cvr=task.cvr, retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.config.queue_poll_interval),
                error_text="等待同域名查询完成",
            )
            return
        LOGGER.info("Virk 邮箱 开始：%s | 域名=%s", task.cvr, task.domain)
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
            task.domain, result.emails,
            retry_after_seconds=result.retry_after_seconds,
        )
        self.store.mark_firecrawl_done(
            cvr=task.cvr,
            emails=result.emails,
            representative=result.representative,
            company_name=result.company_name,
            evidence_url=result.evidence_url,
            retry_after_seconds=result.retry_after_seconds,
        )
        LOGGER.info("Virk 邮箱 完成：%s | 域名=%s | 邮箱=%d | 代表人=%s | 官网公司名=%s",
                    task.cvr, task.domain, len(result.emails),
                    result.representative or "-", result.company_name or "-")

    def _handle_firecrawl_failure(self, task: VirkFirecrawlTask, exc: Exception) -> None:
        attempt = task.retries + 1
        delay = _retry_delay(attempt, self.config.retry_backoff_cap_seconds)
        # 认证/配额错误——最终失败
        if isinstance(exc, FirecrawlError) and getattr(exc, 'code', '') in {"firecrawl_401", "firecrawl_402"}:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(cvr=task.cvr, error_text=str(exc))
            return
        # 达到最大重试次数
        if attempt >= self.config.firecrawl_task_max_retries:
            self._firecrawl_domain_cache.mark_done(task.domain, [])
            self.store.mark_firecrawl_failed(cvr=task.cvr, error_text=str(exc))
            return
        # 还有重试机会
        if isinstance(exc, FirecrawlError) and getattr(exc, 'code', '') == "firecrawl_429":
            delay = max(float(getattr(exc, 'retry_after', 0) or 5.0), 5.0)
        self._firecrawl_domain_cache.defer(task.domain, delay_seconds=delay, error_text=str(exc))
        self.store.defer_firecrawl_task(
            cvr=task.cvr, retries=attempt,
            delay_seconds=delay, error_text=str(exc),
        )
        LOGGER.warning("Virk 邮箱 重试：%s | 域名=%s | 第%d次 | 等待=%.1fs | 原因=%s",
                       task.cvr, task.domain, attempt, delay, exc)

    def _get_firecrawl_service(self) -> FirecrawlEmailService:
        """每线程独立的 FirecrawlEmailService 实例。"""
        service = getattr(self._firecrawl_local, "service", None)
        if service is not None:
            return service
        firecrawl_client = None
        # 优先使用协议爬虫后端
        if self.config.crawl_backend == "protocol" and SiteCrawlClient is not None:
            LOGGER.info("Virk 邮箱 使用协议爬虫后端 (CRAWL_BACKEND=protocol)")
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
            company_name=company_name,
            homepage=homepage,
            domain=domain,
        )


def run_virk_pipeline(
    config: VirkConfig,
    client: VirkClient,
    *,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
) -> None:
    """启动 Virk 完整 pipeline。"""
    runner = VirkPipelineRunner(
        config=config, client=client,
        skip_gmap=skip_gmap, skip_firecrawl=skip_firecrawl,
    )
    runner.run()
