"""英国 xlsx + Companies House + GMap + Firecrawl 并行管道。"""

from __future__ import annotations

import logging
import json
import threading
import time
from pathlib import Path

from england_crawler.companies_house.client import CompaniesHouseClient
from england_crawler.companies_house.client import select_best_candidate
from england_crawler.companies_house.input_source import iter_company_names_from_source
from england_crawler.companies_house.store import CompaniesHouseStore
from england_crawler.google_maps import GoogleMapsClient
from england_crawler.google_maps import GoogleMapsConfig
from england_crawler.fc_email.client import FirecrawlError
from england_crawler.fc_email.domain_cache import FirecrawlDomainCache
from england_crawler.fc_email.email_service import FirecrawlEmailService
from england_crawler.fc_email.email_service import FirecrawlEmailSettings
from england_crawler.snov.client import extract_domain


logger = logging.getLogger(__name__)
IMPORT_BATCH_SIZE = 1000
IMPORT_PROGRESS_INTERVAL = 10000


def _clip_text(value: object, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _config_int(config: object, name: str, default: int) -> int:
    value = getattr(config, name, default)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def _config_float(config: object, name: str, default: float) -> float:
    value = getattr(config, name, default)
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return default


def _config_path(config: object, name: str, default: Path) -> Path:
    value = getattr(config, name, default)
    return Path(value).resolve()


def _backoff_seconds(retries: int, cap_seconds: float) -> float:
    return min(float(2**max(retries, 1)), max(cap_seconds, 1.0))


def _fingerprint(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _load_scope(max_companies: int) -> str:
    return f"limit:{max_companies}" if max(int(max_companies), 0) > 0 else "full"


class CompaniesHousePipelineRunner:
    """协调三段并行管道。"""

    def __init__(
        self,
        config: object,
        *,
        skip_ch: bool,
        skip_gmap: bool,
        skip_firecrawl: bool | None = None,
        skip_snov: bool | None = None,
    ) -> None:
        if skip_firecrawl is None:
            skip_firecrawl = bool(skip_snov)
        self.config = config
        self.skip_ch = skip_ch
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = bool(skip_firecrawl)
        self.output_dir = _config_path(config, "output_dir", Path("output/companies_house"))
        self.source_xlsx_path = _config_path(
            config,
            "input_xlsx",
            _config_path(
                config,
                "source_xlsx_path",
                _config_path(config, "project_root", Path(".")) / "docs" / "英国.xlsx",
            ),
        )
        self.store = CompaniesHouseStore(
            _config_path(config, "store_db_path", self.output_dir / "store.db")
        )
        self.stop_event = threading.Event()
        self.poll_interval = _config_float(config, "queue_poll_interval", 2.0)
        self.stale_running_seconds = _config_int(
            config,
            "stale_running_requeue_seconds",
            600,
        )
        self.snapshot_interval = _config_float(config, "snapshot_flush_interval", 30.0)
        self.retry_backoff_cap_seconds = _config_float(
            config,
            "retry_backoff_cap_seconds",
            180.0,
        )
        self.ch_workers = max(_config_int(config, "ch_workers", 2), 1)
        self.gmap_workers = max(_config_int(config, "gmap_workers", 32), 1)
        self.firecrawl_workers = max(_config_int(config, "snov_workers", 4), 1)
        self.max_companies = _config_int(config, "max_companies", 0)
        self.ch_task_max_retries = max(_config_int(config, "ch_max_retries", 4), 1)
        self.gmap_task_max_retries = max(_config_int(config, "gmap_max_retries", 3), 1)
        self.firecrawl_task_max_retries = max(_config_int(config, "snov_task_max_retries", 5), 1)
        self._firecrawl_local = threading.local()
        self.firecrawl_service_config = FirecrawlEmailSettings(
            project_root=_config_path(config, "project_root", Path(".")),
            keys_inline=list(getattr(config, "firecrawl_keys_inline", []) or []),
            keys_file=Path(getattr(config, "firecrawl_keys_file", self.output_dir / "firecrawl_keys.txt")),
            pool_db=Path(getattr(config, "firecrawl_pool_db", self.output_dir / "cache" / "firecrawl_keys.db")),
            domain_cache_db=_config_path(config, "project_root", Path(".")) / "output" / "firecrawl_cache.db",
            base_url=str(getattr(config, "firecrawl_base_url", "https://api.firecrawl.dev/v2/") or "").strip(),
            timeout_seconds=_config_float(config, "firecrawl_timeout_seconds", 45.0),
            max_retries=max(_config_int(config, "firecrawl_max_retries", 2), 0),
            per_key_limit=max(_config_int(config, "firecrawl_key_per_limit", 2), 1),
            key_wait_seconds=max(_config_int(config, "firecrawl_key_wait_seconds", 20), 1),
            key_cooldown_seconds=max(_config_int(config, "firecrawl_key_cooldown_seconds", 90), 1),
            key_failure_threshold=max(_config_int(config, "firecrawl_key_failure_threshold", 5), 1),
            llm_api_key=str(getattr(config, "llm_api_key", "") or "").strip(),
            llm_base_url=str(getattr(config, "llm_base_url", "https://api.gpteamservices.com/v1") or "").strip(),
            llm_model=str(getattr(config, "llm_model", "gpt-5.1-codex-mini") or "").strip(),
            llm_reasoning_effort=str(getattr(config, "llm_reasoning_effort", "medium") or "").strip(),
            llm_timeout_seconds=_config_float(config, "llm_timeout_seconds", 120.0),
            candidate_limit=max(_config_int(config, "firecrawl_prefilter_limit", 24), 1),
            llm_pick_limit=max(_config_int(config, "firecrawl_llm_pick_count", 12), 1),
        )
        self.firecrawl_domain_cache = FirecrawlDomainCache(self.config.project_root / "output" / "firecrawl_cache.db")
        self.snov_domain_cache = self.firecrawl_domain_cache
        self._seed_firecrawl_domain_cache()

    def _log_run_plan(self) -> None:
        scope = _load_scope(self.max_companies)
        logger.info(
            "Pipeline 启动：source=%s 范围=%s skip_ch=%s skip_gmap=%s skip_firecrawl=%s workers(ch/gmap/firecrawl)=%d/%d/%d",
            self.source_xlsx_path,
            scope,
            self.skip_ch,
            self.skip_gmap,
            self.skip_firecrawl,
            self.ch_workers,
            self.gmap_workers,
            self.firecrawl_workers,
        )

    def _log_stage_workers(self, stage: str, worker_count: int, skipped: bool) -> None:
        if skipped:
            logger.info("%s 阶段已跳过", stage)
            return
        logger.info("%s 阶段启动：workers=%d", stage, worker_count)

    def _log_retry(self, stage: str, task, retries: int, delay_seconds: float, exc: Exception) -> None:
        logger.warning(
            "%s 重试：%s | 第%d次 | 等待=%.1fs | 原因=%s",
            stage,
            getattr(task, "comp_id", ""),
            retries,
            delay_seconds,
            _clip_text(exc),
        )

    def _log_failed(self, stage: str, task, exc: Exception) -> None:
        logger.warning(
            "%s 失败：%s | %s",
            stage,
            getattr(task, "comp_id", ""),
            _clip_text(exc),
        )

    def _log_finish_summary(self) -> None:
        stats = self.store.get_stats()
        logger.info(
            "Pipeline 完成：companies=%d ch=%d/%d gmap=%d/%d firecrawl=%d/%d final=%d",
            stats["companies_total"],
            stats["ch_done"],
            stats["ch_total"],
            stats["gmap_done"],
            stats["gmap_total"],
            stats["firecrawl_done"],
            stats["firecrawl_total"],
            stats["final_total"],
        )

    def _seed_firecrawl_domain_cache(self) -> None:
        rows = self.store._conn.execute(
            """
            SELECT domain, emails_json
            FROM companies
            WHERE TRIM(domain) != '' AND emails_json != '[]'
            """
        ).fetchall()
        pairs: list[tuple[str, list[str]]] = []
        for row in rows:
            try:
                emails = json.loads(str(row["emails_json"] or "[]"))
            except json.JSONDecodeError:
                emails = []
            if not isinstance(emails, list):
                emails = []
            pairs.append(
                (
                    str(row["domain"]).strip().lower(),
                    [str(item).strip().lower() for item in emails if str(item).strip()],
                )
            )
        self.firecrawl_domain_cache.seed_done(pairs)

    def _firecrawl_delay(self, retries: int, exc: FirecrawlError) -> float:
        if exc.code == "firecrawl_429" and exc.retry_after:
            return max(float(exc.retry_after), 5.0)
        return _backoff_seconds(retries, self.retry_backoff_cap_seconds)

    def _get_firecrawl_service(self) -> FirecrawlEmailService:
        if not hasattr(self._firecrawl_local, "service"):
            self._firecrawl_local.service = FirecrawlEmailService(self.firecrawl_service_config)
        return self._firecrawl_local.service

    def _resolve_firecrawl_domain(self, task) -> str:
        return str(task.domain).strip() or extract_domain(str(task.homepage).strip())

    def run(self) -> None:
        """执行全链路。"""
        threads: list[threading.Thread] = []
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            if not self.source_xlsx_path.exists():
                raise FileNotFoundError(f"未找到源文件：{self.source_xlsx_path}")
            if not self.skip_firecrawl:
                self.firecrawl_service_config.validate()
            self._log_run_plan()
            self._load_source_file()
            self._preflight_ch_proxy()
            threads = self._start_workers()
            self._monitor()
        finally:
            self.stop_event.set()
            for thread in threads:
                thread.join(timeout=5.0)
            self.store.export_jsonl_snapshots(self.output_dir)
            self._log_finish_summary()
            self.firecrawl_domain_cache.close()
            self.store.close()

    def _load_source_file(self) -> None:
        fingerprint = _fingerprint(self.source_xlsx_path)
        scope = _load_scope(self.max_companies)
        if self.store.source_is_loaded(self.source_xlsx_path, fingerprint, scope=scope):
            logger.info("xlsx 已载入，跳过重复导入：%s", self.source_xlsx_path)
            return

        logger.info(
            "开始导入 xlsx：文件=%s 范围=%s",
            self.source_xlsx_path,
            scope,
        )
        batch: list[str] = []
        inserted = 0
        loaded = 0
        last_progress = 0
        for company_name in iter_company_names_from_source(
            self.source_xlsx_path,
            limit=self.max_companies,
        ):
            batch.append(company_name)
            loaded += 1
            if len(batch) < IMPORT_BATCH_SIZE:
                if loaded - last_progress >= IMPORT_PROGRESS_INTERVAL:
                    logger.info(
                        "xlsx 导入中：已扫描=%d 已新增=%d",
                        loaded,
                        inserted,
                    )
                    last_progress = loaded
                continue
            inserted += self.store.import_company_names(batch)
            batch.clear()
            if loaded - last_progress >= IMPORT_PROGRESS_INTERVAL:
                logger.info(
                    "xlsx 导入中：已扫描=%d 已新增=%d",
                    loaded,
                    inserted,
                )
                last_progress = loaded
        if batch:
            inserted += self.store.import_company_names(batch)
        self.store.mark_source_loaded(
            self.source_xlsx_path,
            fingerprint,
            loaded,
            scope=scope,
        )
        logger.info(
            "xlsx 导入完成：文件=%s 去重后=%d 新增=%d",
            self.source_xlsx_path,
            loaded,
            inserted,
        )

    def _start_workers(self) -> list[threading.Thread]:
        threads: list[threading.Thread] = []
        self._log_stage_workers("CH", self.ch_workers, self.skip_ch)
        if not self.skip_ch:
            threads.extend(
                self._build_threads("ch", self.ch_workers, self._run_ch_worker)
            )
        self._log_stage_workers("GMap", self.gmap_workers, self.skip_gmap)
        if not self.skip_gmap:
            threads.extend(
                self._build_threads("gmap", self.gmap_workers, self._run_gmap_worker)
            )
        self._log_stage_workers("Firecrawl", self.firecrawl_workers, self.skip_firecrawl)
        if not self.skip_firecrawl:
            threads.extend(
                self._build_threads("firecrawl", self.firecrawl_workers, self._run_firecrawl_worker)
            )
        for thread in threads:
            thread.start()
        return threads

    def _build_threads(
        self,
        prefix: str,
        count: int,
        target,
    ) -> list[threading.Thread]:
        return [
            threading.Thread(
                target=target,
                name=f"companies-house-{prefix}-{index + 1}",
                daemon=True,
            )
            for index in range(max(count, 0))
        ]

    def _preflight_ch_proxy(self) -> None:
        if self.skip_ch or not self.config.ch_proxy.enabled:
            return
        client = CompaniesHouseClient(
            timeout=_config_float(self.config, "ch_timeout_seconds", 30.0),
            proxy_config=self.config.ch_proxy,
            worker_label="companies-house-ch-probe",
        )
        try:
            ok, detail = client.probe_proxy()
            if not ok:
                raise RuntimeError(
                    "CH 代理自检失败："
                    f"proxy={client.describe_proxy()} "
                    f"preproxy={client.describe_preproxy()} "
                    f"session={client.current_session_label()} "
                    f"原因={detail}"
                )
            logger.info(
                "CH 代理自检通过：proxy=%s preproxy=%s session=%s exit_ip=%s",
                client.describe_proxy(),
                client.describe_preproxy(),
                client.current_session_label(),
                detail,
            )
        finally:
            client.close()

    def _run_ch_worker(self) -> None:
        worker_name = threading.current_thread().name
        client = CompaniesHouseClient(
            timeout=_config_float(self.config, "ch_timeout_seconds", 30.0),
            proxy_config=self.config.ch_proxy,
            worker_label=worker_name,
        )
        probe_ok, probe_detail = client.probe_proxy()
        logger.info(
            "CH 代理会话：worker=%s session=%s proxy=%s preproxy=%s exit_ip=%s",
            worker_name,
            client.current_session_label(),
            client.describe_proxy(),
            client.describe_preproxy(),
            probe_detail,
        )
        try:
            while not self.stop_event.is_set():
                task = self.store.claim_ch_task()
                if task is None:
                    time.sleep(self.poll_interval)
                    continue
                if self.config.ch_proxy.enabled and not probe_ok:
                    raise RuntimeError(f"CH 代理不可用：{probe_detail}")
                self._handle_ch_task(client, task)
        finally:
            client.close()

    def _handle_ch_task(self, client: CompaniesHouseClient, task) -> None:
        logger.info("CH 开始：%s | %s", task.comp_id, task.company_name)
        try:
            logger.info("CH 查询：%s | %s", task.comp_id, task.company_name)
            candidates = client.search_companies(task.company_name)
            candidate = select_best_candidate(task.company_name, candidates)
            if candidate is None:
                self.store.mark_ch_done(
                    comp_id=task.comp_id,
                    company_number="",
                    company_status="not_found",
                    ceo="",
                )
                logger.info("CH 未命中：%s | 公司号= | 代表人=", task.comp_id)
                return
            logger.info(
                "CH 命中：%s | 公司号=%s | 状态=%s",
                task.comp_id,
                candidate.company_number,
                candidate.status_text,
            )
            logger.info(
                "CH Officers 查询：%s | 公司号=%s",
                task.comp_id,
                candidate.company_number,
            )
            ceo = client.fetch_first_active_director(candidate.company_number)
            self.store.mark_ch_done(
                comp_id=task.comp_id,
                company_number=candidate.company_number,
                company_status=candidate.status_text,
                ceo=ceo,
            )
            logger.info(
                "CH 完成：%s | 公司号=%s | 代表人=%s",
                task.comp_id,
                candidate.company_number,
                ceo,
            )
        except Exception as exc:
            retries = task.retries + 1
            if retries < self.ch_task_max_retries:
                delay_seconds = _backoff_seconds(retries, self.retry_backoff_cap_seconds)
                self.store.defer_ch_task(
                    comp_id=task.comp_id,
                    retries=retries,
                    delay_seconds=delay_seconds,
                    error_text=str(exc),
                )
                self._log_retry("CH", task, retries, delay_seconds, exc)
                return
            self.store.mark_ch_failed(comp_id=task.comp_id, error_text=str(exc))
            self._log_failed("CH", task, exc)

    def _run_gmap_worker(self) -> None:
        client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="gb"))
        try:
            while not self.stop_event.is_set():
                task = self.store.claim_gmap_task()
                if task is None:
                    time.sleep(self.poll_interval)
                    continue
                self._handle_gmap_task(client, task)
        finally:
            client.close()

    def _handle_gmap_task(self, client: GoogleMapsClient, task) -> None:
        logger.info("GMap 开始：%s | %s", task.comp_id, task.company_name)
        try:
            logger.info("GMap 查询：%s | %s", task.comp_id, task.company_name)
            profile = client.search_company_profile(task.company_name, task.company_name)
            self.store.mark_gmap_done(
                comp_id=task.comp_id,
                homepage=profile.website,
                phone=profile.phone,
            )
            logger.info(
                "GMap 完成：%s | 官网=%s | 电话=%s",
                task.comp_id,
                profile.website,
                profile.phone,
            )
        except Exception as exc:
            retries = task.retries + 1
            if retries < self.gmap_task_max_retries:
                delay_seconds = _backoff_seconds(retries, self.retry_backoff_cap_seconds)
                self.store.defer_gmap_task(
                    comp_id=task.comp_id,
                    retries=retries,
                    delay_seconds=delay_seconds,
                    error_text=str(exc),
                )
                self._log_retry("GMap", task, retries, delay_seconds, exc)
                return
            self.store.mark_gmap_failed(comp_id=task.comp_id, error_text=str(exc))
            self._log_failed("GMap", task, exc)

    def _run_firecrawl_worker(self) -> None:
        try:
            while not self.stop_event.is_set():
                task = self.store.claim_firecrawl_task()
                if task is None:
                    time.sleep(self.poll_interval)
                    continue
                self._handle_firecrawl_task(task)
        finally:
            service = getattr(self._firecrawl_local, "service", None)
            if service is not None:
                service.close()

    def _handle_firecrawl_task(self, task) -> None:
        domain = self._resolve_firecrawl_domain(task)
        if not domain:
            self.store.mark_firecrawl_failed(comp_id=task.comp_id, error_text="缺少域名")
            return
        decision = self.firecrawl_domain_cache.prepare_lookup(domain)
        if decision.status == "done":
            self.store.mark_firecrawl_done(comp_id=task.comp_id, emails=decision.emails)
            logger.info("Firecrawl 命中缓存：%s | 域名=%s | 邮箱=%d", task.comp_id, domain, len(decision.emails))
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                comp_id=task.comp_id,
                retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.poll_interval),
                error_text="等待同域名查询完成",
            )
            logger.info("Firecrawl 等待同域名：%s | 域名=%s", task.comp_id, domain)
            return
        try:
            logger.info("Firecrawl 开始：%s | 域名=%s", task.comp_id, domain)
            emails = self._get_firecrawl_service().get_domain_emails(domain)
            self.firecrawl_domain_cache.mark_done(domain, emails)
            self.store.mark_firecrawl_done(comp_id=task.comp_id, emails=emails)
            logger.info("Firecrawl 完成：%s | 域名=%s | 邮箱=%d", task.comp_id, domain, len(emails))
        except FirecrawlError as exc:
            retries = task.retries + 1
            delay_seconds = self._firecrawl_delay(retries, exc)
            self.firecrawl_domain_cache.defer(
                domain,
                delay_seconds=delay_seconds,
                error_text=str(exc),
            )
            if exc.code in {"firecrawl_401", "firecrawl_402"} or retries >= self.firecrawl_task_max_retries:
                self.store.mark_firecrawl_failed(comp_id=task.comp_id, error_text=str(exc))
                self._log_failed("Firecrawl", task, exc)
                return
            self.store.defer_firecrawl_task(
                comp_id=task.comp_id,
                retries=retries,
                delay_seconds=delay_seconds,
                error_text=str(exc),
            )
            self._log_retry("Firecrawl", task, retries, delay_seconds, exc)
        except Exception as exc:
            retries = task.retries + 1
            delay_seconds = _backoff_seconds(retries, self.retry_backoff_cap_seconds)
            self.firecrawl_domain_cache.defer(
                domain,
                delay_seconds=delay_seconds,
                error_text=str(exc),
            )
            if retries < self.firecrawl_task_max_retries:
                self.store.defer_firecrawl_task(
                    comp_id=task.comp_id,
                    retries=retries,
                    delay_seconds=delay_seconds,
                    error_text=str(exc),
                )
                self._log_retry("Firecrawl", task, retries, delay_seconds, exc)
                return
            self.store.mark_firecrawl_failed(comp_id=task.comp_id, error_text=str(exc))
            self._log_failed("Firecrawl", task, exc)

    def _monitor(self) -> None:
        last_progress = 0.0
        last_export = 0.0
        last_requeue = 0.0
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now - last_requeue >= 60.0:
                recovered = self.store.requeue_stale_running_tasks(
                    older_than_seconds=self.stale_running_seconds
                )
                if recovered:
                    logger.warning("检测到卡死任务并已回收：%d", recovered)
                last_requeue = now
            if now - last_export >= self.snapshot_interval:
                self.store.export_jsonl_snapshots(self.output_dir)
                logger.info("快照刷新：%s", self.output_dir)
                last_export = now
            if now - last_progress >= 5.0:
                stats = self.store.get_stats()
                logger.info(
                    "进度：companies=%d ch=%d/%d (run=%d pending=%d) gmap=%d/%d (run=%d pending=%d) firecrawl=%d/%d (run=%d pending=%d) final=%d",
                    stats["companies_total"],
                    stats["ch_done"],
                    stats["ch_total"],
                    stats["ch_running"],
                    stats["ch_pending"],
                    stats["gmap_done"],
                    stats["gmap_total"],
                    stats["gmap_running"],
                    stats["gmap_pending"],
                    stats["firecrawl_done"],
                    stats["firecrawl_total"],
                    stats["firecrawl_running"],
                    stats["firecrawl_pending"],
                    stats["final_total"],
                )
                last_progress = now
            if self._all_done():
                return
            time.sleep(self.poll_interval)

    def _all_done(self) -> bool:
        ch_done = self.skip_ch or self.store.queue_done("ch_queue")
        gmap_done = self.skip_gmap or self.store.queue_done("gmap_queue")
        firecrawl_done = self.skip_firecrawl or (
            ch_done and gmap_done and self.store.queue_done("snov_queue")
        )
        return ch_done and gmap_done and firecrawl_done


def run_companies_house_pipeline(
    config: object,
    *,
    skip_ch: bool = False,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
    skip_snov: bool | None = None,
) -> None:
    """运行英国新站点。"""
    if skip_snov is not None:
        skip_firecrawl = skip_snov
    runner = CompaniesHousePipelineRunner(
        config,
        skip_ch=skip_ch,
        skip_gmap=skip_gmap,
        skip_firecrawl=skip_firecrawl,
    )
    runner.run()






