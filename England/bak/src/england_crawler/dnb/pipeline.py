"""英国 DNB 流式主流程。"""

from __future__ import annotations

import logging
import json
import re
import threading
import time
from pathlib import Path

from england_crawler.dnb.catalog import INDUSTRY_CATEGORY_COUNT
from england_crawler.dnb.catalog import INDUSTRY_PAGE_COUNT
from england_crawler.dnb.catalog import INDUSTRY_SUBCATEGORY_COUNT
from england_crawler.dnb.catalog import build_industry_seed_segments
from england_crawler.dnb.client import DnbClient
from england_crawler.dnb.client import extract_child_segments
from england_crawler.dnb.client import parse_company_listing
from england_crawler.dnb.client import parse_company_profile
from england_crawler.dnb.config import DnbEnglandConfig
from england_crawler.dnb.runtime.detail_queue import DetailQueueStore
from england_crawler.dnb.runtime.detail_queue import DetailTask
from england_crawler.dnb.store import DnbEnglandStore
from england_crawler.dnb.store import FirecrawlTask
from england_crawler.dnb.store import GMapTask
from england_crawler.dnb.seed_segments import load_seed_rows
from england_crawler.google_maps import GoogleMapsClient
from england_crawler.google_maps import GoogleMapsConfig
from england_crawler.google_maps import GoogleMapsPlaceResult
from england_crawler.fc_email.client import FirecrawlError
from england_crawler.fc_email.domain_cache import FirecrawlDomainCache
from england_crawler.fc_email.email_service import FirecrawlEmailService
from england_crawler.fc_email.email_service import FirecrawlEmailSettings
from england_crawler.snov.client import extract_domain


logger = logging.getLogger(__name__)

LIST_PAGE_SIZE = 50
SNAPSHOT_INTERVAL_SECONDS = 30.0
DETAIL_BACKLOG_SOFT_LIMIT = 5000
FIRECRAWL_KEY_UNAVAILABLE_DELAY_SECONDS = 60.0
CORP_SUFFIX_PATTERNS = (
    r"\bco\.?\s*,?\s*ltd\.?\b",
    r"\bcorporation\b",
    r"\bcorp\.?\b",
    r"\bcompany\b",
    r"\blimited\b",
    r"\bltd\.?\b",
)


def _backoff_seconds(*, attempt: int, base: float = 5.0, cap: float = 180.0) -> float:
    return min(max(base, 1.0) * (2 ** max(attempt - 1, 0)), max(cap, 1.0))


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _strip_company_suffix(name: str) -> str:
    value = _normalize_text(name)
    lowered = value
    for pattern in CORP_SUFFIX_PATTERNS:
        lowered = re.sub(pattern, "", lowered, flags=re.I).strip(" ,()-")
    return _normalize_text(lowered)


def _build_gmap_queries(task: GMapTask) -> list[str]:
    parts = [task.city, task.region, task.country or "United Kingdom"]
    names = [task.company_name_en, _strip_company_suffix(task.company_name_en)]
    queries: list[str] = []
    for name in names:
        for suffix in (
            " ".join(part for part in parts if part),
            f"{task.region} {task.country}".strip(),
            task.country,
        ):
            query = _normalize_text(" ".join(part for part in [name, suffix] if part))
            if query and query not in queries:
                queries.append(query)
    if task.company_name_en and task.company_name_en not in queries:
        queries.append(task.company_name_en)
    return queries


def _merge_place_results(current: GoogleMapsPlaceResult, incoming: GoogleMapsPlaceResult) -> GoogleMapsPlaceResult:
    return GoogleMapsPlaceResult(
        company_name=current.company_name or incoming.company_name,
        phone=current.phone or incoming.phone,
        website=current.website or incoming.website,
        score=max(int(current.score), int(incoming.score)),
    )


def _create_dnb_client_factory(base_client: DnbClient):
    thread_local = threading.local()

    def _get_client() -> DnbClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = DnbClient(
                rate_config=base_client.rate_config,
                cookie_header=base_client.cookie_header,
                cookie_provider=base_client.cookie_provider,
            )
        return thread_local.client

    return _get_client


def _detail_backlog_exceeded(*, companies_total: int, detail_done: int, pending_tasks: int) -> bool:
    backlog = max(int(companies_total) - int(detail_done), 0)
    return backlog >= DETAIL_BACKLOG_SOFT_LIMIT or int(pending_tasks) >= DETAIL_BACKLOG_SOFT_LIMIT


def _build_seed_rows(country_iso_two_code: str) -> list[dict[str, object]]:
    return [
        {
            "segment_id": segment.segment_id,
            "industry_path": segment.industry_path,
            "country_iso_two_code": segment.country_iso_two_code,
            "region_name": segment.region_name,
            "city_name": segment.city_name,
            "expected_count": segment.expected_count,
        }
        for segment in build_industry_seed_segments(country_iso_two_code)
    ]


def _build_page_signature(rows) -> tuple[str, ...]:
    signature: list[str] = []
    for row in rows[:10]:
        value = (
            str(getattr(row, "duns", "")).strip()
            or str(getattr(row, "company_name_url", "")).strip()
            or str(getattr(row, "company_name_en_dnb", "")).strip()
        )
        if value:
            signature.append(value)
    return tuple(signature)


class DnbEnglandPipelineRunner:
    """英国 DNB 流式执行器。"""

    def __init__(
        self,
        *,
        config: DnbEnglandConfig,
        client: DnbClient,
        skip_dnb: bool,
        skip_gmap: bool,
        skip_firecrawl: bool | None = None,
        skip_snov: bool | None = None,
    ) -> None:
        if skip_firecrawl is None:
            skip_firecrawl = bool(skip_snov)
        self.config = config
        self.client = client
        self.skip_dnb = skip_dnb
        self.skip_gmap = skip_gmap
        self.skip_firecrawl = bool(skip_firecrawl)
        self.store = DnbEnglandStore(config.store_db_path)
        self.detail_queue = DetailQueueStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.dnb_done = threading.Event()
        self._dnb_workers_remaining = config.dnb_pipeline_workers
        self._dnb_workers_lock = threading.Lock()
        self._seed_log_lock = threading.Lock()
        self._seed_log_emitted = False
        self._dnb_client_factory = _create_dnb_client_factory(client)
        self._gmap_local = threading.local()
        self._firecrawl_local = threading.local()
        self._firecrawl_settings = FirecrawlEmailSettings(
            keys_inline=list(self.config.firecrawl_keys_inline or []),
            keys_file=Path(self.config.firecrawl_keys_file or (self.config.output_dir / "firecrawl_keys.txt")),
            pool_db=Path(self.config.firecrawl_pool_db or (self.config.output_dir / "cache" / "firecrawl_keys.db")),
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
        self._firecrawl_key_pool = None
        self._firecrawl_key_pool_lock = threading.Lock()
        self._firecrawl_domain_cache = FirecrawlDomainCache(self.config.project_root / "output" / "firecrawl_cache.db")
        self._snov_domain_cache = self._firecrawl_domain_cache
        self._seed_firecrawl_domain_cache()

    def run(self) -> None:
        self.config.validate(skip_firecrawl=self.skip_firecrawl)
        foreign_countries = self.store.foreign_country_counts("United Kingdom")
        if foreign_countries:
            sample = ", ".join(f"{name}={count}" for name, count in foreign_countries[:5])
            max_foreign_count = max(count for _name, count in foreign_countries)
            if max_foreign_count >= 100:
                raise RuntimeError(
                    "England/output/dnb/store.db 中检测到大批历史非英国数据，请先清理旧断点后重跑。"
                    f" 当前发现：{sample}"
                )
            logger.warning(
                "England store 中存在少量非英国记录，继续运行：%s",
                sample,
            )
        workers: list[threading.Thread] = []
        if self.skip_dnb:
            self.dnb_done.set()
        else:
            self.detail_queue.sync_from_companies()
            for idx in range(self.config.dnb_pipeline_workers):
                workers.append(
                    threading.Thread(target=self._dnb_worker, name=f"DNB-UK-{idx+1}", daemon=True)
                )
            for idx in range(self.config.dnb_workers):
                workers.append(
                    threading.Thread(target=self._detail_worker, name=f"DNB-Detail-UK-{idx+1}", daemon=True)
                )
        if not self.skip_gmap:
            for idx in range(self.config.gmap_workers):
                workers.append(threading.Thread(target=self._gmap_worker, name=f"GMap-UK-{idx+1}", daemon=True))
        if not self.skip_firecrawl:
            for idx in range(self.config.snov_workers):
                workers.append(threading.Thread(target=self._firecrawl_worker, name=f"Firecrawl-UK-{idx+1}", daemon=True))
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
            self.detail_queue.close()
            self.store.close()

    def _monitor_until_done(self) -> None:
        last_log = 0.0
        last_snapshot = 0.0
        last_zero_retry = 0.0
        while not self.stop_event.is_set():
            self.store.requeue_stale_running_tasks(
                older_than_seconds=self.config.stale_running_requeue_seconds
            )
            now = time.monotonic()
            if now - last_zero_retry >= 60.0:
                revived = self.store.requeue_expired_firecrawl_tasks()
                if revived:
                    logger.info("Firecrawl 0结果到期，已回队列：%d", revived)
                last_zero_retry = now
            if now - last_snapshot >= SNAPSHOT_INTERVAL_SECONDS:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snapshot = now
            if now - last_log >= 10.0:
                stats = self.store.get_stats()
                logger.info(
                    "进度：segments=%d/%d detail=%d/%d gmap=%d/%d firecrawl=%d/%d final=%d",
                    stats["segments_done"],
                    stats["segments_total"],
                    stats["companies_detail_done"],
                    stats["companies_total"],
                    stats["gmap_running"],
                    stats["gmap_pending"],
                    stats["firecrawl_running"],
                    stats["firecrawl_pending"],
                    stats["final_total"],
                )
                last_log = now
            if self.dnb_done.is_set() and self._queues_idle():
                return
            time.sleep(self.config.queue_poll_interval)

    def _queues_idle(self) -> bool:
        stats = self.store.get_stats()
        detail_pending, detail_running = self.detail_queue.stats()
        gmap_busy = 0 if self.skip_gmap else stats["gmap_pending"] + stats["gmap_running"]
        firecrawl_busy = 0 if self.skip_firecrawl else stats["firecrawl_pending"] + stats["firecrawl_running"]
        detail_busy = 0 if self.skip_dnb else detail_pending + detail_running
        return (detail_busy + gmap_busy + firecrawl_busy) == 0

    def _wait_for_detail_backlog(self) -> None:
        if self.skip_dnb:
            return
        while not self.stop_event.is_set():
            stats = self.store.get_stats()
            detail_pending, _detail_running = self.detail_queue.stats()
            if not _detail_backlog_exceeded(
                companies_total=stats["companies_total"],
                detail_done=stats["companies_detail_done"],
                pending_tasks=detail_pending,
            ):
                return
            logger.info(
                "DNB 明细积压过高，暂停新切片：detail=%d/%d pending=%d",
                stats["companies_detail_done"],
                stats["companies_total"],
                detail_pending,
            )
            time.sleep(max(self.config.queue_poll_interval, 2.0))

    def _seed_rows(self) -> list[dict[str, object]]:
        if self.config.seed_file_path:
            return load_seed_rows(self.config.seed_file_path)
        return _build_seed_rows("gb")

    def _dnb_worker(self) -> None:
        try:
            dnb_client = self._dnb_client_factory()
            seed_rows = self._seed_rows()
            self.store.ensure_seed_signature(seed_rows)
            self.store.ensure_discovery_seeds(seed_rows)
            if self.store.has_discovery_work():
                self._log_seed_loaded_once()
                self._discover_stable_segments(dnb_client)
            produced = 0
            while not self.stop_event.is_set():
                self._wait_for_detail_backlog()
                cursor = self.store.claim_segment(LIST_PAGE_SIZE)
                if cursor is None:
                    break
                seen_page_signatures: set[tuple[str, ...]] = set()
                page_number = cursor.next_page
                while page_number <= cursor.total_pages and not self.stop_event.is_set():
                    self._wait_for_detail_backlog()
                    logger.info(
                        "DNB 抓取切片：%s 第 %d/%d 页",
                        cursor.segment_id,
                        page_number,
                        cursor.total_pages,
                    )
                    try:
                        payload = dnb_client.fetch_company_listing_page(
                            segment=self._cursor_to_segment(cursor),
                            page_number=page_number,
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "DNB 切片页失败，已回队列：%s 第 %d/%d 页，原因=%s",
                            cursor.segment_id,
                            page_number,
                            cursor.total_pages,
                            exc,
                        )
                        self.store.reset_segment(cursor.segment_id)
                        time.sleep(min(_backoff_seconds(attempt=1), 5.0))
                        break
                    rows = parse_company_listing(payload)
                    signature = _build_page_signature(rows)
                    if signature and signature in seen_page_signatures:
                        logger.info(
                            "DNB 页回环，提前结束切片：%s 第 %d/%d 页",
                            cursor.segment_id,
                            page_number,
                            cursor.total_pages,
                        )
                        self.store.advance_segment(
                            cursor.segment_id,
                            cursor.total_pages + 1,
                            cursor.total_pages,
                        )
                        break
                    if signature:
                        seen_page_signatures.add(signature)
                    if not rows:
                        self.store.advance_segment(
                            cursor.segment_id,
                            cursor.total_pages + 1,
                            cursor.total_pages,
                        )
                        break
                    for row in rows:
                        data = row.to_dict()
                        self.store.upsert_company_listing(data)
                        if not self.store.is_company_detail_done(row.duns):
                            self.detail_queue.enqueue(row.duns)
                            produced += 1
                    self.store.advance_segment(cursor.segment_id, page_number + 1, cursor.total_pages)
                    if self.config.max_companies > 0 and produced >= self.config.max_companies:
                        return
                    page_number += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("DNB 英国主流程异常：%s", exc)
        finally:
            with self._dnb_workers_lock:
                self._dnb_workers_remaining -= 1
                if self._dnb_workers_remaining <= 0:
                    logger.info("DNB 英国主流程完成。")
                    self.dnb_done.set()

    def _log_seed_loaded_once(self) -> None:
        with self._seed_log_lock:
            if self._seed_log_emitted:
                return
            logger.info(
                "DNB 全站行业种子已装载：大类=%d 小类=%d 总页=%d",
                INDUSTRY_CATEGORY_COUNT,
                INDUSTRY_SUBCATEGORY_COUNT,
                INDUSTRY_PAGE_COUNT,
            )
            self._seed_log_emitted = True

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
        self._firecrawl_domain_cache.seed_done(pairs)

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.detail_queue.claim()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_detail_task(task)

    def _process_detail_task(self, task: DetailTask) -> None:
        logger.info("DNB 详情开始：%s | %s", task.duns, task.company_name_en_dnb)
        try:
            payload = self._dnb_client_factory().fetch_company_profile(task.company_name_url)
            company = parse_company_profile(
                record=self._record_from_dict(
                    {
                        "duns": task.duns,
                        "company_name_en_dnb": task.company_name_en_dnb,
                        "company_name_url": task.company_name_url,
                        "address": task.address,
                        "city": task.city,
                        "region": task.region,
                        "country": task.country,
                        "postal_code": task.postal_code,
                        "sales_revenue": task.sales_revenue,
                    }
                ),
                payload=payload,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("DNB 详情失败（%s）: %s", task.duns, exc)
            attempt = task.retries + 1
            if attempt >= self.config.detail_task_max_retries:
                self.detail_queue.mark_failed(
                    task.duns,
                    retries=attempt,
                    error_text=str(exc),
                )
                logger.warning(
                    "DNB 详情放弃（%s）: 已达到最大重试次数=%d",
                    task.duns,
                    self.config.detail_task_max_retries,
                )
                return
            delay = _backoff_seconds(
                attempt=attempt,
                base=10.0,
                cap=self.config.retry_backoff_cap_seconds,
            )
            self.detail_queue.defer(
                duns=task.duns,
                retries=attempt,
                delay_seconds=delay,
                error_text=str(exc),
            )
            return
        result = company.to_dict()
        self.store.upsert_company_detail(result)
        self.detail_queue.mark_done(task.duns)
        logger.info(
            "DNB 详情完成：%s | 负责人=%s | 官网=%s",
            result.get("duns", ""),
            result.get("key_principal", ""),
            result.get("dnb_website", ""),
        )

    def _fetch_detail_rows(self, rows: list[dict[str, str]]) -> tuple[int, bool]:
        success = 0
        for row in rows:
            task = DetailTask(
                duns=str(row.get("duns", "")),
                company_name_en_dnb=str(row.get("company_name_en_dnb", "")),
                company_name_url=str(row.get("company_name_url", "")),
                address=str(row.get("address", "")),
                city=str(row.get("city", "")),
                region=str(row.get("region", "")),
                country=str(row.get("country", "")),
                postal_code=str(row.get("postal_code", "")),
                sales_revenue=str(row.get("sales_revenue", "")),
                retries=0,
            )
            before = self.store.is_company_detail_done(task.duns)
            self._process_detail_task(task)
            after = self.store.is_company_detail_done(task.duns)
            if (not before) and after:
                success += 1
        return success, False

    def _discover_stable_segments(self, dnb_client: DnbClient) -> None:
        while not self.stop_event.is_set():
            row = self.store.claim_discovery_node()
            if row is None:
                return
            segment_id = str(row["segment_id"])
            segment = self._row_to_segment(row)
            logger.info("探索切片：%s", segment_id)
            try:
                payload = dnb_client.fetch_company_listing_page(segment=segment, page_number=1)
            except Exception as exc:  # noqa: BLE001
                logger.warning("探索切片失败，已回队列：%s，原因=%s", segment_id, exc)
                self.store.reset_discovery_node(segment_id)
                time.sleep(min(_backoff_seconds(attempt=1), 5.0))
                continue
            expected = int(payload.get("candidatesMatchedQuantityInt", 0) or 0)
            geos = extract_child_segments(
                industry_path=segment.industry_path,
                payload=payload,
                country_iso_two_code=segment.country_iso_two_code,
            )
            if expected > 0:
                self.store.upsert_leaf_segment(
                    segment_id=segment.segment_id,
                    industry_path=segment.industry_path,
                    country_iso_two_code=segment.country_iso_two_code,
                    region_name=segment.region_name,
                    city_name=segment.city_name,
                    expected_count=expected,
                )
            if geos:
                for child in geos:
                    self.store.enqueue_discovery_node(
                        segment_id=child.segment_id,
                        industry_path=child.industry_path,
                        country_iso_two_code=child.country_iso_two_code,
                        region_name=child.region_name,
                        city_name=child.city_name,
                        expected_count=child.expected_count,
                    )
            self.store.mark_discovery_node_done(segment_id, expected_count=expected)

    def _gmap_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_gmap_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_gmap_task(task)

    def _process_gmap_task(self, task: GMapTask) -> None:
        logger.info("GMap 开始：%s | %s", task.duns, task.company_name_en)
        try:
            result = GoogleMapsPlaceResult()
            for query in _build_gmap_queries(task):
                logger.info("GMap 查询：%s | %s", task.duns, query)
                candidate = self._get_gmap_client().search_company_profile(query, company_name=task.company_name_en)
                result = _merge_place_results(result, candidate)
                if result.website:
                    break
        except Exception as exc:  # noqa: BLE001
            self._retry_or_fail_gmap(task, str(exc))
            return
        final_website = result.website or task.dnb_website
        source = "gmap" if result.website else ("dnb" if task.dnb_website else "")
        self.store.mark_gmap_done(
            duns=task.duns,
            website=final_website,
            source=source,
            phone=result.phone,
        )
        current = self.store.get_company(task.duns) or {}
        logger.info(
            "GMap 完成：%s | 官网=%s | 来源=%s",
            task.duns,
            str(current.get("website", "")).strip(),
            str(current.get("website_source", "")).strip(),
        )

    def _firecrawl_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_firecrawl_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_firecrawl_task(task)

    def _process_firecrawl_task(self, task: FirecrawlTask) -> None:
        effective_domain = str(task.domain or '').strip().lower() or extract_domain(task.homepage)
        if not effective_domain:
            self.store.mark_firecrawl_failed(duns=task.duns, error_text="缺少域名")
            return
        decision = self._firecrawl_domain_cache.prepare_lookup(effective_domain)
        if decision.status == "done":
            self.store.mark_firecrawl_done(
                duns=task.duns,
                emails=decision.emails,
                retry_after_seconds=decision.retry_after_seconds,
            )
            logger.info("Firecrawl 命中缓存：%s | 域名=%s | 邮箱=%d", task.duns, effective_domain, len(decision.emails))
            return
        if decision.status == "wait":
            self.store.defer_firecrawl_task(
                duns=task.duns,
                retries=task.retries,
                delay_seconds=max(decision.wait_seconds, self.config.queue_poll_interval),
                error_text="等待同域名查询完成",
            )
            logger.info("Firecrawl 等待同域名：%s | 域名=%s", task.duns, effective_domain)
            return
        try:
            logger.info("Firecrawl 开始：%s | 域名=%s", task.duns, effective_domain)
            result = self._get_firecrawl_service().discover_emails(
                company_name=task.company_name_en_dnb,
                homepage=task.homepage,
                domain=effective_domain,
            )
            emails = result.emails
        except FirecrawlError as exc:
            delay = self._firecrawl_delay(task.retries + 1, exc)
            self._firecrawl_domain_cache.defer(
                effective_domain,
                delay_seconds=delay,
                error_text=str(exc),
            )
            self._retry_or_fail_firecrawl(task, exc, effective_domain, delay)
            return
        except Exception as exc:  # noqa: BLE001
            delay = _backoff_seconds(attempt=task.retries + 1, base=10.0, cap=self.config.retry_backoff_cap_seconds)
            self._firecrawl_domain_cache.defer(
                effective_domain,
                delay_seconds=delay,
                error_text=str(exc),
            )
            self._retry_or_fail_firecrawl(task, exc, effective_domain, delay)
            return
        self._firecrawl_domain_cache.mark_done(
            effective_domain,
            emails,
            retry_after_seconds=result.retry_after_seconds,
        )
        self.store.mark_firecrawl_done(
            duns=task.duns,
            emails=emails,
            retry_after_seconds=result.retry_after_seconds,
        )
        logger.info("Firecrawl 完成：%s | 域名=%s | 邮箱=%d", task.duns, effective_domain, len(emails))

    def _firecrawl_delay(self, attempt: int, exc: FirecrawlError) -> float:
        if exc.code == "firecrawl_5xx":
            return 0.0
        if exc.code == "firecrawl_429" and exc.retry_after:
            return max(float(exc.retry_after), 5.0)
        return _backoff_seconds(attempt=attempt, base=10.0, cap=self.config.retry_backoff_cap_seconds)

    def _retry_or_fail_firecrawl(self, task, exc: Exception, domain: str, delay: float) -> None:
        code = exc.code if isinstance(exc, FirecrawlError) else "unknown"
        attempt = task.retries + 1
        if code in {"firecrawl_401", "firecrawl_402"} or attempt >= self.config.snov_task_max_retries:
            self.store.mark_firecrawl_failed(duns=task.duns, error_text=str(exc))
            logger.warning("Firecrawl 失败：%s | 域名=%s | 原因=%s", task.duns, domain, exc)
            return
        self.store.defer_firecrawl_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=str(exc))
        logger.warning("Firecrawl 重试：%s | 域名=%s | 第%d次 | 等待=%.1fs | 原因=%s", task.duns, domain, attempt, delay, exc)

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

    def _retry_or_fail_gmap(self, task: GMapTask, error_text: str) -> None:
        attempt = task.retries + 1
        if attempt >= self.config.gmap_max_retries:
            self.store.mark_gmap_failed(duns=task.duns, error_text=error_text)
            return
        delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
        self.store.defer_gmap_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=error_text)

    def _get_gmap_client(self) -> GoogleMapsClient:
        if not hasattr(self._gmap_local, "client"):
            self._gmap_local.client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="gb"))
        return self._gmap_local.client

    def _cursor_to_segment(self, cursor):
        from england_crawler.dnb.models import Segment

        return Segment(
            industry_path=cursor.industry_path,
            country_iso_two_code=cursor.country_iso_two_code,
            region_name=cursor.region_name,
            city_name=cursor.city_name,
            expected_count=cursor.expected_count,
            segment_type="city" if cursor.city_name else ("region" if cursor.region_name else "country"),
        )

    def _row_to_segment(self, row: dict[str, object]):
        from england_crawler.dnb.models import Segment

        return Segment(
            industry_path=str(row["industry_path"]),
            country_iso_two_code=str(row["country_iso_two_code"]),
            region_name=str(row["region_name"]),
            city_name=str(row["city_name"]),
            expected_count=int(row["expected_count"] or 0),
            segment_type="city" if str(row["city_name"]) else ("region" if str(row["region_name"]) else "country"),
        )

    def _record_from_dict(self, payload: dict[str, str]):
        from england_crawler.dnb.models import CompanyRecord

        return CompanyRecord.from_dict(payload)


def run_dnb_pipeline(
    *,
    config: DnbEnglandConfig,
    client: DnbClient,
    skip_dnb: bool = False,
    skip_gmap: bool = False,
    skip_firecrawl: bool = False,
    skip_snov: bool | None = None,
) -> None:
    if skip_snov is not None:
        skip_firecrawl = skip_snov
    runner = DnbEnglandPipelineRunner(
        config=config,
        client=client,
        skip_dnb=skip_dnb,
        skip_gmap=skip_gmap,
        skip_firecrawl=skip_firecrawl,
    )
    runner.run()







