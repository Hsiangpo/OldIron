"""韩国 DNB 流式主流程。"""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from korea_crawler.dnb.catalog import INDUSTRY_CATEGORY_COUNT
from korea_crawler.dnb.catalog import INDUSTRY_PAGE_COUNT
from korea_crawler.dnb.catalog import INDUSTRY_SUBCATEGORY_COUNT
from korea_crawler.dnb.catalog import build_country_industry_segments
from korea_crawler.dnb.client import DnbClient
from korea_crawler.dnb.client import extract_child_segments
from korea_crawler.dnb.client import parse_company_listing
from korea_crawler.dnb.client import parse_company_profile
from korea_crawler.dnb.config import DnbKoreaConfig
from korea_crawler.dnb.naming import SiteNameService
from korea_crawler.dnb.runtime.detail_queue import DetailQueueStore
from korea_crawler.dnb.runtime.detail_queue import DetailTask
from korea_crawler.dnb.store import DnbKoreaStore
from korea_crawler.dnb.store import GMapTask
from korea_crawler.dnb.store import SiteTask
from korea_crawler.dnb.store import SnovTask
from korea_crawler.google_maps import GoogleMapsClient
from korea_crawler.google_maps import GoogleMapsConfig
from korea_crawler.google_maps import GoogleMapsPlaceResult
from korea_crawler.snov.client import SnovClient
from korea_crawler.snov.client import SnovConfig
from korea_crawler.snov.client import SnovCredentialPool
from korea_crawler.snov.client import SnovNoCreditError
from korea_crawler.snov.client import SnovRateLimitError
from korea_crawler.snov.client import load_snov_credentials_from_env


logger = logging.getLogger(__name__)

LIST_PAGE_SIZE = 50
MAX_VISIBLE_PAGES = 20
MAX_LEAF_RECORDS = LIST_PAGE_SIZE * MAX_VISIBLE_PAGES
DISCOVERY_COUNTRY_CODE = "kr"
SNAPSHOT_INTERVAL_SECONDS = 30.0
DETAIL_BACKLOG_SOFT_LIMIT = 5000
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
    parts = [task.city, task.region, task.country or "Republic Of Korea"]
    names = [task.company_name_en, _strip_company_suffix(task.company_name_en)]
    queries: list[str] = []
    for name in names:
        for suffix in (" ".join(part for part in parts if part), f"{task.region} {task.country}".strip(), task.country):
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


def _build_discovery_seed_rows() -> list[dict[str, object]]:
    return [
        {
            "segment_id": segment.segment_id,
            "industry_path": segment.industry_path,
            "country_iso_two_code": segment.country_iso_two_code,
            "region_name": segment.region_name,
            "city_name": segment.city_name,
            "expected_count": segment.expected_count,
        }
        for segment in build_country_industry_segments(DISCOVERY_COUNTRY_CODE)
    ]


def _page_signature(rows: list[object]) -> tuple[str, ...]:
    signature: list[str] = []
    for row in rows[:10]:
        duns = str(getattr(row, "duns", "") or "").strip()
        if duns:
            signature.append(duns)
            continue
        company_name_url = str(getattr(row, "company_name_url", "") or "").strip()
        if company_name_url:
            signature.append(company_name_url)
    return tuple(signature)

class DnbKoreaPipelineRunner:
    """韩国 DNB 流式执行器。"""

    def __init__(
        self,
        *,
        config: DnbKoreaConfig,
        client: DnbClient,
        skip_dnb: bool,
        skip_gmap: bool,
        skip_site_name: bool,
        skip_snov: bool,
    ) -> None:
        self.config = config
        self.client = client
        self.skip_dnb = skip_dnb
        self.skip_gmap = skip_gmap
        self.skip_site_name = skip_site_name
        self.skip_snov = skip_snov
        self.store = DnbKoreaStore(config.store_db_path)
        self.detail_queue = DetailQueueStore(config.store_db_path)
        self.stop_event = threading.Event()
        self.dnb_done = threading.Event()
        self._dnb_workers_remaining = config.dnb_pipeline_workers
        self._dnb_workers_lock = threading.Lock()
        self._seed_log_lock = threading.Lock()
        self._seed_log_emitted = False
        self._dnb_client_factory = _create_dnb_client_factory(client)
        self._gmap_local = threading.local()
        self._snov_local = threading.local()
        self._site_service: SiteNameService | None = None
        self._snov_pool = SnovCredentialPool(
            load_snov_credentials_from_env(
                self.config.snov_client_id,
                self.config.snov_client_secret,
            ),
            no_credit_cooldown_seconds=3600.0,
        )

    def run(self) -> None:
        self.config.validate(skip_site_name=self.skip_site_name, skip_snov=self.skip_snov)
        workers: list[threading.Thread] = []
        if self.skip_dnb:
            self.dnb_done.set()
        else:
            self.detail_queue.sync_from_companies()
            for idx in range(self.config.dnb_pipeline_workers):
                workers.append(
                    threading.Thread(
                        target=self._dnb_worker,
                        name=f"DNB-KR-{idx+1}",
                        daemon=True,
                    )
                )
            for idx in range(self.config.dnb_workers):
                workers.append(
                    threading.Thread(
                        target=self._detail_worker,
                        name=f"DNB-Detail-KR-{idx+1}",
                        daemon=True,
                    )
                )
        if not self.skip_gmap:
            for idx in range(self.config.gmap_workers):
                workers.append(threading.Thread(target=self._gmap_worker, name=f"GMap-KR-{idx+1}", daemon=True))
        if not self.skip_site_name:
            for idx in range(self.config.site_workers):
                workers.append(threading.Thread(target=self._site_worker, name=f"SiteName-KR-{idx+1}", daemon=True))
        if not self.skip_snov:
            for idx in range(self.config.snov_workers):
                workers.append(threading.Thread(target=self._snov_worker, name=f"Snov-KR-{idx+1}", daemon=True))
        for worker in workers:
            worker.start()
        try:
            self._monitor_until_done()
        finally:
            self.stop_event.set()
            for worker in workers:
                worker.join(timeout=2)
            self.store.export_jsonl_snapshots(self.config.output_dir)
            self.detail_queue.close()
            self.store.close()

    def _monitor_until_done(self) -> None:
        last_log = 0.0
        last_snapshot = 0.0
        while not self.stop_event.is_set():
            self.store.requeue_stale_running_tasks(
                older_than_seconds=self.config.stale_running_requeue_seconds
            )
            now = time.monotonic()
            if now - last_snapshot >= SNAPSHOT_INTERVAL_SECONDS:
                self.store.export_jsonl_snapshots(self.config.output_dir)
                last_snapshot = now
            if now - last_log >= 10.0:
                stats = self.store.get_stats()
                logger.info(
                    "进度：segments=%d/%d detail=%d/%d gmap=%d/%d site=%d/%d snov=%d/%d final=%d",
                    stats["segments_done"],
                    stats["segments_total"],
                    stats["companies_detail_done"],
                    stats["companies_total"],
                    stats["gmap_running"],
                    stats["gmap_pending"],
                    stats["site_running"],
                    stats["site_pending"],
                    stats["snov_running"],
                    stats["snov_pending"],
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
        site_busy = 0 if self.skip_site_name else stats["site_pending"] + stats["site_running"]
        snov_busy = 0 if self.skip_snov else stats["snov_pending"] + stats["snov_running"]
        detail_busy = 0 if self.skip_dnb else detail_pending + detail_running
        return (detail_busy + gmap_busy + site_busy + snov_busy) == 0

    def _dnb_worker(self) -> None:
        try:
            dnb_client = self._dnb_client_factory()
            self.store.ensure_discovery_seeds(_build_discovery_seed_rows())
            if self.store.has_discovery_work():
                self._log_seed_loaded_once()
                self._discover_stable_segments(dnb_client)
            produced = 0
            while not self.stop_event.is_set():
                self._wait_for_detail_backlog()
                cursor = self.store.claim_segment(LIST_PAGE_SIZE)
                if cursor is None:
                    break
                page_number = cursor.next_page
                seen_signatures: set[tuple[str, ...]] = set()
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
                    if not rows:
                        self.store.advance_segment(
                            cursor.segment_id,
                            cursor.total_pages + 1,
                            cursor.total_pages,
                        )
                        break
                    signature = _page_signature(rows)
                    if signature and signature in seen_signatures:
                        logger.info(
                            "DNB 检测到分页回环，提前结束切片：%s 第 %d/%d 页",
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
                        seen_signatures.add(signature)
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
            logger.exception("DNB 韩国主流程异常：%s", exc)
        finally:
            with self._dnb_workers_lock:
                self._dnb_workers_remaining -= 1
                if self._dnb_workers_remaining <= 0:
                    logger.info("DNB 韩国主流程完成。")
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

    def _detail_worker(self) -> None:
        while not self.stop_event.is_set():
            self.detail_queue.sync_from_companies()
            task = self.detail_queue.claim()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_detail_task(task)

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
            related = payload.get("relatedIndustries", {}) if isinstance(payload, dict) else {}
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
            elif expected > MAX_LEAF_RECORDS and isinstance(related, dict) and related:
                for slug in related.values():
                    industry_path = str(slug or "").strip()
                    if not industry_path or industry_path == segment.industry_path:
                        continue
                    child_segment_id = "|".join(
                        [industry_path, segment.country_iso_two_code, segment.region_name, segment.city_name]
                    )
                    self.store.enqueue_discovery_node(
                        segment_id=child_segment_id,
                        industry_path=industry_path,
                        country_iso_two_code=segment.country_iso_two_code,
                        region_name=segment.region_name,
                        city_name=segment.city_name,
                        expected_count=0,
                    )
            self.store.mark_discovery_node_done(segment_id, expected_count=expected)

    def _fetch_detail_rows(self, rows: list[dict[str, str]]) -> tuple[int, bool]:
        if not rows:
            return 0, False
        success = 0

        def _worker(record: dict[str, str]) -> dict[str, str]:
            client = self._dnb_client_factory()
            logger.info(
                "DNB 详情开始：%s | %s",
                record.get("duns", ""),
                record.get("company_name_en_dnb", record.get("company_name", "")),
            )
            payload = client.fetch_company_profile(record["company_name_url"])
            company = parse_company_profile(
                record=self._record_from_dict(record),
                payload=payload,
            )
            return company.to_dict()

        with ThreadPoolExecutor(max_workers=max(self.config.dnb_workers, 1)) as executor:
            futures = {executor.submit(_worker, row): row for row in rows}
            for future in as_completed(futures):
                source = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("DNB 详情失败（%s）: %s", source.get("duns", ""), exc)
                    self.detail_queue.enqueue(str(source.get("duns", "")).strip())
                    continue
                self.store.upsert_company_detail(result)
                self.detail_queue.mark_done(str(result.get("duns", "")).strip())
                logger.info(
                    "DNB 详情完成：%s | 负责人=%s | 官网=%s",
                    result.get("duns", ""),
                    result.get("key_principal", ""),
                    result.get("dnb_website", ""),
                )
                success += 1
        return success, False

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
            delay = _backoff_seconds(
                attempt=task.retries + 1,
                base=10.0,
                cap=self.config.retry_backoff_cap_seconds,
            )
            self.detail_queue.defer(
                duns=task.duns,
                retries=task.retries + 1,
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
                if result.website and result.company_name:
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
            company_name_local_gmap=result.company_name,
            phone=result.phone,
        )
        current = self.store.get_company(task.duns) or {}
        logger.info(
            "GMap 完成：%s | 韩文名=%s | 官网=%s | 来源=%s",
            task.duns,
            str(current.get("company_name_en_gmap", "")).strip(),
            str(current.get("website", "")).strip(),
            str(current.get("website_source", "")).strip(),
        )

    def _site_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_site_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_site_task(task)

    def _process_site_task(self, task: SiteTask) -> None:
        logger.info("Site 开始：%s | 官网=%s", task.duns, task.website)
        try:
            result = self._get_site_service().extract_homepage_name(
                company_name_en_dnb=task.company_name_en_dnb,
                website=task.website,
            )
        except Exception as exc:  # noqa: BLE001
            self._retry_or_fail_site(task, str(exc))
            return
        self.store.mark_site_done(
            duns=task.duns,
            company_name_local=result.company_name_local,
            evidence_url=result.evidence_url,
            evidence_quote=result.evidence_quote,
            confidence=result.confidence,
        )
        logger.info(
            "Site 完成：%s | 韩文名=%s | 置信度=%.2f | 证据=%s",
            task.duns,
            result.company_name_local,
            result.confidence,
            result.evidence_url,
        )

    def _snov_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_snov_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_snov_task(task)

    def _process_snov_task(self, task: SnovTask) -> None:
        if not task.domain:
            self.store.mark_snov_failed(duns=task.duns, error_text="缺少域名")
            return
        logger.info("Snov 开始：%s | 域名=%s", task.duns, task.domain)
        try:
            emails = self._get_snov_client().get_domain_emails(task.domain)
        except (SnovRateLimitError, SnovNoCreditError) as exc:
            self._retry_or_fail_snov(task, str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            self._retry_or_fail_snov(task, str(exc))
            return
        self.store.mark_snov_done(duns=task.duns, emails=emails)
        logger.info("Snov 完成：%s | 域名=%s | 邮箱=%d", task.duns, task.domain, len(emails))

    def _retry_or_fail_gmap(self, task: GMapTask, error_text: str) -> None:
        attempt = task.retries + 1
        if attempt >= self.config.gmap_max_retries:
            self.store.mark_gmap_failed(duns=task.duns, error_text=error_text)
            return
        delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
        self.store.defer_gmap_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=error_text)

    def _retry_or_fail_site(self, task: SiteTask, error_text: str) -> None:
        attempt = task.retries + 1
        if attempt >= self.config.site_max_retries:
            self.store.mark_site_failed(duns=task.duns, error_text=error_text)
            return
        delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
        self.store.defer_site_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=error_text)

    def _retry_or_fail_snov(self, task: SnovTask, error_text: str) -> None:
        attempt = task.retries + 1
        if attempt >= self.config.snov_task_max_retries:
            self.store.mark_snov_failed(duns=task.duns, error_text=error_text)
            return
        delay = _backoff_seconds(attempt=attempt, base=10.0, cap=self.config.retry_backoff_cap_seconds)
        self.store.defer_snov_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=error_text)

    def _get_gmap_client(self) -> GoogleMapsClient:
        if not hasattr(self._gmap_local, "client"):
            self._gmap_local.client = GoogleMapsClient(GoogleMapsConfig(hl="ko", gl="kr"))
        return self._gmap_local.client

    def _get_snov_client(self) -> SnovClient:
        if not hasattr(self._snov_local, "client"):
            self._snov_local.client = SnovClient(
                SnovConfig(
                    client_id=self.config.snov_client_id,
                    client_secret=self.config.snov_client_secret,
                    timeout=self.config.snov_timeout_seconds,
                ),
                credential_pool=self._snov_pool,
            )
        return self._snov_local.client

    def _get_site_service(self) -> SiteNameService:
        if self._site_service is None:
            self._site_service = SiteNameService.from_env(self.config.project_root)
        return self._site_service

    def _cursor_to_segment(self, cursor: SegmentCursor):
        from korea_crawler.dnb.models import Segment

        return Segment(
            industry_path=cursor.industry_path,
            country_iso_two_code=cursor.country_iso_two_code,
            region_name=cursor.region_name,
            city_name=cursor.city_name,
            expected_count=cursor.expected_count,
            segment_type="city" if cursor.city_name else ("region" if cursor.region_name else "country"),
        )

    def _row_to_segment(self, row: dict[str, object]):
        from korea_crawler.dnb.models import Segment

        return Segment(
            industry_path=str(row["industry_path"]),
            country_iso_two_code=str(row["country_iso_two_code"]),
            region_name=str(row["region_name"]),
            city_name=str(row["city_name"]),
            expected_count=int(row["expected_count"] or 0),
            segment_type="city" if str(row["city_name"]) else ("region" if str(row["region_name"]) else "country"),
        )

    def _record_from_dict(self, payload: dict[str, str]):
        from korea_crawler.dnb.models import CompanyRecord

        return CompanyRecord.from_dict(payload)


def run_dnbkorea_pipeline(
    *,
    config: DnbKoreaConfig,
    client: DnbClient,
    skip_dnb: bool = False,
    skip_gmap: bool = False,
    skip_site_name: bool = False,
    skip_snov: bool = False,
) -> None:
    runner = DnbKoreaPipelineRunner(
        config=config,
        client=client,
        skip_dnb=skip_dnb,
        skip_gmap=skip_gmap,
        skip_site_name=skip_site_name,
        skip_snov=skip_snov,
    )
    runner.run()
