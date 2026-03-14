"""流式四段主流程。"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from thailand_crawler.client import DnbClient
from thailand_crawler.client import parse_company_listing
from thailand_crawler.client import parse_company_profile
from thailand_crawler.config import LIST_PAGE_SIZE
from thailand_crawler.config import MAX_LEAF_RECORDS
from thailand_crawler.gmap import GoogleMapsClient
from thailand_crawler.gmap import GoogleMapsPlaceResult
from thailand_crawler.gmap import build_gmap_queries
from thailand_crawler.gmap import clean_homepage
from thailand_crawler.models import CompanyRecord
from thailand_crawler.models import Segment
from thailand_crawler.snov import SnovClient
from thailand_crawler.snov import SnovConfig
from thailand_crawler.snov import SnovCredentialPool
from thailand_crawler.snov import SnovNoCreditError
from thailand_crawler.snov import SnovRateLimitError
from thailand_crawler.snov import load_snov_credentials_from_env
from thailand_crawler.streaming.industry_catalog import build_country_industry_segments
from thailand_crawler.streaming.config import StreamPipelineConfig
from thailand_crawler.streaming.firecrawl_client import FirecrawlClient
from thailand_crawler.streaming.firecrawl_client import FirecrawlKeyAuditSummary
from thailand_crawler.streaming.firecrawl_client import audit_firecrawl_keys
from thailand_crawler.streaming.firecrawl_client import FirecrawlClientConfig
from thailand_crawler.streaming.firecrawl_client import FirecrawlError
from thailand_crawler.streaming.key_pool import FirecrawlKeyPool
from thailand_crawler.streaming.key_pool import KeyPoolConfig
from thailand_crawler.streaming.llm_client import SiteNameLlmClient
from thailand_crawler.streaming.site_name_service import SiteNameService
from thailand_crawler.streaming.store import SiteTask
from thailand_crawler.streaming.store import SnovTask
from thailand_crawler.streaming.store import StreamStore
from thailand_crawler.streaming.store import WebsiteTask


logger = logging.getLogger(__name__)


class SiteServiceHealthGate:
    """site 基础服务健康门。"""

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        base_backoff_seconds: float = 15.0,
        cap_backoff_seconds: float = 120.0,
    ) -> None:
        self._failure_threshold = max(int(failure_threshold), 1)
        self._base_backoff_seconds = max(float(base_backoff_seconds), 1.0)
        self._cap_backoff_seconds = max(float(cap_backoff_seconds), self._base_backoff_seconds)
        self._failure_streak = 0
        self._pause_until = 0.0
        self._probe_in_flight = False
        self._lock = threading.Lock()

    def record_success(self) -> None:
        with self._lock:
            self._failure_streak = 0
            self._pause_until = 0.0
            self._probe_in_flight = False

    def record_failure(self, *, now: float | None = None) -> float:
        current = float(now if now is not None else time.monotonic())
        with self._lock:
            self._probe_in_flight = False
            self._failure_streak += 1
            if self._failure_streak < self._failure_threshold:
                return 0.0
            attempt = self._failure_streak - self._failure_threshold + 1
            delay = _backoff_seconds(
                attempt=attempt,
                base=self._base_backoff_seconds,
                cap=self._cap_backoff_seconds,
            )
            self._pause_until = max(self._pause_until, current + delay)
            return delay

    def wait_seconds(self, *, now: float | None = None) -> float:
        current = float(now if now is not None else time.monotonic())
        with self._lock:
            return max(self._pause_until - current, 0.0)

    def acquire_attempt(self, *, now: float | None = None) -> bool:
        current = float(now if now is not None else time.monotonic())
        with self._lock:
            if self._pause_until > current:
                return False
            if self._failure_streak < self._failure_threshold:
                return True
            if self._probe_in_flight:
                return False
            self._probe_in_flight = True
            return True

    def cancel_attempt(self) -> None:
        with self._lock:
            self._probe_in_flight = False


def _merge_gmap_result(current: GoogleMapsPlaceResult, incoming: GoogleMapsPlaceResult) -> GoogleMapsPlaceResult:
    merged = GoogleMapsPlaceResult(
        company_name_en=current.company_name_en or incoming.company_name_en,
        company_name_th=current.company_name_th or incoming.company_name_th,
        phone=current.phone or incoming.phone,
        website=current.website or incoming.website,
        score=max(current.score, incoming.score),
    )
    return merged

def _parse_count(value: object) -> int:
    text = str(value or '').replace(',', '').strip()
    return int(text) if text.isdigit() else 0


def _page_signature(rows: list[CompanyRecord]) -> tuple[str, ...]:
    signature: list[str] = []
    for row in rows:
        marker = row.duns.strip() or row.company_name_url.strip() or row.company_name.strip()
        if marker:
            signature.append(marker)
    return tuple(signature)


def _segment_from_href(industry_path: str, href: str, expected_count: int) -> Segment:
    tokens = [token for token in str(href).split('.') if token]
    country = tokens[0] if len(tokens) > 0 else ''
    region = tokens[1] if len(tokens) > 1 else ''
    city = tokens[2] if len(tokens) > 2 else ''
    segment_type = 'city' if city else ('region' if region else 'country')
    return Segment(
        industry_path=industry_path,
        country_iso_two_code=country,
        region_name=region,
        city_name=city,
        expected_count=expected_count,
        segment_type=segment_type,
    )


def _discover_stable_segments(
    *,
    store: StreamStore,
    client: DnbClient,
    stop_event: threading.Event,
    max_leaf_records: int = MAX_LEAF_RECORDS,
    max_nodes: int = 0,
) -> int:
    processed = 0
    while not stop_event.is_set():
        node = store.claim_discovery_node()
        if node is None:
            break
        processed += 1
        logger.info('探索切片：%s', node.segment_id)
        payload = client.fetch_company_listing_page(node, page_number=1)
        count = int(payload.get('candidatesMatchedQuantityInt', 0) or 0)
        geos = payload.get('companyInformationGeos', [])
        related = payload.get('relatedIndustries', {})
        if count > 0:
            leaf = Segment.from_dict(node.to_dict())
            leaf.expected_count = count
            store.upsert_leaf_segment(leaf)
        if geos:
            for geo in geos:
                href = str(geo.get('href', '')).strip()
                if not href:
                    continue
                child = _segment_from_href(node.industry_path, href, _parse_count(geo.get('quantity', 0)))
                store.enqueue_discovery_node(child)
        if count > max_leaf_records and isinstance(related, dict) and related:
            for slug in related.values():
                industry_path = str(slug or '').strip()
                if not industry_path or industry_path == node.industry_path:
                    continue
                child = Segment(
                    industry_path=industry_path,
                    country_iso_two_code=node.country_iso_two_code,
                    region_name=node.region_name,
                    city_name=node.city_name,
                    expected_count=0,
                    segment_type='industry',
                )
                store.enqueue_discovery_node(child)
        store.mark_discovery_node_done(node.segment_id, expected_count=count)

        if max_nodes > 0 and processed >= max_nodes:
            break
    return processed

def _backoff_seconds(*, attempt: int, base: float = 5.0, cap: float = 180.0) -> float:
    return min(max(base, 1.0) * (2 ** max(attempt - 1, 0)), max(cap, 1.0))


def _build_root_segments() -> list[Segment]:
    return build_country_industry_segments('th')


def _create_dnb_client_factory(base_client: DnbClient):
    thread_local = threading.local()

    def _get_client() -> DnbClient:
        if not hasattr(thread_local, 'client'):
            thread_local.client = DnbClient(rate_config=base_client.rate_config, cookie_header=base_client.cookie_header)
        return thread_local.client

    return _get_client


def _create_gmap_factory():
    thread_local = threading.local()

    def _get_client() -> GoogleMapsClient:
        if not hasattr(thread_local, 'client'):
            thread_local.client = GoogleMapsClient()
        return thread_local.client

    return _get_client


def _create_snov_factory(config: StreamPipelineConfig):
    thread_local = threading.local()
    credential_pool = SnovCredentialPool(
        load_snov_credentials_from_env(
            config.snov_client_id,
            config.snov_client_secret,
        ),
        no_credit_cooldown_seconds=3600.0,
    )

    def _get_client() -> SnovClient:
        if not hasattr(thread_local, 'client'):
            thread_local.client = SnovClient(
                SnovConfig(
                    client_id=config.snov_client_id,
                    client_secret=config.snov_client_secret,
                    timeout=config.snov_timeout_seconds,
                    retry_delay=config.snov_retry_delay_seconds,
                    max_retries=config.snov_max_retries,
                ),
                credential_pool=credential_pool,
            )
        return thread_local.client

    return _get_client


def _create_site_service_factory(config: StreamPipelineConfig):
    SiteNameService.ensure_keys_file(config.firecrawl_keys_file, config.firecrawl_keys_inline)
    audit = audit_firecrawl_keys(
        key_file=config.firecrawl_keys_file,
        config=FirecrawlClientConfig(
            base_url=config.firecrawl_base_url,
            timeout_seconds=config.firecrawl_timeout_seconds,
            max_retries=config.firecrawl_max_retries,
        ),
    )
    logger.info(
        'Firecrawl key 预检：总数=%d 可用=%d 移除401=%d 移除无额度=%d 保留限流=%d 保留未知=%d',
        audit.total,
        audit.usable,
        audit.removed_unauthorized,
        audit.removed_no_credit,
        audit.kept_rate_limited,
        audit.kept_unknown,
    )
    keys = FirecrawlKeyPool.load_keys(config.firecrawl_keys_file)
    key_pool = FirecrawlKeyPool(
        keys=keys,
        key_file=config.firecrawl_keys_file,
        db_path=config.firecrawl_pool_db,
        config=KeyPoolConfig(
            per_key_limit=config.firecrawl_key_per_limit,
            wait_seconds=config.firecrawl_key_wait_seconds,
            cooldown_seconds=config.firecrawl_key_cooldown_seconds,
            failure_threshold=config.firecrawl_key_failure_threshold,
        ),
    )
    thread_local = threading.local()

    def _get_service() -> SiteNameService:
        if not hasattr(thread_local, 'service'):
            firecrawl = FirecrawlClient(
                key_pool=key_pool,
                config=FirecrawlClientConfig(
                    base_url=config.firecrawl_base_url,
                    timeout_seconds=config.firecrawl_timeout_seconds,
                    max_retries=config.firecrawl_max_retries,
                ),
            )
            llm = SiteNameLlmClient(
                api_key=config.llm_api_key,
                base_url=config.llm_base_url,
                model=config.llm_model,
                reasoning_effort=config.llm_reasoning_effort,
                timeout_seconds=config.llm_timeout_seconds,
            )
            thread_local.service = SiteNameService(firecrawl=firecrawl, llm=llm)
        return thread_local.service

    return _get_service


class StreamPipelineRunner:
    """Thailand 流式主流程执行器。"""

    def __init__(
        self,
        *,
        project_root: Path,
        output_dir: Path,
        client: DnbClient,
        max_companies: int,
        dnb_workers: int,
        website_workers: int,
        site_workers: int,
        snov_workers: int,
        skip_dnb: bool,
        skip_website: bool,
        skip_site: bool,
        skip_snov: bool,
    ) -> None:
        self.project_root = project_root
        self.output_dir = output_dir
        self.client = client
        self.skip_dnb = skip_dnb
        self.skip_website = skip_website
        self.skip_site = skip_site
        self.skip_snov = skip_snov
        self.config = StreamPipelineConfig.from_env(
            project_root=project_root,
            output_dir=output_dir,
            max_companies=max_companies,
            dnb_workers=dnb_workers,
            website_workers=website_workers,
            site_workers=site_workers,
            snov_workers=snov_workers,
        )
        self.config.validate(skip_site=skip_site, skip_snov=skip_snov)
        self.store = StreamStore(self.config.store_db_path)
        self.stop_event = threading.Event()
        self.dnb_done = threading.Event()
        self._gmap_factory = _create_gmap_factory()
        self._snov_factory = _create_snov_factory(self.config)
        self._site_service_factory = None if skip_site else _create_site_service_factory(self.config)
        self._site_health_gate = SiteServiceHealthGate()

    def run(self) -> None:
        threads: list[threading.Thread] = []
        if self.skip_dnb:
            self.dnb_done.set()
        else:
            threads.append(threading.Thread(target=self._dnb_worker, name='DNB-Stream', daemon=True))
        if not self.skip_website:
            for index in range(self.config.website_workers):
                threads.append(threading.Thread(target=self._website_worker, name=f'Website-{index + 1}', daemon=True))
        if not self.skip_site:
            for index in range(self.config.site_workers):
                threads.append(threading.Thread(target=self._site_worker, name=f'Site-{index + 1}', daemon=True))
        if not self.skip_snov:
            for index in range(self.config.snov_workers):
                threads.append(threading.Thread(target=self._snov_worker, name=f'Snov-{index + 1}', daemon=True))
        for thread in threads:
            thread.start()
        self._monitor_until_done()
        self.stop_event.set()
        for thread in threads:
            thread.join(timeout=2)
        logger.info('流式主流程完成：成品=%d', self.store.get_stats()['final_total'])

    def _monitor_until_done(self) -> None:
        last_log = 0.0
        while True:
            recovered = self.store.requeue_stale_running_tasks(older_than_seconds=self.config.stale_running_requeue_seconds)
            if recovered > 0:
                logger.info('已回收超时 running 任务：%d', recovered)
            now = time.monotonic()
            if now - last_log >= 10.0:
                stats = self.store.get_stats()
                logger.info(
                    '进度：segments=%d/%d companies=%d detail=%d website=%d/%d site=%d/%d snov=%d/%d final=%d',
                    stats['segments_done'],
                    stats['segments_total'],
                    stats['companies_total'],
                    stats['companies_detail_done'],
                    stats['website_pending'],
                    stats['website_running'],
                    stats['site_pending'],
                    stats['site_running'],
                    stats['snov_pending'],
                    stats['snov_running'],
                    stats['final_total'],
                )
                last_log = now
            if self.dnb_done.is_set() and self._queues_idle():
                return
            time.sleep(self.config.queue_poll_interval)

    def _queues_idle(self) -> bool:
        stats = self.store.get_stats()
        website_busy = 0 if self.skip_website else stats['website_pending'] + stats['website_running']
        site_busy = 0 if self.skip_site else stats['site_pending'] + stats['site_running']
        snov_busy = 0 if self.skip_snov else stats['snov_pending'] + stats['snov_running']
        return (website_busy + site_busy + snov_busy) == 0
    def _dnb_worker(self) -> None:
        try:
            seeds = _build_root_segments()
            inserted = self.store.ensure_discovery_seeds(seeds)
            logger.info('D&B 全站行业 seed：总数=%d 新增=%d', len(seeds), inserted)
            if self.store.has_discovery_work():
                logger.info('D&B 切片发现开始：国家=th 行业页=%d', len(seeds))
                _discover_stable_segments(store=self.store, client=self.client, stop_event=self.stop_event)
                if self.stop_event.is_set() or not self.store.discovery_done():
                    logger.info('D&B 切片发现未完成，等待下次续跑。')
                    return
                logger.info('D&B 切片发现完成：共 %d 个稳定切片', self.store.segment_count())
            get_client = _create_dnb_client_factory(self.client)
            produced = 0
            while not self.stop_event.is_set():
                cursor = self.store.next_segment(LIST_PAGE_SIZE)
                if cursor is None:
                    break
                segment_id = cursor.segment.segment_id
                page_number = cursor.next_page
                seen_signatures: set[tuple[str, ...]] = set()
                while page_number <= cursor.total_pages and not self.stop_event.is_set():
                    logger.info('D&B 抓取切片：%s 第 %d/%d 页', segment_id, page_number, cursor.total_pages)
                    payload = self.client.fetch_company_listing_page(cursor.segment, page_number=page_number)
                    rows = parse_company_listing(payload)
                    signature = _page_signature(rows)
                    if not rows:
                        self.store.advance_segment(segment_id, cursor.total_pages + 1, cursor.total_pages)
                        logger.info('D&B 切片空页停止：%s 第 %d 页', segment_id, page_number)
                        break
                    if signature in seen_signatures:
                        self.store.advance_segment(segment_id, cursor.total_pages + 1, cursor.total_pages)
                        logger.info('D&B 切片检测到回环，停止后续分页：%s 第 %d 页', segment_id, page_number)
                        break
                    seen_signatures.add(signature)
                    detail_targets: list[CompanyRecord] = []
                    for row in rows:
                        if not row.duns:
                            continue
                        self.store.upsert_company_listing(row)
                        if not self.store.is_company_detail_done(row.duns):
                            detail_targets.append(row)
                    produced += self._fetch_detail_rows(detail_targets, get_client)
                    self.store.advance_segment(segment_id, page_number + 1, cursor.total_pages)
                    if self.config.max_companies > 0 and produced >= self.config.max_companies:
                        logger.info('D&B 达到 max_companies=%d，停止继续扩展。', self.config.max_companies)
                        return
                    page_number += 1
            logger.info('D&B 主流程完成。')
        except Exception as exc:  # noqa: BLE001
            logger.exception('D&B 主流程异常：%s', exc)
        finally:
            self.dnb_done.set()

    def _fetch_detail_rows(self, rows: list[CompanyRecord], get_client) -> int:
        if not rows:
            return 0
        success = 0

        def _worker(record: CompanyRecord) -> CompanyRecord:
            payload = get_client().fetch_company_profile(record.company_name_url)
            return parse_company_profile(record, payload)

        with ThreadPoolExecutor(max_workers=max(self.config.dnb_workers, 1)) as executor:
            futures = {executor.submit(_worker, row): row for row in rows}
            for future in as_completed(futures):
                source = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning('D&B 详情失败（%s）: %s', source.duns or source.company_name_url, exc)
                    continue
                self.store.upsert_company_detail(result)
                success += 1
        if success > 0:
            logger.info('D&B 详情新增：%d', success)
        return success

    def _website_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_website_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_website_task(task)

    def _process_website_task(self, task: WebsiteTask) -> None:
        try:
            probe = CompanyRecord(company_name=task.company_name_en, city=task.city, region=task.region, country=task.country)
            gmap_result = GoogleMapsPlaceResult()
            for query in build_gmap_queries(probe):
                candidate = self._gmap_factory().search_company_profile(query, company_name=task.company_name_en)
                gmap_result = _merge_gmap_result(gmap_result, candidate)
                if gmap_result.company_name_th and gmap_result.phone and gmap_result.website:
                    break
        except Exception as exc:  # noqa: BLE001
            attempt = task.retries + 1
            if attempt >= self.config.website_max_retries:
                self.store.mark_website_failed(duns=task.duns, error_text=str(exc))
                logger.warning('GMAP 补全失败终止：%s %s', task.duns, exc)
                return
            delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
            self.store.defer_website_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=str(exc))
            logger.warning('GMAP 补全重试：%s %.1fs 后重试，原因=%s', task.duns, delay, exc)
            return

        dnb_website = clean_homepage(task.dnb_website)
        final_website = gmap_result.website or dnb_website
        final_source = 'gmap' if gmap_result.website else ('dnb' if dnb_website else '')
        self.store.mark_website_done(
            duns=task.duns,
            website=final_website,
            source=final_source,
            company_name_th=gmap_result.company_name_th,
            phone=gmap_result.phone,
        )
        if gmap_result.company_name_th or gmap_result.phone or final_website:
            logger.info(
                'GMAP 补全完成：%s 泰文=%s 电话=%s 官网=%s 来源=%s',
                task.duns,
                gmap_result.company_name_th or '-',
                gmap_result.phone or '-',
                final_website or '-',
                final_source or '-',
            )
            return
        logger.info('GMAP 无结果：%s', task.duns)
    def _site_worker(self) -> None:
        while not self.stop_event.is_set():
            wait_seconds = self._site_health_gate.wait_seconds()
            if wait_seconds > 0:
                time.sleep(min(wait_seconds, max(self.config.queue_poll_interval, 1.0)))
                continue
            if not self._site_health_gate.acquire_attempt():
                time.sleep(self.config.queue_poll_interval)
                continue
            task = self.store.claim_site_task()
            if task is None:
                self._site_health_gate.cancel_attempt()
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_site_task(task)

    def _process_site_task(self, task: SiteTask) -> None:
        try:
            result = self._site_service_factory().extract_homepage_name(
                company_name_en=task.company_name_en,
                website=task.website,
            )
        except FirecrawlError as exc:
            self._handle_site_firecrawl_error(task, exc)
            return
        except Exception as exc:  # noqa: BLE001
            attempt = task.retries + 1
            if attempt >= self.config.site_max_retries:
                self.store.mark_site_failed(duns=task.duns, error_text=str(exc))
                logger.warning('site 阶段失败终止：%s %s', task.duns, exc)
                return
            delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
            self.store.defer_site_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=str(exc))
            logger.warning('site 阶段重试：%s %.1fs 后重试，原因=%s', task.duns, delay, exc)
            return
        self._site_health_gate.record_success()
        self.store.mark_site_done(
            duns=task.duns,
            company_name_th=result.company_name_th,
            evidence_url=result.evidence_url,
            evidence_quote=result.evidence_quote,
            confidence=result.confidence,
        )
        resolved = self.store.get_company(task.duns) or {}
        logger.info('site 阶段完成：%s 公司名=%s', task.duns, str(resolved.get('company_name_resolved', '')))

    def _handle_site_firecrawl_error(self, task: SiteTask, exc: FirecrawlError) -> None:
        retryable = {'firecrawl_key_unavailable', 'firecrawl_429', 'firecrawl_5xx', 'firecrawl_request_failed'}
        outage_codes = {'firecrawl_key_unavailable', 'firecrawl_5xx', 'firecrawl_request_failed'}
        if exc.code not in retryable:
            self.store.mark_site_failed(duns=task.duns, error_text=exc.code)
            logger.warning('site 阶段失败终止：%s %s', task.duns, exc.code)
            return
        attempt = task.retries + 1
        if attempt >= self.config.site_max_retries:
            self.store.mark_site_failed(duns=task.duns, error_text=exc.code)
            logger.warning('site 阶段达到最大重试：%s %s', task.duns, exc.code)
            return
        delay = float(exc.retry_after or 0) if exc.code == 'firecrawl_429' else 0.0
        if delay <= 0:
            delay = _backoff_seconds(attempt=attempt, cap=self.config.retry_backoff_cap_seconds)
        service_pause = 0.0
        if exc.code in outage_codes:
            service_pause = self._site_health_gate.record_failure()
        self.store.defer_site_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=exc.code)
        if service_pause > 0:
            logger.warning('site 服务退避：%.1fs，原因=%s', service_pause, exc.code)
        logger.warning('site 阶段基础服务重试：%s %.1fs 后重试，原因=%s', task.duns, delay, exc.code)

    def _snov_worker(self) -> None:
        while not self.stop_event.is_set():
            task = self.store.claim_snov_task()
            if task is None:
                time.sleep(self.config.queue_poll_interval)
                continue
            self._process_snov_task(task)

    def _process_snov_task(self, task: SnovTask) -> None:
        if not task.domain:
            self.store.mark_snov_failed(duns=task.duns, error_text='缺少域名')
            return
        try:
            emails = self._snov_factory().get_domain_emails(task.domain)
        except (SnovRateLimitError, SnovNoCreditError) as exc:
            self._retry_snov_task(task, str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            self._retry_snov_task(task, str(exc))
            return
        self.store.mark_snov_done(duns=task.duns, emails=emails)
        logger.info('Snov 阶段完成：%s 新增邮箱=%d', task.duns, len(emails))

    def _retry_snov_task(self, task: SnovTask, error_text: str) -> None:
        attempt = task.retries + 1
        if attempt >= self.config.snov_task_max_retries:
            self.store.mark_snov_failed(duns=task.duns, error_text=error_text)
            logger.warning('Snov 阶段失败终止：%s %s', task.duns, error_text)
            return
        delay = _backoff_seconds(attempt=attempt, base=10.0, cap=self.config.retry_backoff_cap_seconds)
        self.store.defer_snov_task(duns=task.duns, retries=attempt, delay_seconds=delay, error_text=error_text)
        logger.warning('Snov 阶段重试：%s %.1fs 后重试，原因=%s', task.duns, delay, error_text)


def run_stream_pipeline(
    *,
    project_root: Path,
    output_dir: Path,
    client: DnbClient,
    max_companies: int = 0,
    dnb_workers: int = 4,
    website_workers: int = 4,
    site_workers: int = 2,
    snov_workers: int = 4,
    skip_dnb: bool = False,
    skip_website: bool = False,
    skip_site: bool = False,
    skip_snov: bool = False,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    runner = StreamPipelineRunner(
        project_root=project_root,
        output_dir=output_dir,
        client=client,
        max_companies=max_companies,
        dnb_workers=dnb_workers,
        website_workers=website_workers,
        site_workers=site_workers,
        snov_workers=snov_workers,
        skip_dnb=skip_dnb,
        skip_website=skip_website,
        skip_site=skip_site,
        skip_snov=skip_snov,
    )
    runner.run()
