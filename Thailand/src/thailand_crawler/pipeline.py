"""主流水线。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import json
import logging
import math
import os
import threading
import time
from pathlib import Path

from thailand_crawler.client import DnbClient
from thailand_crawler.client import parse_company_listing
from thailand_crawler.client import parse_company_profile
from thailand_crawler.config import DEFAULT_DETAIL_CONCURRENCY
from thailand_crawler.config import DEFAULT_ENRICH_POLL_INTERVAL
from thailand_crawler.config import DEFAULT_GMAP_BATCH_SIZE
from thailand_crawler.config import DEFAULT_GMAP_CONCURRENCY
from thailand_crawler.config import LIST_PAGE_SIZE
from thailand_crawler.config import MAX_LEAF_RECORDS
from thailand_crawler.config import DEFAULT_SNOV_BATCH_SIZE
from thailand_crawler.config import DEFAULT_SNOV_CONCURRENCY
from thailand_crawler.gmap import GoogleMapsClient
from thailand_crawler.gmap import build_gmap_query
from thailand_crawler.gmap import clean_homepage
from thailand_crawler.models import CompanyRecord
from thailand_crawler.models import Segment
from thailand_crawler.snov import SnovClient
from thailand_crawler.snov import SnovConfig
from thailand_crawler.snov import SnovCredentialPool
from thailand_crawler.snov import SnovNoCreditError
from thailand_crawler.snov import SnovRateLimitError
from thailand_crawler.snov import extract_domain
from thailand_crawler.snov import load_snov_credentials_from_env
from thailand_crawler.snov import merge_emails


logger = logging.getLogger(__name__)

_FILE_LOCKS: dict[str, threading.RLock] = {}
_FILE_LOCKS_GUARD = threading.Lock()
_ATOMIC_WRITE_RETRY_DELAYS = (0.2, 0.5, 1.0, 2.0, 3.0)


def _get_path_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _FILE_LOCKS_GUARD:
        lock = _FILE_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _FILE_LOCKS[key] = lock
    return lock


def _parse_count(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    return int(text) if text.isdigit() else 0


def _segment_from_href(industry_path: str, href: str, expected_count: int) -> Segment:
    tokens = [token for token in str(href).split(".") if token]
    country = tokens[0] if len(tokens) > 0 else ""
    region = tokens[1] if len(tokens) > 1 else ""
    city = tokens[2] if len(tokens) > 2 else ""
    segment_type = "city" if city else ("region" if region else "country")
    return Segment(
        industry_path=industry_path,
        country_iso_two_code=country,
        region_name=region,
        city_name=city,
        expected_count=expected_count,
        segment_type=segment_type,
    )


def discover_segments(
    client: DnbClient,
    root_segment: Segment,
    max_leaf_records: int = MAX_LEAF_RECORDS,
    max_segments: int = 0,
) -> list[Segment]:
    logger.info("探索切片：%s", root_segment.segment_id)
    payload = client.fetch_company_listing_page(root_segment, page_number=1)
    count = int(payload.get("candidatesMatchedQuantityInt", 0) or 0)
    geos = payload.get("companyInformationGeos", [])
    related = payload.get("relatedIndustries", {})

    if geos:
        segments: list[Segment] = []
        for geo in geos:
            if max_segments > 0 and len(segments) >= max_segments:
                break
            href = str(geo.get("href", "")).strip()
            if not href:
                continue
            child = _segment_from_href(root_segment.industry_path, href, _parse_count(geo.get("quantity", 0)))
            remaining = max(max_segments - len(segments), 0) if max_segments > 0 else 0
            segments.extend(
                discover_segments(
                    client,
                    child,
                    max_leaf_records=max_leaf_records,
                    max_segments=remaining,
                )
            )
        return segments

    if count > max_leaf_records and isinstance(related, dict) and related:
        segments: list[Segment] = []
        for slug in related.values():
            if max_segments > 0 and len(segments) >= max_segments:
                break
            industry_path = str(slug or "").strip()
            if not industry_path or industry_path == root_segment.industry_path:
                continue
            child = Segment(
                industry_path=industry_path,
                country_iso_two_code=root_segment.country_iso_two_code,
                region_name=root_segment.region_name,
                city_name=root_segment.city_name,
                expected_count=0,
                segment_type="industry",
            )
            remaining = max(max_segments - len(segments), 0) if max_segments > 0 else 0
            segments.extend(
                discover_segments(
                    client,
                    child,
                    max_leaf_records=max_leaf_records,
                    max_segments=remaining,
                )
            )
        if segments:
            return segments

    leaf = Segment.from_dict(root_segment.to_dict())
    leaf.expected_count = count
    return [leaf]


def _load_jsonl_records(path: Path, *, model: type[CompanyRecord] | None = None) -> list:
    with _get_path_lock(path):
        if not path.exists():
            return []
        rows: list = []
        with path.open("r", encoding="utf-8") as fp:
            for line in fp:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if model is CompanyRecord:
                    rows.append(CompanyRecord.from_dict(payload))
                else:
                    rows.append(payload)
        return rows


def _count_jsonl_records(path: Path) -> int:
    with _get_path_lock(path):
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as fp:
            return sum(1 for line in fp if line.strip())


def _load_list_checkpoint(path: Path) -> set[str]:
    with _get_path_lock(path):
        if not path.exists():
            return set()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return set()
    return {str(item).strip() for item in payload.get("completed_segments", []) if str(item).strip()}


def _save_list_checkpoint(path: Path, completed_segments: set[str]) -> None:
    with _get_path_lock(path):
        path.write_text(
            json.dumps({"completed_segments": sorted(completed_segments)}, ensure_ascii=False),
            encoding="utf-8",
        )


def _load_processed_duns(path: Path) -> set[str]:
    with _get_path_lock(path):
        if not path.exists():
            return set()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return set()
    return {str(item).strip() for item in payload.get("processed_duns", []) if str(item).strip()}


def _save_processed_duns(path: Path, processed_duns: set[str]) -> None:
    with _get_path_lock(path):
        path.write_text(
            json.dumps({"processed_duns": sorted(processed_duns)}, ensure_ascii=False),
            encoding="utf-8",
        )


def _load_record_map(path: Path) -> dict[str, CompanyRecord]:
    return {record.duns: record for record in _load_jsonl_records(path, model=CompanyRecord)}


def _merge_records(current: CompanyRecord, previous: CompanyRecord | None) -> CompanyRecord:
    if previous is None:
        return current
    merged = CompanyRecord.from_dict(current.to_dict())
    for field in (
        "company_name",
        "company_name_url",
        "address",
        "region",
        "city",
        "country",
        "postal_code",
        "sales_revenue",
        "website",
        "domain",
        "key_principal",
        "phone",
        "trade_style_name",
        "formatted_revenue",
    ):
        if not getattr(merged, field) and getattr(previous, field):
            setattr(merged, field, getattr(previous, field))
    if not merged.emails and previous.emails:
        merged.emails = list(previous.emails)
    return merged


def _merge_record_maps(source_map: dict[str, CompanyRecord], old_map: dict[str, CompanyRecord]) -> dict[str, CompanyRecord]:
    merged: dict[str, CompanyRecord] = {}
    for duns, record in source_map.items():
        merged[duns] = _merge_records(record, old_map.get(duns))
    for duns, record in old_map.items():
        if duns not in merged:
            merged[duns] = record
    return merged


def _should_route_downstream(record: CompanyRecord) -> bool:
    return bool(record.company_name and record.key_principal)


def _clone_dnb_client(client: DnbClient) -> DnbClient:
    return DnbClient(rate_config=client.rate_config, cookie_header=client.cookie_header)


def _create_thread_local_client_factory(base_client: DnbClient):
    if not hasattr(base_client, "rate_config") or not hasattr(base_client, "cookie_header"):
        return lambda: base_client
    thread_local = threading.local()

    def _get_client() -> DnbClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = _clone_dnb_client(base_client)
        return thread_local.client

    return _get_client


def _create_thread_local_gmap_factory():
    thread_local = threading.local()

    def _get_client() -> GoogleMapsClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = GoogleMapsClient()
        return thread_local.client

    return _get_client


def _create_thread_local_snov_factory(client_id: str, client_secret: str):
    thread_local = threading.local()
    credential_pool = SnovCredentialPool(
        load_snov_credentials_from_env(client_id, client_secret),
        no_credit_cooldown_seconds=3600.0,
    )

    def _get_client() -> SnovClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = SnovClient(
                SnovConfig(client_id=client_id, client_secret=client_secret),
                credential_pool=credential_pool,
            )
        return thread_local.client

    return _get_client


def _resolve_batch_limit(max_items: int, default_batch_size: int) -> int:
    if max_items > 0:
        return max_items
    return default_batch_size


def is_segment_discovery_complete(output_dir: Path) -> bool:
    return _count_jsonl_records(output_dir / "segments.jsonl") > 0


def is_company_discovery_complete(output_dir: Path) -> bool:
    segments = _load_segments(output_dir)
    if not segments:
        return False
    detail_count = _count_jsonl_records(output_dir / "companies.jsonl")
    if detail_count > 0:
        return True
    expected_total = sum(max(segment.expected_count, 0) for segment in segments)
    current_total = _count_jsonl_records(output_dir / "company_ids.jsonl")
    checkpoint_file = output_dir / "checkpoint_list.json"
    completed_segments = _load_list_checkpoint(checkpoint_file)
    if completed_segments and len(completed_segments) >= len(segments):
        return True
    return current_total >= expected_total > 0


def mark_company_discovery_complete(output_dir: Path) -> None:
    segments = _load_segments(output_dir)
    if not segments:
        return
    checkpoint_file = output_dir / "checkpoint_list.json"
    _save_list_checkpoint(checkpoint_file, {segment.segment_id for segment in segments})


def _atomic_write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _get_path_lock(path):
        with tmp_path.open("w", encoding="utf-8") as fp:
            for row in rows:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
        last_error: PermissionError | None = None
        for delay in (0.0, *_ATOMIC_WRITE_RETRY_DELAYS):
            if delay > 0:
                time.sleep(delay)
            try:
                tmp_path.replace(path)
                return
            except PermissionError as exc:
                last_error = exc
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            logger.warning("临时文件清理失败：%s", tmp_path)
        if last_error is not None:
            raise last_error


def run_segment_discovery(output_dir: Path, client: DnbClient, max_segments: int = 0) -> int:
    root = Segment(industry_path="construction", country_iso_two_code="th", expected_count=117112, segment_type="country")
    logger.info("切片发现开始：行业=%s 国家=%s", root.industry_path, root.country_iso_two_code)
    segments = discover_segments(
        client,
        root,
        max_leaf_records=MAX_LEAF_RECORDS,
        max_segments=max_segments,
    )
    segments_path = output_dir / "segments.jsonl"
    _atomic_write_jsonl(segments_path, [segment.to_dict() for segment in segments])
    logger.info("切片发现完成：共 %d 个稳定切片，输出=%s", len(segments), segments_path)
    return len(segments)


def _load_segments(output_dir: Path) -> list[Segment]:
    path = output_dir / "segments.jsonl"
    return [Segment.from_dict(row) for row in _load_jsonl_records(path)]


def run_company_discovery(
    output_dir: Path,
    client: DnbClient,
    max_segments: int = 0,
    max_pages_per_segment: int = 0,
) -> int:
    segments = _load_segments(output_dir)
    if max_segments > 0:
        segments = segments[:max_segments]
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / "checkpoint_list.json"
    existing = {record.duns: record for record in _load_jsonl_records(output_file, model=CompanyRecord)}
    completed_segments = _load_list_checkpoint(checkpoint_file)
    pending_segments = [segment for segment in segments if segment.segment_id not in completed_segments]
    discovered = 0
    logger.info("主体抓取开始：切片=%d，待处理切片=%d，已有公司=%d", len(segments), len(pending_segments), len(existing))
    for segment in pending_segments:
        expected_pages = max(1, math.ceil(segment.expected_count / LIST_PAGE_SIZE))
        if max_pages_per_segment > 0:
            expected_pages = min(expected_pages, max_pages_per_segment)
        logger.info(
            "开始抓取切片：%s，预计页数=%d，预计记录=%d",
            segment.segment_id,
            expected_pages,
            segment.expected_count,
        )
        seen_first_duns: set[str] = set()
        for page_number in range(1, expected_pages + 1):
            payload = client.fetch_company_listing_page(segment, page_number=page_number)
            rows = parse_company_listing(payload)
            if not rows:
                logger.info("切片无数据，停止：%s 第 %d 页", segment.segment_id, page_number)
                break
            first_duns = rows[0].duns
            if first_duns and first_duns in seen_first_duns:
                logger.warning("检测到分页回卷，提前停止切片: %s 第 %d 页", segment.segment_id, page_number)
                break
            if first_duns:
                seen_first_duns.add(first_duns)
            for row in rows:
                if row.duns and row.duns not in existing:
                    existing[row.duns] = row
                    discovered += 1
            logger.info(
                "切片进度：%s 第 %d/%d 页，本页=%d，累计新增=%d",
                segment.segment_id,
                page_number,
                expected_pages,
                len(rows),
                discovered,
            )
            if len(rows) < LIST_PAGE_SIZE:
                break
        completed_segments.add(segment.segment_id)
        _save_list_checkpoint(checkpoint_file, completed_segments)
    _atomic_write_jsonl(output_file, [record.to_dict() for record in existing.values()])
    logger.info("主体抓取完成：累计新增=%d，输出=%s", discovered, output_file)
    return discovered


def run_company_details(
    output_dir: Path,
    client: DnbClient,
    max_items: int = 0,
    detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY,
) -> int:
    input_file = output_dir / "company_ids.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint_detail.json"
    if not input_file.exists():
        return 0
    source_rows = _load_jsonl_records(input_file, model=CompanyRecord)
    existing = _load_record_map(output_file)
    pending = [record for record in source_rows if record.duns not in existing]
    if max_items > 0:
        pending = pending[:max_items]
    logger.info("详情补齐开始：待处理=%d，已完成=%d，并发=%d", len(pending), len(existing), detail_concurrency)
    written = 0
    failed = 0
    get_client = _create_thread_local_client_factory(client)

    def _worker(record: CompanyRecord) -> CompanyRecord:
        payload = get_client().fetch_company_profile(record.company_name_url)
        return parse_company_profile(record, payload)

    with ThreadPoolExecutor(max_workers=max(1, detail_concurrency)) as executor:
        futures = {executor.submit(_worker, record): record for record in pending}
        for future in as_completed(futures):
            record = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                failed += 1
                logger.warning("详情失败（%s）: %s", record.duns or record.company_name_url, exc)
                continue
            existing[result.duns] = result
            written += 1
            if written <= 5 or written % 100 == 0:
                logger.info(
                    "详情进度：成功=%d 失败=%d 最新=%s",
                    written,
                    failed,
                    record.duns or record.company_name_url,
                )
            if written % 200 == 0:
                _save_processed_duns(checkpoint_file, set(existing.keys()))
                _atomic_write_jsonl(output_file, [item.to_dict() for item in existing.values()])
    _atomic_write_jsonl(output_file, [record.to_dict() for record in existing.values()])
    _save_processed_duns(checkpoint_file, set(existing.keys()))
    logger.info("详情补齐完成：成功=%d 失败=%d，输出=%s", written, failed, output_file)
    return written


def run_gmap_enrichment(
    output_dir: Path,
    max_items: int = 0,
    gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY,
) -> int:
    input_file = output_dir / "companies.jsonl"
    output_file = output_dir / "companies_enriched.jsonl"
    checkpoint_file = output_dir / "checkpoint_gmap.json"
    if not input_file.exists():
        return 0
    source_map = _load_record_map(input_file)
    records_map = _merge_record_maps(source_map, _load_record_map(output_file))
    processed_duns = _load_processed_duns(checkpoint_file)
    updated = 0
    pending = [
        record for record in records_map.values()
        if _should_route_downstream(record) and not record.website and record.company_name and record.duns not in processed_duns
    ]
    if max_items > 0:
        pending = pending[:max_items]
    logger.info("GMAP 补官网开始：待处理=%d，并发=%d", len(pending), gmap_concurrency)
    get_client = _create_thread_local_gmap_factory()
    processed = 0

    def _worker(record: CompanyRecord) -> CompanyRecord:
        result = CompanyRecord.from_dict(record.to_dict())
        website = clean_homepage(get_client().search_official_website(build_gmap_query(record)))
        if website:
            result.website = website
            result.domain = extract_domain(website)
        return result

    with ThreadPoolExecutor(max_workers=max(1, gmap_concurrency)) as executor:
        futures = {executor.submit(_worker, record): record for record in pending}
        for future in as_completed(futures):
            original = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                processed += 1
                processed_duns.add(original.duns)
                logger.warning("GMAP 失败（%s）: %s", original.duns, exc)
                if processed <= 5 or processed % 20 == 0:
                    logger.info("GMAP 进度：处理=%d 更新=%d 最新=%s", processed, updated, original.duns)
                continue
            records_map[result.duns] = result
            processed_duns.add(result.duns)
            processed += 1
            if result.website and result.website != original.website:
                updated += 1
            if processed <= 5 or processed % 20 == 0:
                logger.info("GMAP 进度：处理=%d 更新=%d 最新=%s", processed, updated, original.duns)
    _atomic_write_jsonl(output_file, [record.to_dict() for record in records_map.values()])
    _save_processed_duns(checkpoint_file, processed_duns)
    logger.info("GMAP 补官网完成：更新=%d，输出=%s", updated, output_file)
    return updated


def run_snov_enrichment(
    output_dir: Path,
    max_items: int = 0,
    snov_concurrency: int = DEFAULT_SNOV_CONCURRENCY,
) -> int:
    input_file = output_dir / "companies_enriched.jsonl"
    if not input_file.exists():
        input_file = output_dir / "companies.jsonl"
    output_file = output_dir / "companies_with_emails.jsonl"
    checkpoint_file = output_dir / "checkpoint_snov.json"
    if not input_file.exists():
        return 0
    client_id = os.getenv("SNOV_CLIENT_ID", "").strip()
    client_secret = os.getenv("SNOV_CLIENT_SECRET", "").strip()
    source_map = _load_record_map(input_file)
    records_map = _merge_record_maps(source_map, _load_record_map(output_file))
    if not client_id or not client_secret:
        _atomic_write_jsonl(output_file, [record.to_dict() for record in records_map.values()])
        logger.info("Snov 跳过：未配置 SNOV_CLIENT_ID/SNOV_CLIENT_SECRET，直接落盘=%s", output_file)
        return 0
    processed_duns = _load_processed_duns(checkpoint_file)
    updated = 0
    pending = [
        record for record in records_map.values()
        if _should_route_downstream(record) and record.domain and not record.emails and record.duns not in processed_duns
    ]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        return 0
    logger.info("Snov 补邮箱开始：待处理=%d，并发=%d", len(pending), snov_concurrency)
    get_client = _create_thread_local_snov_factory(client_id, client_secret)
    processed = 0

    def _worker(record: CompanyRecord) -> tuple[str, list[str]]:
        return record.duns, get_client().get_domain_emails(record.domain)

    with ThreadPoolExecutor(max_workers=max(1, snov_concurrency)) as executor:
        futures = {executor.submit(_worker, record): record for record in pending}
        for future in as_completed(futures):
            record = futures[future]
            try:
                duns, emails = future.result()
            except (SnovRateLimitError, SnovNoCreditError) as exc:
                processed += 1
                logger.warning("Snov 额度/限流跳过（%s）: %s", record.duns, exc)
                if processed <= 5 or processed % 20 == 0:
                    logger.info("Snov 进度：处理=%d 更新=%d 最新=%s", processed, updated, record.duns)
                continue
            except Exception as exc:
                processed += 1
                logger.warning("Snov 失败（%s）: %s", record.duns, exc)
                if processed <= 5 or processed % 20 == 0:
                    logger.info("Snov 进度：处理=%d 更新=%d 最新=%s", processed, updated, record.duns)
                continue
            processed_duns.add(duns)
            processed += 1
            merged = merge_emails(records_map[duns].emails, emails)
            if merged != records_map[duns].emails:
                records_map[duns].emails = merged
                updated += 1
            if processed <= 5 or processed % 20 == 0:
                logger.info("Snov 进度：处理=%d 更新=%d 最新=%s", processed, updated, record.duns)
    _atomic_write_jsonl(output_file, [record.to_dict() for record in records_map.values()])
    _save_processed_duns(checkpoint_file, processed_duns)
    logger.info("Snov 补邮箱完成：更新=%d，输出=%s", updated, output_file)
    return updated


def run_parallel_enrichment_pipeline(
    output_dir: Path,
    client: DnbClient,
    max_items: int = 0,
    detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY,
    gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY,
    snov_concurrency: int = DEFAULT_SNOV_CONCURRENCY,
    skip_gmap: bool = False,
    skip_snov: bool = False,
    poll_interval: int = DEFAULT_ENRICH_POLL_INTERVAL,
) -> None:
    detail_done = threading.Event()
    gmap_done = threading.Event()

    def _phase_detail() -> None:
        try:
            run_company_details(
                output_dir=output_dir,
                client=client,
                max_items=max_items,
                detail_concurrency=detail_concurrency,
            )
        finally:
            detail_done.set()

    def _phase_gmap() -> None:
        if skip_gmap:
            gmap_done.set()
            return
        batch_limit = _resolve_batch_limit(max_items, DEFAULT_GMAP_BATCH_SIZE)
        try:
            while True:
                updated = run_gmap_enrichment(
                    output_dir=output_dir,
                    max_items=batch_limit,
                    gmap_concurrency=gmap_concurrency,
                )
                if detail_done.is_set() and updated == 0:
                    break
                time.sleep(poll_interval)
        except Exception as exc:
            logger.exception("GMap 线程异常: %s", exc)
        finally:
            gmap_done.set()

    def _phase_snov() -> None:
        if skip_snov:
            return
        batch_limit = _resolve_batch_limit(max_items, DEFAULT_SNOV_BATCH_SIZE)
        try:
            while True:
                updated = run_snov_enrichment(
                    output_dir=output_dir,
                    max_items=batch_limit,
                    snov_concurrency=snov_concurrency,
                )
                if detail_done.is_set() and gmap_done.is_set() and updated == 0:
                    break
                time.sleep(poll_interval)
        except Exception as exc:
            logger.exception("Snov 线程异常: %s", exc)

    threads = [
        threading.Thread(target=_phase_detail, name="DNB-Detail", daemon=True),
        threading.Thread(target=_phase_gmap, name="DNB-GMap", daemon=True),
    ]
    if not skip_snov:
        threads.append(threading.Thread(target=_phase_snov, name="DNB-Snov", daemon=True))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def deduplicate_records(output_dir: Path) -> int:
    source_file = output_dir / "companies_with_emails.jsonl"
    if not source_file.exists():
        source_file = output_dir / "companies_enriched.jsonl"
    if not source_file.exists():
        source_file = output_dir / "companies.jsonl"
    if not source_file.exists():
        return 0
    rows = _load_jsonl_records(source_file, model=CompanyRecord)
    best: dict[str, CompanyRecord] = {}
    for record in rows:
        key = f"{record.duns}|{record.domain}" if record.domain else record.duns
        score = (
            1 if record.emails else 0,
            1 if record.phone else 0,
            1 if record.key_principal else 0,
            1 if record.website else 0,
        )
        current = best.get(key)
        if current is None:
            best[key] = record
            continue
        current_score = (
            1 if current.emails else 0,
            1 if current.phone else 0,
            1 if current.key_principal else 0,
            1 if current.website else 0,
        )
        if score > current_score:
            best[key] = record
    output_file = output_dir / "final_companies.jsonl"
    _atomic_write_jsonl(output_file, [record.to_dict() for record in best.values()])
    logger.info("最终去重完成：输入=%d，输出=%d，文件=%s", len(rows), len(best), output_file)
    return len(best)
