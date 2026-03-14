"""catch.co.kr 官网补齐模块 — Google Maps 官网补齐与增量 Snov 队列。"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from korea_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig
from korea_crawler.snov.client import is_valid_domain
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)

DEFAULT_GMAP_CONCURRENCY = 32
DEFAULT_SNAPSHOT_FLUSH_INTERVAL = 500
DEFAULT_SN0V_QUEUE_POLL_INTERVAL = 10
GMAP_QUEUE_FILENAME = "companies_gmap_queue.jsonl"
G_MAP_SN0V_OUTPUT_FILENAME = "companies_with_emails_gmap.jsonl"
G_MAP_SN0V_CHECKPOINT_FILENAME = "checkpoint_snov_gmap.json"
BLOCKED_HOMEPAGE_HOST_HINTS = (
    "wikipedia.org",
    "wikidata.org",
    "namu.wiki",
    "google.",
    "gstatic.",
    "googleusercontent.",
    "googleapis.",
    "g.page",
    "goo.gl",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "blog.naver.com",
    "jobkorea.co.kr",
    "saramin.co.kr",
    "catch.co.kr",
    "stayfolio.com",
)
DOMAIN_HINT_PATTERN = re.compile(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")
_gmap_thread_local = threading.local()


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _clean_homepage(raw_url: str) -> str:
    url = _normalize_text(raw_url)
    if not url:
        return ""
    url = re.sub(r"\s+", "", url)
    if url.startswith("mailto:"):
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    if url.startswith("www."):
        url = f"https://{url}"
    if not url.startswith(("http://", "https://")):
        if DOMAIN_HINT_PATTERN.fullmatch(url):
            url = f"https://{url}"
        else:
            return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return ""
    if any(hint in host for hint in BLOCKED_HOMEPAGE_HOST_HINTS):
        return ""
    try:
        host_ascii = host.encode("idna").decode("ascii")
    except UnicodeError:
        return ""
    if not is_valid_domain(host_ascii):
        return ""
    netloc = parsed.netloc or parsed.path
    if netloc:
        url = url.replace(netloc, host_ascii, 1)
    return url


def _load_jsonl_records(filepath: Path) -> list[dict]:
    if not filepath.exists():
        return []
    rows: list[dict] = []
    with filepath.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict) and row.get("comp_id"):
                rows.append(row)
    return rows


def _atomic_write_jsonl(filepath: Path, records: list[dict]) -> None:
    tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fp:
        for row in records:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(filepath)


def _load_checkpoint_ids(filepath: Path) -> set[str]:
    if not filepath.exists():
        return set()
    try:
        payload = json.loads(filepath.read_text(encoding="utf-8"))
    except Exception:
        return set()
    return {str(x).strip() for x in payload.get("processed_ids", []) if str(x).strip()}


def _save_checkpoint_ids(filepath: Path, processed_ids: set[str]) -> None:
    filepath.write_text(
        json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
        encoding="utf-8",
    )


def _merge_companies_for_gmap(source_rows: list[dict], enriched_rows: list[dict]) -> list[dict]:
    enriched_map = {
        str(row.get("comp_id", "")): row
        for row in enriched_rows
        if row.get("comp_id")
    }
    merged: list[dict] = []
    seen: set[str] = set()
    for src in source_rows:
        comp_id = str(src.get("comp_id", "")).strip()
        if not comp_id or comp_id in seen:
            continue
        seen.add(comp_id)
        out = dict(src)
        if not out.get("homepage"):
            old = enriched_map.get(comp_id, {})
            if old.get("homepage"):
                out["homepage"] = old.get("homepage")
        merged.append(out)
    return merged


def _get_gmap_client(search_pb: str, hl: str, gl: str) -> GoogleMapsClient:
    pb_template = search_pb.strip() if search_pb.strip() else GoogleMapsConfig().pb_template
    if not hasattr(_gmap_thread_local, "client"):
        _gmap_thread_local.client = GoogleMapsClient(
            GoogleMapsConfig(
                hl=hl,
                gl=gl,
                pb_template=pb_template,
                min_delay=0.4,
                max_delay=0.9,
                long_rest_interval=150,
                long_rest_seconds=5.0,
            )
        )
    return _gmap_thread_local.client


def _build_gmap_query(row: dict) -> str:
    return _normalize_text(str(row.get("company_name", "")))


def run_gmap_stream(
    output_dir: Path,
    max_items: int = 0,
    gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY,
    gmap_search_pb: str = "",
    gmap_hl: str = "ko",
    gmap_gl: str = "kr",
    snapshot_flush_interval: int = DEFAULT_SNAPSHOT_FLUSH_INTERVAL,
) -> tuple[int, int]:
    source_file = output_dir / "companies.jsonl"
    enriched_file = output_dir / "companies_enriched.jsonl"
    checkpoint_file = output_dir / "checkpoint_gmap.json"
    queue_file = output_dir / GMAP_QUEUE_FILENAME

    source_rows = _load_jsonl_records(source_file)
    if not source_rows:
        return 0, 0
    merged_rows = _merge_companies_for_gmap(source_rows, _load_jsonl_records(enriched_file))
    if not merged_rows:
        return 0, 0
    if not enriched_file.exists():
        _atomic_write_jsonl(enriched_file, merged_rows)

    processed_ids = _load_checkpoint_ids(checkpoint_file)
    pending = [
        row for row in merged_rows
        if not row.get("homepage")
        and row.get("company_name")
        and row.get("ceo")
        and str(row.get("comp_id", "")) not in processed_ids
    ]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        return 0, sum(1 for row in merged_rows if str(row.get("homepage", "")).strip())

    logger.info("Catch Google Maps 补官网: 待处理 %d 条, 并发=%d", len(pending), gmap_concurrency)

    merged_map = {str(row.get("comp_id", "")): row for row in merged_rows}
    processed = 0
    found = 0
    failed = 0
    lock = threading.Lock()

    def _worker(raw_record: dict) -> tuple[str, str]:
        comp_id = str(raw_record.get("comp_id", ""))
        query = _build_gmap_query(raw_record)
        homepage = _get_gmap_client(gmap_search_pb, gmap_hl, gmap_gl).search_official_website(query)
        return comp_id, _clean_homepage(homepage)

    try:
        with queue_file.open("a", encoding="utf-8") as queue_fp:
            with ThreadPoolExecutor(max_workers=gmap_concurrency) as executor:
                futures = {executor.submit(_worker, row): row for row in pending}
                for fut in as_completed(futures):
                    original = futures[fut]
                    comp_id = str(original.get("comp_id", ""))
                    try:
                        result_comp_id, homepage = fut.result()
                        with lock:
                            row = merged_map.get(result_comp_id)
                            if row is not None and homepage:
                                row["homepage"] = homepage
                                queue_fp.write(json.dumps(row, ensure_ascii=False) + "\n")
                                queue_fp.flush()
                                found += 1
                            processed_ids.add(comp_id)
                            processed += 1
                            if processed <= 5 or processed % 20 == 0:
                                pct = processed / len(pending) * 100
                                logger.info(
                                    "[GMAP %d/%d] %.1f%% %s | HP=%s",
                                    processed,
                                    len(pending),
                                    pct,
                                    original.get("company_name", ""),
                                    homepage[:60] if homepage else "-",
                                )
                            if processed % snapshot_flush_interval == 0:
                                _save_checkpoint_ids(checkpoint_file, processed_ids)
                                _atomic_write_jsonl(enriched_file, list(merged_map.values()))
                    except Exception as exc:
                        failed += 1
                        processed_ids.add(comp_id)
                        logger.warning("Catch Google Maps 查询失败 (%s): %s", comp_id, exc)
    finally:
        _save_checkpoint_ids(checkpoint_file, processed_ids)
        _atomic_write_jsonl(enriched_file, list(merged_map.values()))

    logger.info("Catch Google Maps 完成: 处理 %d 条 | 找到官网 %d 条 | 失败 %d 条", processed, found, failed)
    return processed, found


def _count_pending_queue(queue_file: Path, checkpoint_file: Path) -> int:
    queue_ids = [row.get("comp_id", "") for row in _load_jsonl_records(queue_file)]
    processed_ids = _load_checkpoint_ids(checkpoint_file)
    return sum(1 for comp_id in queue_ids if comp_id and comp_id not in processed_ids)


def merge_incremental_results_into_standard(output_dir: Path) -> int:
    standard_file = output_dir / "companies_with_emails.jsonl"
    enriched_file = output_dir / "companies_enriched.jsonl"
    gmap_output_file = output_dir / G_MAP_SN0V_OUTPUT_FILENAME
    if not standard_file.exists() or not enriched_file.exists() or not gmap_output_file.exists():
        return 0

    enriched_map = {str(row.get("comp_id", "")): row for row in _load_jsonl_records(enriched_file) if row.get("comp_id")}
    gmap_output_map = {str(row.get("comp_id", "")): row for row in _load_jsonl_records(gmap_output_file) if row.get("comp_id")}
    if not enriched_map and not gmap_output_map:
        return 0

    updated_count = 0
    updated_rows: list[dict] = []
    seen: set[str] = set()
    for row in _load_jsonl_records(standard_file):
        comp_id = str(row.get("comp_id", ""))
        if not comp_id:
            continue
        enriched = enriched_map.get(comp_id)
        if enriched and enriched.get("homepage") and row.get("homepage") != enriched.get("homepage"):
            row["homepage"] = enriched.get("homepage")
            updated_count += 1
        gmap_result = gmap_output_map.get(comp_id)
        if gmap_result and gmap_result.get("emails") != row.get("emails"):
            row["emails"] = list(gmap_result.get("emails", []))
            if gmap_result.get("homepage"):
                row["homepage"] = gmap_result.get("homepage")
            updated_count += 1
        updated_rows.append(row)
        seen.add(comp_id)

    for comp_id, gmap_result in gmap_output_map.items():
        if comp_id in seen:
            continue
        updated_rows.append(gmap_result)
        updated_count += 1

    _atomic_write_jsonl(standard_file, updated_rows)
    return updated_count


def run_incremental_snov_from_queue(
    output_dir: Path,
    max_items: int,
    snov_concurrency: int,
    snov_delay: float,
    gmap_done_event: threading.Event,
    poll_interval: int = DEFAULT_SN0V_QUEUE_POLL_INTERVAL,
) -> int:
    queue_file = output_dir / GMAP_QUEUE_FILENAME
    checkpoint_file = output_dir / G_MAP_SN0V_CHECKPOINT_FILENAME
    total_found = 0

    while True:
        pending = _count_pending_queue(queue_file, checkpoint_file)
        if pending > 0:
            count = run_snov_pipeline(
                output_dir=output_dir,
                max_items=max_items,
                concurrency=snov_concurrency,
                request_delay=snov_delay,
                input_filename=GMAP_QUEUE_FILENAME,
                output_filename=G_MAP_SN0V_OUTPUT_FILENAME,
                checkpoint_filename=G_MAP_SN0V_CHECKPOINT_FILENAME,
            )
            total_found += count
            merge_incremental_results_into_standard(output_dir)
        if gmap_done_event.is_set() and _count_pending_queue(queue_file, checkpoint_file) == 0:
            break
        time.sleep(poll_interval)

    logger.info("Catch 增量 Snov 完成: 找到邮箱 %d 条", total_found)
    return total_found
