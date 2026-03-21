"""khia.or.kr 爬虫入口 — 四阶段: 列表→详情补齐→Google Maps补官网→Snov。"""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from lxml import html as lxml_html

from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig
from korea_crawler.khia_client import KhiaClient, RateLimitConfig
from korea_crawler.models import CompanyRecord
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent

POLL_INTERVAL = 10
DEFAULT_DETAIL_CONCURRENCY = 4
DEFAULT_GMAP_CONCURRENCY = 3

_thread_local = threading.local()
_gmap_thread_local = threading.local()


def _get_client() -> KhiaClient:
    if not hasattr(_thread_local, "client"):
        rate_config = RateLimitConfig(
            min_delay=0.15,
            max_delay=0.45,
            long_rest_interval=300,
            long_rest_seconds=5.0,
        )
        _thread_local.client = KhiaClient(rate_config=rate_config)
    return _thread_local.client


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


def _count_lines(filepath: Path) -> int:
    if not filepath.exists():
        return 0
    with filepath.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _extract_first_ceo(raw_ceo: str) -> str:
    if not raw_ceo:
        return ""
    parts = re.split(r"[/,·、]| 외 ", raw_ceo)
    return parts[0].strip()


def _clean_homepage(raw_url: str) -> str:
    url = _normalize_text(html.unescape(raw_url or ""))
    if not url or url == "-":
        return ""
    url = re.sub(r"[)\],.;]+$", "", url)
    if url.startswith("mailto:"):
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    if url.startswith("www."):
        url = f"https://{url}"
    if not url.startswith(("http://", "https://")):
        if re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$", url):
            url = f"https://{url}"
        else:
            return ""
    parsed = urlparse(url)
    host = (parsed.netloc or parsed.path).strip().lower()
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    if "khia.or.kr" in host:
        return ""
    return url


def _extract_item_id(raw_href: str) -> str:
    match = re.search(r"/sub03_01/(\d+)", raw_href or "")
    return match.group(1) if match else ""


def _extract_total_count(tree: lxml_html.HtmlElement) -> int:
    text = _normalize_text(tree.xpath("string(//div[@id='bo_list_total'])"))
    match = re.search(r"전체\s*([0-9,]+)", text)
    if not match:
        return 0
    return int(match.group(1).replace(",", ""))


def _parse_list_page(html_text: str) -> tuple[list[dict[str, str]], int]:
    """解析列表页，返回公司记录和总条数。"""
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return [], 0

    rows: list[dict[str, str]] = []
    for tr in tree.xpath("//tbody/tr"):
        link_nodes = tr.xpath('.//td[contains(@class, "td_subject")]//a[@href][1]')
        if not link_nodes:
            continue

        link = link_nodes[0]
        item_id = _extract_item_id(link.get("href", ""))
        if not item_id:
            continue

        company_name = _normalize_text(link.text_content())
        tds = tr.xpath("./td")
        homepage = ""
        ceo = ""
        if len(tds) >= 3:
            hp_href = tds[2].xpath('.//a[@href][1]/@href')
            hp_text = _normalize_text(tds[2].text_content())
            homepage = _clean_homepage(hp_href[0] if hp_href else hp_text)
        if len(tds) >= 4:
            ceo = _extract_first_ceo(_normalize_text(tds[3].text_content()))

        rows.append(
            {
                "item_id": item_id,
                "company_name": company_name,
                "ceo": ceo,
                "homepage": homepage,
            }
        )

    return rows, _extract_total_count(tree)


def _extract_homepage_from_detail(html_text: str) -> str:
    """从详情正文尝试提取官网域名，提取不到返回空。"""
    if not html_text or KhiaClient.is_not_found_page(html_text):
        return ""
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return ""

    content = _normalize_text(tree.xpath("string(//*[@id='bo_v_con'])"))
    if not content:
        return ""

    match = re.search(r"https?://[^\s<>\"]+", content)
    if match:
        return _clean_homepage(match.group(0))

    match = re.search(r"\b(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?", content)
    if match:
        return _clean_homepage(match.group(0))

    return ""


def _load_existing_comp_ids(filepath: Path) -> set[str]:
    if not filepath.exists():
        return set()
    existed: set[str] = set()
    with filepath.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                comp_id = json.loads(line).get("comp_id", "")
            except Exception:
                continue
            if comp_id:
                existed.add(comp_id)
    return existed


def _load_jsonl_records(filepath: Path) -> list[dict]:
    if not filepath.exists():
        return []
    records: list[dict] = []
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
                records.append(row)
    return records


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
    """以 companies.jsonl 为主，保留历史 gmap 产出的 homepage。"""
    enriched_map: dict[str, dict] = {
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


def enrich_homepage_with_gmap(
    output_dir: Path,
    max_items: int = 0,
    gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY,
    gmap_search_pb: str = "",
    gmap_hl: str = "ko",
    gmap_gl: str = "kr",
) -> tuple[int, int]:
    """Phase 3: 用公司名走 Google Maps 协议查询官网，输出 companies_enriched.jsonl。"""
    source_file = output_dir / "companies.jsonl"
    enriched_file = output_dir / "companies_enriched.jsonl"
    checkpoint_file = output_dir / "checkpoint_gmap.json"

    source_rows = _load_jsonl_records(source_file)
    if not source_rows:
        return 0, 0
    merged_rows = _merge_companies_for_gmap(source_rows, _load_jsonl_records(enriched_file))
    if not merged_rows:
        return 0, 0

    processed_ids = _load_checkpoint_ids(checkpoint_file)
    pending = [r for r in merged_rows if not r.get("homepage") and r.get("comp_id", "") not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        if not enriched_file.exists():
            _atomic_write_jsonl(enriched_file, merged_rows)
        return 0, 0

    logger.info("KHIA Google Maps 补官网: 待处理 %d 条, 并发=%d", len(pending), gmap_concurrency)

    merged_map = {str(row.get("comp_id")): row for row in merged_rows}
    processed = 0
    found = 0
    failed = 0
    lock = threading.Lock()

    def _worker(raw_record: dict) -> tuple[str, str]:
        comp_id = str(raw_record.get("comp_id", ""))
        company_name = str(raw_record.get("company_name", ""))
        homepage = _get_gmap_client(gmap_search_pb, gmap_hl, gmap_gl).search_official_website(company_name)
        return comp_id, _clean_homepage(homepage)

    try:
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
                except Exception as exc:
                    failed += 1
                    processed_ids.add(comp_id)
                    logger.warning("KHIA Google Maps 查询失败 (%s): %s", comp_id, exc)
    finally:
        _save_checkpoint_ids(checkpoint_file, processed_ids)
        _atomic_write_jsonl(enriched_file, list(merged_map.values()))

    logger.info("KHIA Google Maps 完成: 处理 %d 条 | 找到官网 %d 条 | 失败 %d 条", processed, found, failed)
    return processed, found


def crawl_list(output_dir: Path, max_pages: int = 0) -> int:
    """Phase 1: 爬列表，输出 company_ids.jsonl。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / "checkpoint_list.json"

    last_page = 0
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        last_page = int(data.get("last_page", 0))

    current_page = last_page + 1
    upper_bound = max_pages if max_pages > 0 else 10**9
    mode = "a" if last_page > 0 else "w"
    seen_comp_ids = _load_existing_comp_ids(output_file) if mode == "a" else set()
    total_written = 0
    total_count = 0

    client = _get_client()
    logger.info("KHIA 列表爬虫: 从第 %d 页开始, 上限 %s 页", current_page, upper_bound)

    try:
        with output_file.open(mode, encoding="utf-8") as fp:
            while current_page <= upper_bound:
                html_text = client.get_list_html(page=current_page)
                rows, parsed_total = _parse_list_page(html_text)
                if parsed_total > 0 and total_count == 0:
                    total_count = parsed_total
                    logger.info("KHIA 列表总量: %d 家公司", total_count)

                if not rows:
                    if current_page == 1:
                        logger.warning("第 1 页未解析到公司数据，可能触发风控或页面结构变更")
                    else:
                        logger.info("第 %d 页无公司数据，列表阶段结束", current_page)
                    break

                page_written = 0
                for row in rows:
                    comp_id = f"KH_{row['item_id']}"
                    if comp_id in seen_comp_ids:
                        continue

                    record = CompanyRecord(
                        comp_id=comp_id,
                        company_name=row["company_name"],
                        ceo=row["ceo"],
                        homepage=row["homepage"],
                    )
                    fp.write(record.to_json_line() + "\n")
                    seen_comp_ids.add(comp_id)
                    total_written += 1
                    page_written += 1

                fp.flush()
                checkpoint_file.write_text(
                    json.dumps({"last_page": current_page}, ensure_ascii=False),
                    encoding="utf-8",
                )

                if current_page <= 3 or current_page % 10 == 0:
                    logger.info(
                        "第 %d 页: 新增 %d / 解析 %d | 累计 %d",
                        current_page,
                        page_written,
                        len(rows),
                        total_written,
                    )

                # 当前站点通常第一页即全部数据，达到总量后直接结束。
                if max_pages <= 0 and total_count > 0 and len(seen_comp_ids) >= total_count:
                    break

                current_page += 1

    except Exception:
        checkpoint_file.write_text(
            json.dumps({"last_page": current_page - 1}, ensure_ascii=False),
            encoding="utf-8",
        )
        raise

    logger.info("KHIA 列表完成: 新增 %d 条公司", total_written)
    return total_written


def crawl_details(
    output_dir: Path,
    max_items: int = 0,
    detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY,
) -> int:
    """Phase 2: 详情补齐阶段，输出 companies.jsonl。"""
    ids_file = output_dir / "company_ids.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint_detail.json"

    if not ids_file.exists():
        return 0

    records: list[dict] = []
    with ids_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    processed_ids: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed_ids = set(data.get("processed_ids", []))
    processed_ids.update(_load_existing_comp_ids(output_file))

    pending = [r for r in records if r.get("comp_id", "") not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        return 0

    logger.info("KHIA 详情补齐: 待处理 %d 条, 并发=%d", len(pending), detail_concurrency)

    write_lock = threading.Lock()
    written = 0
    failed = 0

    def _worker(raw_record: dict) -> CompanyRecord:
        record = CompanyRecord.from_dict(raw_record)
        if record.homepage:
            return record
        item_id = record.comp_id.replace("KH_", "", 1)
        detail_html = _get_client().get_detail_html(item_id)
        homepage = _extract_homepage_from_detail(detail_html)
        if homepage:
            record.homepage = homepage
        return record

    try:
        with (
            output_file.open("a", encoding="utf-8") as fp,
            ThreadPoolExecutor(max_workers=detail_concurrency) as executor,
        ):
            futures = {executor.submit(_worker, r): r for r in pending}

            for fut in as_completed(futures):
                original = futures[fut]
                try:
                    result = fut.result()
                    with write_lock:
                        fp.write(result.to_json_line() + "\n")
                        fp.flush()
                        processed_ids.add(result.comp_id)
                        written += 1

                        if written % 50 == 0:
                            checkpoint_file.write_text(
                                json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
                                encoding="utf-8",
                            )

                        if written <= 5 or written % 50 == 0:
                            pct = written / len(pending) * 100
                            logger.info(
                                "[%d/%d] %.1f%% %s | CEO=%s | HP=%s",
                                written,
                                len(pending),
                                pct,
                                result.company_name,
                                result.ceo or "-",
                                result.homepage[:50] if result.homepage else "-",
                            )
                except Exception as exc:
                    failed += 1
                    logger.warning("KHIA 详情失败 (%s): %s", original.get("comp_id", ""), exc)

    finally:
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
            encoding="utf-8",
        )

    logger.info("KHIA 详情完成: 成功 %d 条 | 失败 %d 条", written, failed)
    return written


def _resolve_snov_input_filename(output_dir: Path, skip_gmap: bool) -> str:
    if skip_gmap:
        return "companies.jsonl"
    return "companies_enriched.jsonl"


def _resolve_snov_output_filename(skip_gmap: bool) -> str:
    if skip_gmap:
        return "companies_with_emails.jsonl"
    return "companies_with_emails_enriched.jsonl"


def _resolve_snov_checkpoint_filename(skip_gmap: bool) -> str:
    if skip_gmap:
        return "checkpoint_snov.json"
    return "checkpoint_snov_enriched.json"


def _sync_snov_output_if_needed(output_dir: Path, skip_gmap: bool, allow_overwrite: bool) -> None:
    if skip_gmap:
        return
    if not allow_overwrite:
        return
    src_file = output_dir / _resolve_snov_output_filename(skip_gmap=False)
    dst_file = output_dir / "companies_with_emails.jsonl"
    if not src_file.exists():
        return
    shutil.copyfile(src_file, dst_file)


def _refresh_snov_enriched_state(output_dir: Path) -> int:
    """当官网更新后，清理 Snov 的旧状态，确保新增官网能被重新查询。"""
    input_file = output_dir / "companies_enriched.jsonl"
    output_file = output_dir / "companies_with_emails_enriched.jsonl"
    checkpoint_file = output_dir / "checkpoint_snov_enriched.json"
    if not input_file.exists():
        return 0

    source_rows = _load_jsonl_records(input_file)
    source_map = {str(row.get("comp_id", "")): row for row in source_rows if row.get("comp_id")}
    output_rows = _load_jsonl_records(output_file)
    output_map = {str(row.get("comp_id", "")): row for row in output_rows if row.get("comp_id")}

    stale_ids: set[str] = set()
    for comp_id, src in source_map.items():
        src_homepage = str(src.get("homepage", "")).strip()
        if not src_homepage:
            continue
        out = output_map.get(comp_id)
        if out is None:
            stale_ids.add(comp_id)
            continue
        out_homepage = str(out.get("homepage", "")).strip()
        if out_homepage != src_homepage:
            stale_ids.add(comp_id)

    if not stale_ids:
        return 0

    kept_rows = [row for row in output_rows if str(row.get("comp_id", "")) not in stale_ids]
    _atomic_write_jsonl(output_file, kept_rows)

    processed_ids = _load_checkpoint_ids(checkpoint_file)
    processed_ids = {cid for cid in processed_ids if cid not in stale_ids}
    _save_checkpoint_ids(checkpoint_file, processed_ids)
    return len(stale_ids)


def _build_khia_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KHIA 会员企业数据爬取")
    parser.add_argument("--max-pages", type=int, default=0, help="列表最大页数")
    parser.add_argument("--max-items", type=int, default=0, help="详情/Snov最大条数")
    parser.add_argument("--skip-list", action="store_true", help="跳过列表阶段")
    parser.add_argument("--skip-detail", action="store_true", help="跳过详情阶段")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 官网补齐阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过Snov阶段")
    parser.add_argument("--serial", action="store_true", help="串行模式")
    parser.add_argument(
        "--detail-concurrency",
        type=int,
        default=DEFAULT_DETAIL_CONCURRENCY,
        help=f"详情阶段并发数（默认 {DEFAULT_DETAIL_CONCURRENCY}）",
    )
    parser.add_argument(
        "--gmap-concurrency",
        type=int,
        default=DEFAULT_GMAP_CONCURRENCY,
        help=f"Google Maps 阶段并发数（默认 {DEFAULT_GMAP_CONCURRENCY}）",
    )
    parser.add_argument("--gmap-hl", default="ko", help="Google Maps 语言参数 hl（默认 ko）")
    parser.add_argument("--gmap-gl", default="kr", help="Google Maps 地区参数 gl（默认 kr）")
    parser.add_argument("--gmap-search-pb", default="", help="Google Maps 搜索 pb 参数（可选）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def _run_khia_phase2(args: argparse.Namespace, output_dir: Path, phase1_done: threading.Event, phase2_done: threading.Event) -> None:
    ids_file = output_dir / "company_ids.jsonl"
    total_detail_processed = 0
    total_gmap_processed = 0
    total_gmap_found = 0

    while not ids_file.exists() or _count_lines(ids_file) == 0:
        if phase1_done.is_set():
            break
        time.sleep(POLL_INTERVAL)

    try:
        while True:
            if not args.skip_detail:
                count = crawl_details(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    detail_concurrency=args.detail_concurrency,
                )
                total_detail_processed += count

            if not args.skip_gmap:
                gmap_processed, gmap_found = enrich_homepage_with_gmap(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    gmap_concurrency=args.gmap_concurrency,
                    gmap_search_pb=args.gmap_search_pb,
                    gmap_hl=args.gmap_hl,
                    gmap_gl=args.gmap_gl,
                )
                total_gmap_processed += gmap_processed
                total_gmap_found += gmap_found

            if args.max_items > 0 and total_detail_processed >= args.max_items:
                break

            if phase1_done.is_set():
                if not args.skip_detail:
                    final = crawl_details(
                        output_dir=output_dir,
                        max_items=args.max_items,
                        detail_concurrency=args.detail_concurrency,
                    )
                    total_detail_processed += final
                if not args.skip_gmap:
                    gmap_processed, gmap_found = enrich_homepage_with_gmap(
                        output_dir=output_dir,
                        max_items=args.max_items,
                        gmap_concurrency=args.gmap_concurrency,
                        gmap_search_pb=args.gmap_search_pb,
                        gmap_hl=args.gmap_hl,
                        gmap_gl=args.gmap_gl,
                    )
                    total_gmap_processed += gmap_processed
                    total_gmap_found += gmap_found
                break
            time.sleep(POLL_INTERVAL)
    except Exception as exc:
        logger.error("Phase 2 异常: %s", exc)
    finally:
        phase2_done.set()
        logger.info(
            "Phase 2 总计: 详情处理 %d 条 | GMAP处理 %d 条 | GMAP找到官网 %d 条",
            total_detail_processed,
            total_gmap_processed,
            total_gmap_found,
        )


def _run_khia_phase1(args: argparse.Namespace, output_dir: Path, phase1_done: threading.Event) -> None:
    try:
        crawl_list(output_dir=output_dir, max_pages=args.max_pages)
    finally:
        phase1_done.set()


def _run_khia_phase3(args: argparse.Namespace, output_dir: Path, phase2_done: threading.Event) -> None:
    total_found = 0
    output_filename = _resolve_snov_output_filename(args.skip_gmap)
    checkpoint_filename = _resolve_snov_checkpoint_filename(args.skip_gmap)

    try:
        # 开启 gmap 时，先等待官网补齐结束再跑 Snov，避免“官网更新晚于邮箱查询”导致漏查。
        if not args.skip_gmap:
            while not phase2_done.is_set():
                time.sleep(POLL_INTERVAL)
            stale_count = _refresh_snov_enriched_state(output_dir)
            if stale_count > 0:
                logger.info("Snov 状态校准: 发现 %d 条官网更新记录，已重置为待查询", stale_count)

        companies_file = output_dir / _resolve_snov_input_filename(output_dir, args.skip_gmap)
        while not companies_file.exists() or _count_lines(companies_file) == 0:
            if phase2_done.is_set():
                companies_file = output_dir / _resolve_snov_input_filename(output_dir, args.skip_gmap)
                break
            time.sleep(POLL_INTERVAL)

        while True:
            input_filename = _resolve_snov_input_filename(output_dir, args.skip_gmap)
            count = run_snov_pipeline(
                output_dir=output_dir,
                max_items=args.max_items,
                input_filename=input_filename,
                output_filename=output_filename,
                checkpoint_filename=checkpoint_filename,
            )
            total_found += count

            if args.max_items > 0 and total_found >= args.max_items:
                break
            if not args.skip_gmap:
                break
            if phase2_done.is_set():
                input_filename = _resolve_snov_input_filename(output_dir, args.skip_gmap)
                final = run_snov_pipeline(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    input_filename=input_filename,
                    output_filename=output_filename,
                    checkpoint_filename=checkpoint_filename,
                )
                total_found += final
                break
            time.sleep(POLL_INTERVAL)
    except Exception as exc:
        logger.error("Phase 3 异常: %s", exc)
    finally:
        logger.info("Phase 3 总计找到邮箱: %d 条", total_found)


def _run_khia_serial(args: argparse.Namespace, output_dir: Path) -> None:
    if not args.skip_list:
        crawl_list(output_dir=output_dir, max_pages=args.max_pages)
    if not args.skip_detail:
        crawl_details(
            output_dir=output_dir,
            max_items=args.max_items,
            detail_concurrency=args.detail_concurrency,
        )
    if not args.skip_gmap:
        enrich_homepage_with_gmap(
            output_dir=output_dir,
            max_items=args.max_items,
            gmap_concurrency=args.gmap_concurrency,
            gmap_search_pb=args.gmap_search_pb,
            gmap_hl=args.gmap_hl,
            gmap_gl=args.gmap_gl,
        )
    if not args.skip_snov:
        input_filename = _resolve_snov_input_filename(output_dir, args.skip_gmap)
        run_snov_pipeline(
            output_dir=output_dir,
            max_items=args.max_items,
            input_filename=input_filename,
            output_filename=_resolve_snov_output_filename(args.skip_gmap),
            checkpoint_filename=_resolve_snov_checkpoint_filename(args.skip_gmap),
        )


def _run_khia_parallel(
    args: argparse.Namespace,
    output_dir: Path,
    phase1_done: threading.Event,
    phase2_done: threading.Event,
) -> None:
    threads: list[threading.Thread] = []

    if not args.skip_list:
        t1 = threading.Thread(target=_run_khia_phase1, args=(args, output_dir, phase1_done), name="Phase1-List", daemon=True)
        threads.append(t1)
        t1.start()

    if not args.skip_detail or not args.skip_gmap:
        t2 = threading.Thread(
            target=_run_khia_phase2,
            args=(args, output_dir, phase1_done, phase2_done),
            name="Phase2-Detail",
            daemon=True,
        )
        threads.append(t2)
        t2.start()

    if not args.skip_snov:
        t3 = threading.Thread(target=_run_khia_phase3, args=(args, output_dir, phase2_done), name="Phase3-Snov", daemon=True)
        threads.append(t3)
        t3.start()

    for t in threads:
        t.join()


def run_khia(argv: list[str]) -> int:
    """khia.or.kr 爬取入口。"""
    parser = _build_khia_parser()
    args = parser.parse_args(argv)
    args.detail_concurrency = max(1, args.detail_concurrency)
    args.gmap_concurrency = max(1, args.gmap_concurrency)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = ROOT / "output" / "khia"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== khia.or.kr 爬虫启动 ===")

    phase1_done = threading.Event()
    phase2_done = threading.Event()
    if args.skip_list:
        phase1_done.set()
    if args.skip_detail and args.skip_gmap:
        phase2_done.set()

    try:
        if args.serial:
            _run_khia_serial(args, output_dir)
        else:
            _run_khia_parallel(args, output_dir, phase1_done, phase2_done)

        _sync_snov_output_if_needed(
            output_dir=output_dir,
            skip_gmap=args.skip_gmap,
            allow_overwrite=args.max_items <= 0,
        )
        final_file = output_dir / "companies_with_emails.jsonl"
        if final_file.exists():
            logger.info("--- 域名去重 ---")
            deduped = deduplicate_by_domain(final_file)
            logger.info("去重完成: %d 条", deduped)
    except KeyboardInterrupt:
        logger.warning("用户中断，已保存断点。")
        return 130

    logger.info("=== khia.or.kr 爬虫完毕 ===")
    return 0
