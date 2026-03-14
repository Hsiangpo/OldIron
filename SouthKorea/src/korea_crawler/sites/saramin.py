"""saramin.co.kr 爬虫入口 — 三阶段: 列表→详情→Snov。"""

from __future__ import annotations

import argparse
import html
import json
import logging
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from lxml import html as lxml_html

from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.google_maps.pipeline import (
    DEFAULT_GMAP_CONCURRENCY,
    merge_incremental_results_into_standard,
    run_gmap_stream,
    run_incremental_snov_from_queue,
)
from korea_crawler.models import CompanyRecord
from korea_crawler.saramin_client import (
    DEFAULT_LOC_MCD,
    RateLimitConfig,
    SaraminClient,
)
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent

POLL_INTERVAL = 10
PAGE_SIZE = 100
DEFAULT_DETAIL_CONCURRENCY = 5

_thread_local = threading.local()


def _get_client() -> SaraminClient:
    if not hasattr(_thread_local, "client"):
        rate_config = RateLimitConfig(
            min_delay=0.2,
            max_delay=0.7,
            long_rest_interval=300,
            long_rest_seconds=5.0,
        )
        _thread_local.client = SaraminClient(rate_config=rate_config)
    return _thread_local.client


def _count_lines(filepath: Path) -> int:
    if not filepath.exists():
        return 0
    with filepath.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp)


def _build_gmap_query(row: dict) -> str:
    return str(row.get("company_name", "")).strip()


def _run_gmap_and_incremental_snov(output_dir: Path, args: argparse.Namespace) -> None:
    if args.skip_gmap:
        return

    gmap_done_event = threading.Event()

    def _phase_gmap() -> None:
        logger.info("--- Phase 4: Google Maps 官网补齐 ---")
        processed, found = run_gmap_stream(
            output_dir=output_dir,
            site_label="Saramin",
            query_builder=_build_gmap_query,
            max_items=args.max_items,
            gmap_concurrency=args.gmap_concurrency,
            gmap_search_pb=args.gmap_search_pb,
            gmap_hl=args.gmap_hl,
            gmap_gl=args.gmap_gl,
        )
        logger.info("Phase 4 完成: 处理 %d 条 | 新官网 %d 条", processed, found)
        gmap_done_event.set()

    threads: list[threading.Thread] = []
    t_gmap = threading.Thread(target=_phase_gmap, name="Phase4-GMap", daemon=True)
    threads.append(t_gmap)
    t_gmap.start()

    if not args.skip_snov:
        def _phase_snov() -> None:
            logger.info("--- Phase 5: 增量 Snov（边补官网边查询）---")
            run_incremental_snov_from_queue(
                output_dir=output_dir,
                site_label="Saramin",
                max_items=args.max_items,
                snov_concurrency=args.snov_concurrency,
                snov_delay=args.snov_delay,
                gmap_done_event=gmap_done_event,
            )
            updated = merge_incremental_results_into_standard(output_dir)
            logger.info("Phase 5 合并回主文件: %d 处更新", updated)

        t_snov = threading.Thread(target=_phase_snov, name="Phase5-SnovGMap", daemon=True)
        threads.append(t_snov)
        t_snov.start()

    for thread in threads:
        thread.join()


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _extract_query_param(raw_href: str, key: str) -> str:
    if not raw_href:
        return ""
    href = html.unescape(raw_href)
    parsed = urlparse(href)
    values = parse_qs(parsed.query).get(key, [])
    return values[0].strip() if values else ""


def _extract_first_ceo(raw_ceo: str) -> str:
    if not raw_ceo:
        return ""
    parts = re.split(r"[/,·、]| 외 ", raw_ceo)
    return parts[0].strip()


def _clean_homepage(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    url = html.unescape(url)
    url = re.sub(r"[)\],.;]+$", "", url)
    if url.startswith("//"):
        url = f"https:{url}"
    if url.startswith("www."):
        url = f"https://{url}"
    if not url.startswith(("http://", "https://")):
        if re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$", url):
            url = f"https://{url}"
        else:
            return ""
    if not url.startswith(("http://", "https://")):
        return ""
    if "saramin.co.kr" in url:
        return ""
    if "map.kakao.com" in url:
        return ""
    return url


def _parse_list_contents(contents_html: str) -> list[dict[str, str]]:
    """解析列表返回的 contents HTML，提取公司ID和公司名。"""
    if not contents_html:
        return []

    try:
        tree = lxml_html.fromstring(contents_html)
    except Exception:
        return []

    rows: list[dict[str, str]] = []
    seen_csn: set[str] = set()

    for item in tree.xpath('//div[contains(@class, "list_item")]'):
        company_link = item.xpath(
            './/div[contains(@class, "company_nm")]'
            '//a[contains(@href, "/zf_user/company-info/view-inner-recruit") '
            'or contains(@href, "/zf_user/company-info/view?")][1]'
        )
        if not company_link:
            continue

        link = company_link[0]
        href = link.get("href", "")
        csn = _extract_query_param(href, "csn")
        if not csn or csn in seen_csn:
            continue

        company_name = _normalize_text(link.text_content())
        rec_href = item.xpath(
            './/div[contains(@class, "job_tit")]'
            '//a[contains(@href, "rec_idx=")][1]/@href'
        )
        rec_idx = _extract_query_param(rec_href[0], "rec_idx") if rec_href else ""

        rows.append(
            {
                "csn": csn,
                "company_name": company_name,
                "rec_idx": rec_idx,
            }
        )
        seen_csn.add(csn)

    return rows


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


def crawl_list(
    output_dir: Path,
    max_pages: int = 0,
    loc_mcd: str = DEFAULT_LOC_MCD,
) -> int:
    """Phase 1: 爬列表，输出 company_ids.jsonl。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / "checkpoint_list.json"

    last_page = 0
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        last_page = int(data.get("last_page", 0))

    current_page = last_page + 1
    mode = "a" if last_page > 0 else "w"
    seen_comp_ids = _load_existing_comp_ids(output_file) if mode == "a" else set()

    total_written = 0
    total_pages = 0
    client = _get_client()
    upper_bound = max_pages if max_pages > 0 else 10**9

    logger.info("Saramin 列表爬虫: 从第 %d 页开始, 上限 %s 页", current_page, upper_bound)

    try:
        with output_file.open(mode, encoding="utf-8") as fp:
            while current_page <= upper_bound:
                data = client.get_list_json(page=current_page, loc_mcd=loc_mcd, page_count=PAGE_SIZE)
                total_count = int(data.get("total_count", 0) or 0)
                if total_count > 0 and total_pages == 0:
                    total_pages = max(1, math.ceil(total_count / PAGE_SIZE))
                    logger.info("Saramin 预估页数: %d (总岗位 %d)", total_pages, total_count)

                rows = _parse_list_contents(str(data.get("contents", "")))
                if not rows:
                    logger.info("第 %d 页无数据，列表阶段结束", current_page)
                    break

                page_written = 0
                for row in rows:
                    comp_id = f"SA_{row['csn']}"
                    if comp_id in seen_comp_ids:
                        continue

                    record = CompanyRecord(
                        comp_id=comp_id,
                        company_name=row["company_name"],
                    )
                    payload = json.loads(record.to_json_line())
                    if row["rec_idx"]:
                        payload["rec_idx"] = row["rec_idx"]
                    fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    seen_comp_ids.add(comp_id)
                    total_written += 1
                    page_written += 1

                fp.flush()
                checkpoint_file.write_text(
                    json.dumps({"last_page": current_page}, ensure_ascii=False),
                    encoding="utf-8",
                )

                if current_page <= 3 or current_page % 20 == 0:
                    pct = (current_page / total_pages * 100) if total_pages else 0
                    logger.info(
                        "第 %d 页: 新增 %d / 解析 %d | 累计 %d | %.1f%%",
                        current_page, page_written, len(rows), total_written, pct,
                    )

                if max_pages <= 0 and total_pages > 0 and current_page >= total_pages:
                    break

                current_page += 1

    except Exception:
        checkpoint_file.write_text(
            json.dumps({"last_page": current_page - 1}, ensure_ascii=False),
            encoding="utf-8",
        )
        raise

    logger.info("Saramin 列表完成: 新增 %d 条公司", total_written)
    return total_written


def _extract_detail_value(tree: lxml_html.HtmlElement, label: str) -> str:
    dt_nodes = tree.xpath(f'//dt[contains(@class, "tit") and normalize-space()="{label}"]')
    for dt in dt_nodes:
        dd = dt.getnext()
        if dd is None or dd.tag.lower() != "dd":
            continue
        text = _normalize_text(dd.text_content())
        if text:
            return text
    return ""


def _extract_detail_link(tree: lxml_html.HtmlElement, label: str) -> str:
    dt_nodes = tree.xpath(f'//dt[contains(@class, "tit") and normalize-space()="{label}"]')
    for dt in dt_nodes:
        dd = dt.getnext()
        if dd is None or dd.tag.lower() != "dd":
            continue
        href = dd.xpath('.//a[@href][1]/@href')
        if href:
            cleaned = _clean_homepage(href[0])
            if cleaned:
                return cleaned
        text = _clean_homepage(_normalize_text(dd.text_content()))
        if text:
            return text
    return ""


def _extract_company_name(tree: lxml_html.HtmlElement) -> str:
    name = tree.xpath('string(//input[@id="companyinfo_company_nm"]/@value)')
    if name.strip():
        return _normalize_text(name)
    heading = tree.xpath('string(//h1[contains(@class, "company_name")])')
    if heading.strip():
        return _normalize_text(heading)
    title = tree.xpath("string(//title)")
    if title:
        return _normalize_text(title.split(" 20", 1)[0])
    return ""


def _parse_company_html(html_text: str) -> tuple[str, str, str]:
    """解析公司详情页，返回 (ceo, homepage, company_name)。"""
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return "", "", ""

    company_name = _extract_company_name(tree)
    ceo = _extract_first_ceo(_extract_detail_value(tree, "대표자명"))
    homepage = _extract_detail_link(tree, "홈페이지")
    if homepage:
        return ceo, homepage, company_name

    desc = tree.xpath('string(//meta[@name="Description"]/@content)')
    if desc:
        match = re.search(r"홈페이지\s*[:：]\s*([^\s,|]+)", desc)
        if match:
            homepage = _clean_homepage(match.group(1))
    return ceo, homepage, company_name


def crawl_details(
    output_dir: Path,
    max_items: int = 0,
    detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY,
) -> int:
    """Phase 2: 并发爬公司详情，提取 CEO 与官网。"""
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

    logger.info("Saramin 详情爬虫: 待处理 %d 条, 并发=%d", len(pending), detail_concurrency)

    write_lock = threading.Lock()
    written = 0
    failed = 0

    def _worker(raw_record: dict) -> CompanyRecord:
        record = CompanyRecord.from_dict(raw_record)
        client = _get_client()
        csn = record.comp_id.replace("SA_", "", 1)
        detail_html = client.get_company_html(csn)
        ceo, homepage, company_name = _parse_company_html(detail_html)
        if not homepage and raw_record.get("rec_idx"):
            homepage = _clean_homepage(client.get_homepage_from_rec_idx(str(raw_record["rec_idx"])))
        if ceo:
            record.ceo = ceo
        if homepage:
            record.homepage = homepage
        if company_name and not record.company_name:
            record.company_name = company_name
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
                                written, len(pending), pct,
                                result.company_name,
                                result.ceo or "-",
                                result.homepage[:50] if result.homepage else "-",
                            )
                except Exception as exc:
                    failed += 1
                    logger.warning("Saramin 详情失败 (%s): %s", original.get("comp_id", ""), exc)

    finally:
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
            encoding="utf-8",
        )

    logger.info("Saramin 详情完成: 成功 %d 条 | 失败 %d 条", written, failed)
    return written


def run_saramin(argv: list[str]) -> int:
    """saramin.co.kr 全量爬取入口。"""
    parser = argparse.ArgumentParser(description="saramin.co.kr 公司数据爬取")
    parser.add_argument("--max-pages", type=int, default=0, help="列表最大页数")
    parser.add_argument("--max-items", type=int, default=0, help="详情/Snov最大条数")
    parser.add_argument("--skip-list", action="store_true", help="跳过列表阶段")
    parser.add_argument("--skip-detail", action="store_true", help="跳过详情阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过Snov阶段")
    parser.add_argument("--serial", action="store_true", help="串行模式")
    parser.add_argument(
        "--detail-concurrency",
        type=int,
        default=DEFAULT_DETAIL_CONCURRENCY,
        help=f"详情阶段并发数（默认 {DEFAULT_DETAIL_CONCURRENCY}）",
    )
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 官网补齐阶段")
    parser.add_argument("--gmap-concurrency", type=int, default=DEFAULT_GMAP_CONCURRENCY, help="Google Maps 阶段并发数")
    parser.add_argument("--gmap-hl", default="ko", help="Google Maps 语言参数 hl")
    parser.add_argument("--gmap-gl", default="kr", help="Google Maps 地区参数 gl")
    parser.add_argument("--gmap-search-pb", default="", help="Google Maps 搜索 pb 参数")
    parser.add_argument(
        "--snov-concurrency",
        type=int,
        default=5,
        help="Snov阶段并发数（默认 5，建议 1~3 避免 429）",
    )
    parser.add_argument(
        "--snov-delay",
        type=float,
        default=1.0,
        help="Snov每条请求后等待秒数（默认 1.0，过低可能触发 429）",
    )
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args(argv)
    args.detail_concurrency = max(1, args.detail_concurrency)
    args.gmap_concurrency = max(1, args.gmap_concurrency)
    args.snov_concurrency = max(1, args.snov_concurrency)
    args.snov_delay = max(0.0, args.snov_delay)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = ROOT / "output" / "saramin"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== saramin.co.kr 爬虫启动 ===")

    phase1_done = threading.Event()
    phase2_done = threading.Event()
    if args.skip_list:
        phase1_done.set()
    if args.skip_detail:
        phase2_done.set()

    def _phase1() -> None:
        try:
            crawl_list(output_dir=output_dir, max_pages=args.max_pages)
        finally:
            phase1_done.set()

    def _phase2() -> None:
        ids_file = output_dir / "company_ids.jsonl"
        total_processed = 0
        while not ids_file.exists() or _count_lines(ids_file) == 0:
            if phase1_done.is_set():
                break
            time.sleep(POLL_INTERVAL)
        try:
            while True:
                count = crawl_details(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    detail_concurrency=args.detail_concurrency,
                )
                total_processed += count
                if args.max_items > 0 and total_processed >= args.max_items:
                    break
                if phase1_done.is_set():
                    final = crawl_details(
                        output_dir=output_dir,
                        max_items=args.max_items,
                        detail_concurrency=args.detail_concurrency,
                    )
                    total_processed += final
                    break
                time.sleep(POLL_INTERVAL)
        except Exception as exc:
            logger.error("Phase 2 异常: %s", exc)
        finally:
            phase2_done.set()
            logger.info("Phase 2 总计处理: %d 条", total_processed)

    def _phase3() -> None:
        companies_file = output_dir / "companies.jsonl"
        total_found = 0
        while not companies_file.exists() or _count_lines(companies_file) == 0:
            if phase2_done.is_set():
                break
            time.sleep(POLL_INTERVAL)
        try:
            while True:
                count = run_snov_pipeline(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=args.snov_concurrency,
                    request_delay=args.snov_delay,
                )
                total_found += count
                if args.max_items > 0 and total_found >= args.max_items:
                    break
                if phase2_done.is_set():
                    final = run_snov_pipeline(
                        output_dir=output_dir,
                        max_items=args.max_items,
                        concurrency=args.snov_concurrency,
                        request_delay=args.snov_delay,
                    )
                    total_found += final
                    break
                time.sleep(POLL_INTERVAL)
        except Exception as exc:
            logger.error("Phase 3 异常: %s", exc)
        finally:
            logger.info("Phase 3 总计找到邮箱: %d 条", total_found)

    try:
        if args.serial:
            if not args.skip_list:
                crawl_list(output_dir=output_dir, max_pages=args.max_pages)
            if not args.skip_detail:
                crawl_details(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    detail_concurrency=args.detail_concurrency,
                )
            if not args.skip_snov:
                run_snov_pipeline(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=args.snov_concurrency,
                    request_delay=args.snov_delay,
                )
        else:
            threads: list[threading.Thread] = []
            if not args.skip_list:
                t1 = threading.Thread(target=_phase1, name="Phase1-List", daemon=True)
                threads.append(t1)
                t1.start()
            if not args.skip_detail:
                t2 = threading.Thread(target=_phase2, name="Phase2-Detail", daemon=True)
                threads.append(t2)
                t2.start()
            if not args.skip_snov:
                t3 = threading.Thread(target=_phase3, name="Phase3-Snov", daemon=True)
                threads.append(t3)
                t3.start()
            for t in threads:
                t.join()

        _run_gmap_and_incremental_snov(output_dir, args)

        final_file = output_dir / "companies_with_emails.jsonl"
        if final_file.exists():
            logger.info("--- 域名去重 ---")
            deduped = deduplicate_by_domain(final_file)
            logger.info("去重完成: %d 条", deduped)

    except KeyboardInterrupt:
        logger.warning("用户中断，已保存断点。")
        return 130

    logger.info("=== saramin.co.kr 爬虫完毕 ===")
    return 0
