"""bizok.incheon.go.kr 爬虫入口 — 三阶段: 列表→详情→Snov。

列表页: GET /platform/ofc/ofcList.do?pgno=N (每页9条, 共2761页, 24847家)
详情页: GET /platform/ofc/ofcDetail.do?ofc_key=N (含官网URL)
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from lxml import html as lxml_html

from korea_crawler.incheon_client import IncheonClient, RateLimitConfig
from korea_crawler.models import CompanyRecord
from korea_crawler.google_maps.pipeline import (
    DEFAULT_GMAP_CONCURRENCY,
    merge_incremental_results_into_standard,
    run_gmap_stream,
    run_incremental_snov_from_queue,
)
from korea_crawler.snov.pipeline import run_snov_pipeline
from korea_crawler.dedup import deduplicate_by_domain

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent  # South Korea/

# ──────────── 列表页解析 ────────────

_thread_local = threading.local()


def _get_client() -> IncheonClient:
    if not hasattr(_thread_local, "client"):
        rate_config = RateLimitConfig(
            min_delay=0.15, max_delay=0.5,
            long_rest_interval=500, long_rest_seconds=5.0,
        )
        _thread_local.client = IncheonClient(rate_config=rate_config)
    return _thread_local.client


def _parse_list_page(html_text: str) -> list[dict]:
    """从列表页 HTML 提取公司基础信息。"""
    results: list[dict] = []
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return results

    # 1) 提取公司名 + ofc_key：从 <a href="ofcDetail..."> 里取
    name_map: dict[str, str] = {}  # ofc_key -> company_name
    for a_el in tree.xpath('//a[contains(@href, "ofcDetail.do")]'):
        href = a_el.get("href", "")
        m = re.search(r'ofc_key=(\d+)', href)
        if not m:
            continue
        key = m.group(1)
        text = (a_el.text_content() or "").strip()
        # 跳过 "기업 정보 더보기"、图片名、空文本
        if (not text or text == "기업 정보 더보기"
                or text.endswith((".jpg", ".png", ".gif", ".jpeg"))
                or len(text) <= 1):
            continue
        if key not in name_map:
            name_map[key] = text

    # 2) 提取 CEO：从所有文本节点匹配 "대표자명 : XXX"
    ceo_list: list[str] = []
    for text_node in tree.xpath('//text()'):
        t = text_node.strip()
        if t.startswith("대표자명 : "):
            ceo = t.replace("대표자명 : ", "").strip()
            ceo_list.append(ceo)

    # 3) 按出现顺序配对
    keys_ordered = list(name_map.keys())
    for idx, key in enumerate(keys_ordered):
        ceo = ceo_list[idx] if idx < len(ceo_list) else ""
        results.append({
            "ofc_key": key,
            "company_name": name_map[key],
            "ceo": ceo,
        })

    return results


def _parse_detail_html(html_text: str) -> str:
    """从详情页提取官网 URL。

    只认 class="btn-box" 里的 "기업 홈페이지" 链接。
    不使用备选逻辑（之前误将页面 footer 的 mois.go.kr 政府横幅
    当作公司官网，污染了 78% 的数据）。
    """
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return ""

    # 精确匹配：btn-box 内的"홈페이지"链接
    # 注意：文字在 <a><span>기업 홈페이지</span></a> 里，
    # 必须用 contains(., ...) 而非 contains(text(), ...) 才能匹配到 span 内文字
    links = tree.xpath('//p[contains(@class,"btn-box")]//a[contains(., "홈페이지")]/@href')
    if links:
        hp = links[0].strip()
        if hp and hp.startswith("http") and "bizok.incheon.go.kr" not in hp:
            return hp

    # 宽松匹配：任何包含"홈페이지"文字的链接（含嵌套 span）
    links = tree.xpath('//a[contains(., "홈페이지")]/@href')
    if links:
        hp = links[0].strip()
        if hp and hp.startswith("http") and "bizok.incheon.go.kr" not in hp:
            return hp

    return ""


# ──────────── Phase 1: 列表爬取 ────────────

def crawl_list(output_dir: Path, max_pages: int = 0) -> int:
    """分页爬取列表，输出 company_ids.jsonl。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / "checkpoint_list.json"

    last_page = 0
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        last_page = data.get("last_page", 0)

    current_page = last_page + 1
    total_written = 0
    mode = "a" if last_page > 0 else "w"

    client = _get_client()
    end_page = max_pages if max_pages > 0 else 2761  # 绝对页码上限

    logger.info("仁川列表爬虫: 从第 %d 页开始, 上限 %d 页", current_page, end_page)

    try:
        with output_file.open(mode, encoding="utf-8") as fp:
            while current_page <= end_page:
                html = client.get_html(f"/platform/ofc/ofcList.do?pgno={current_page}")
                companies = _parse_list_page(html)

                if not companies:
                    logger.info("第 %d 页无数据，爬取完毕", current_page)
                    break

                for comp in companies:
                    record = CompanyRecord(
                        comp_id=f"IC_{comp['ofc_key']}",
                        company_name=comp["company_name"],
                        ceo=comp["ceo"],
                    )
                    fp.write(record.to_json_line() + "\n")
                    total_written += 1

                fp.flush()
                checkpoint_file.write_text(
                    json.dumps({"last_page": current_page}, ensure_ascii=False),
                    encoding="utf-8",
                )

                pct = current_page / end_page * 100
                if current_page <= 3 or current_page % 20 == 0:
                    logger.info(
                        "第 %d/%d 页 [%d条] | 累计 %d | %.1f%%",
                        current_page, end_page, len(companies), total_written, pct,
                    )

                current_page += 1

    except Exception:
        checkpoint_file.write_text(
            json.dumps({"last_page": current_page - 1}, ensure_ascii=False),
            encoding="utf-8",
        )
        raise

    logger.info("仁川列表完成: %d 条", total_written)
    return total_written


# ──────────── Phase 2: 详情爬取（取官网） ────────────

DEFAULT_DETAIL_CONCURRENCY = 4


def crawl_details(
    output_dir: Path,
    max_items: int = 0,
    detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY,
) -> int:
    """并发爬取详情页，提取官网，输出 companies.jsonl。"""
    ids_file = output_dir / "company_ids.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint_detail.json"

    if not ids_file.exists():
        return 0

    records: list[CompanyRecord] = []
    with ids_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(CompanyRecord.from_dict(json.loads(line)))

    # 兼容异常中断场景：输出文件里已写成功、但 checkpoint 尚未来得及落盘时，
    # 下次启动会重复写入。这里把 companies.jsonl 的 comp_id 也并入已处理集合。
    existing_output_ids: set[str] = set()
    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    existing_output_ids.add(json.loads(line).get("comp_id", ""))
                except Exception:
                    continue

    processed = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed = set(data.get("processed_ids", []))
    processed.update(x for x in existing_output_ids if x)

    pending = [r for r in records if r.comp_id not in processed]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        return 0

    logger.info("仁川详情爬虫: 待处理 %d 条, 并发=%d", len(pending), detail_concurrency)

    write_lock = threading.Lock()
    written = 0
    failed = 0

    def _worker(record: CompanyRecord) -> CompanyRecord:
        client = _get_client()
        ofc_key = record.comp_id.replace("IC_", "")
        html = client.get_html(f"/platform/ofc/ofcDetail.do?ofc_key={ofc_key}")
        record.homepage = _parse_detail_html(html)
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
                        processed.add(result.comp_id)
                        written += 1

                        if written % 50 == 0:
                            checkpoint_file.write_text(
                                json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
                                encoding="utf-8",
                            )

                        if written <= 5 or written % 50 == 0:
                            pct = written / len(pending) * 100
                            logger.info(
                                "[%d/%d] %.1f%% %s | CEO=%s | HP=%s",
                                written, len(pending), pct,
                                result.company_name,
                                result.ceo or "-",
                                result.homepage[:40] if result.homepage else "-",
                            )
                except Exception as exc:
                    failed += 1
                    logger.warning("详情失败 (%s): %s", original.comp_id, exc)

    finally:
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
            encoding="utf-8",
        )

    logger.info("仁川详情完成: 成功 %d 条 | 失败 %d 条", written, failed)
    return written


# ──────────── 入口 ────────────

# 流水线轮询间隔（秒）
POLL_INTERVAL = 10


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
            site_label="Incheon",
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
                site_label="Incheon",
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


def _count_lines(filepath: Path) -> int:
    """快速统计文件行数。"""
    if not filepath.exists():
        return 0
    with filepath.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp)


def run_incheon(argv: list[str]) -> int:
    """bizok.incheon.go.kr 全量爬取入口 — 三阶段流水线并行。"""
    parser = argparse.ArgumentParser(description="仁川企业信息爬取")
    parser.add_argument("--max-pages", type=int, default=0, help="列表最大页数")
    parser.add_argument("--max-items", type=int, default=0, help="详情/Snov最大条数")
    parser.add_argument("--skip-list", action="store_true", help="跳过列表阶段")
    parser.add_argument("--skip-detail", action="store_true", help="跳过详情阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过Snov阶段")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 官网补齐阶段")
    parser.add_argument("--gmap-concurrency", type=int, default=DEFAULT_GMAP_CONCURRENCY, help="Google Maps 阶段并发数")
    parser.add_argument("--gmap-hl", default="ko", help="Google Maps 语言参数 hl")
    parser.add_argument("--gmap-gl", default="kr", help="Google Maps 地区参数 gl")
    parser.add_argument("--gmap-search-pb", default="", help="Google Maps 搜索 pb 参数")
    parser.add_argument("--snov-concurrency", type=int, default=2, help="Snov 阶段并发数")
    parser.add_argument("--snov-delay", type=float, default=1.0, help="Snov 单条查询后等待秒数")
    parser.add_argument("--serial", action="store_true", help="串行模式")
    parser.add_argument(
        "--detail-concurrency",
        type=int,
        default=DEFAULT_DETAIL_CONCURRENCY,
        help=f"详情阶段并发数（默认 {DEFAULT_DETAIL_CONCURRENCY}）",
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

    output_dir = ROOT / "output" / "incheon"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== bizok.incheon.go.kr 爬虫启动 ===")

    phase1_done = threading.Event()
    phase2_done = threading.Event()
    if args.skip_list:
        phase1_done.set()
    if args.skip_detail:
        phase2_done.set()

    # ── Phase 1: 列表爬取 ──
    def _phase1(out, max_pg, event):
        try:
            crawl_list(output_dir=out, max_pages=max_pg)
        finally:
            event.set()

    # ── Phase 2: 详情爬取（流水线，边等 Phase 1 边解析） ──
    def _phase2(out, max_it, wait_event, done_event, detail_concurrency):
        ids_file = out / "company_ids.jsonl"
        total_processed = 0

        # 等 Phase 1 产生第一条数据
        while not ids_file.exists() or _count_lines(ids_file) == 0:
            if wait_event.is_set():
                break
            time.sleep(POLL_INTERVAL)

        try:
            while True:
                count = crawl_details(
                    output_dir=out,
                    max_items=max_it,
                    detail_concurrency=detail_concurrency,
                )
                total_processed += count

                if max_it > 0 and total_processed >= max_it:
                    break

                # Phase 1 完成了就最后再跑一轮然后退出
                if wait_event.is_set():
                    final = crawl_details(
                        output_dir=out,
                        max_items=max_it,
                        detail_concurrency=detail_concurrency,
                    )
                    total_processed += final
                    break

                # 还有新数据没处理就继续，没有就等一等
                time.sleep(POLL_INTERVAL)

        except Exception as exc:
            logger.error("Phase 2 异常: %s", exc)
        finally:
            done_event.set()
            logger.info("Phase 2 总计处理: %d 条", total_processed)

    # ── Phase 3: Snov 邮箱（流水线，边等 Phase 2 边查邮箱） ──
    def _phase3(out, max_it, wait_event):
        companies_file = out / "companies.jsonl"
        total_found = 0

        while not companies_file.exists() or _count_lines(companies_file) == 0:
            if wait_event.is_set():
                break
            time.sleep(POLL_INTERVAL)

        try:
            while True:
                count = run_snov_pipeline(output_dir=out, max_items=max_it, concurrency=args.snov_concurrency, request_delay=args.snov_delay)
                total_found += count

                if max_it > 0 and total_found >= max_it:
                    break

                if wait_event.is_set():
                    final = run_snov_pipeline(output_dir=out, max_items=max_it, concurrency=args.snov_concurrency, request_delay=args.snov_delay)
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
                run_snov_pipeline(output_dir=output_dir, max_items=args.max_items, concurrency=args.snov_concurrency, request_delay=args.snov_delay)
        else:
            threads: list[threading.Thread] = []

            if not args.skip_list:
                t1 = threading.Thread(
                    target=_phase1,
                    args=(output_dir, args.max_pages, phase1_done),
                    name="Phase1-List", daemon=True,
                )
                threads.append(t1)
                t1.start()

            if not args.skip_detail:
                t2 = threading.Thread(
                    target=_phase2,
                    args=(
                        output_dir,
                        args.max_items,
                        phase1_done,
                        phase2_done,
                        args.detail_concurrency,
                    ),
                    name="Phase2-Detail", daemon=True,
                )
                threads.append(t2)
                t2.start()

            if not args.skip_snov:
                t3 = threading.Thread(
                    target=_phase3,
                    args=(output_dir, args.max_items, phase2_done),
                    name="Phase3-Snov", daemon=True,
                )
                threads.append(t3)
                t3.start()

            for t in threads:
                t.join()

        _run_gmap_and_incremental_snov(output_dir, args)

        # 域名去重
        final_file = output_dir / "companies_with_emails.jsonl"
        if final_file.exists():
            logger.info("--- 域名去重 ---")
            deduped = deduplicate_by_domain(final_file)
            logger.info("去重完成: %d 条", deduped)

    except KeyboardInterrupt:
        logger.warning("用户中断，已保存断点。")
        return 130

    logger.info("=== bizok.incheon.go.kr 爬虫完毕 ===")
    return 0
