"""catch.co.kr 站点执行入口 — 四阶段流水线并行，支持断点续跑和域名去重。"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from pathlib import Path

from korea_crawler.detail_parser import crawl_details
from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.list_crawler import crawl_list
from korea_crawler.sites.catch_gmap import (
    DEFAULT_GMAP_CONCURRENCY,
    merge_incremental_results_into_standard,
    run_gmap_stream,
    run_incremental_snov_from_queue,
)
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent
POLL_INTERVAL = 10


def _count_lines(filepath: Path) -> int:
    """快速统计文件行数。"""
    if not filepath.exists():
        return 0
    count = 0
    with filepath.open("r", encoding="utf-8") as fp:
        for line in fp:
            if line.strip():
                count += 1
    return count


def _phase1_list(output_dir: Path, max_pages: int, done_event: threading.Event) -> None:
    """Phase 1: 列表 API。"""
    try:
        count = crawl_list(output_dir=output_dir, max_pages=max_pages)
        logger.info("Phase 1 完成: %d 条", count)
    except Exception as exc:
        logger.error("Phase 1 异常: %s", exc)
    finally:
        done_event.set()


def _phase2_detail(
    output_dir: Path,
    max_items: int,
    phase1_done: threading.Event,
    phase2_done: threading.Event,
) -> None:
    """Phase 2: 详情页解析。"""
    ids_file = output_dir / "company_ids.jsonl"
    total_processed = 0

    while not ids_file.exists() or _count_lines(ids_file) == 0:
        if phase1_done.is_set():
            break
        time.sleep(POLL_INTERVAL)

    try:
        while True:
            count = crawl_details(output_dir=output_dir, max_items=max_items)
            total_processed += count

            if max_items > 0 and total_processed >= max_items:
                break
            if phase1_done.is_set():
                final = crawl_details(output_dir=output_dir, max_items=max_items)
                total_processed += final
                break
            time.sleep(POLL_INTERVAL)
    except Exception as exc:
        logger.error("Phase 2 异常: %s", exc)
    finally:
        phase2_done.set()
        logger.info("Phase 2 总计处理: %d 条", total_processed)


def _phase3_snov(
    output_dir: Path,
    max_items: int,
    phase2_done: threading.Event,
    snov_concurrency: int,
    snov_delay: float,
) -> None:
    """Phase 3: 原始官网走 Snov。"""
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
                max_items=max_items,
                concurrency=snov_concurrency,
                request_delay=snov_delay,
            )
            total_found += count

            if max_items > 0 and total_found >= max_items:
                break
            if phase2_done.is_set():
                final = run_snov_pipeline(
                    output_dir=output_dir,
                    max_items=max_items,
                    concurrency=snov_concurrency,
                    request_delay=snov_delay,
                )
                total_found += final
                break
            time.sleep(POLL_INTERVAL)
    except Exception as exc:
        logger.error("Phase 3 异常: %s", exc)
    finally:
        logger.info("Phase 3 总计找到邮箱: %d 条", total_found)


def _run_gmap_and_incremental_snov(output_dir: Path, args: argparse.Namespace) -> None:
    """Phase 3.5-3.6: GMap 补官网并实时喂增量 Snov。"""
    if args.skip_gmap:
        return

    gmap_done_event = threading.Event()

    def _phase35() -> None:
        try:
            logger.info("--- Phase 3.5: Google Maps 官网补齐 ---")
            gmap_processed, gmap_found = run_gmap_stream(
                output_dir=output_dir,
                max_items=args.max_items,
                gmap_concurrency=args.gmap_concurrency,
                gmap_search_pb=args.gmap_search_pb,
                gmap_hl=args.gmap_hl,
                gmap_gl=args.gmap_gl,
            )
            logger.info("Phase 3.5 完成: 处理 %d 条 | 新官网 %d 条", gmap_processed, gmap_found)
        finally:
            gmap_done_event.set()

    threads: list[threading.Thread] = []
    t35 = threading.Thread(target=_phase35, name="Phase35-GMap", daemon=True)
    threads.append(t35)
    t35.start()

    if not args.skip_snov:
        def _phase36() -> None:
            logger.info("--- Phase 3.6: 增量 Snov（边补官网边查询）---")
            run_incremental_snov_from_queue(
                output_dir=output_dir,
                max_items=args.max_items,
                snov_concurrency=args.snov_concurrency,
                snov_delay=args.snov_delay,
                gmap_done_event=gmap_done_event,
            )
            updated = merge_incremental_results_into_standard(output_dir)
            logger.info("Phase 3.6 合并回主文件: %d 处更新", updated)

        t36 = threading.Thread(target=_phase36, name="Phase36-SnovGMap", daemon=True)
        threads.append(t36)
        t36.start()

    for thread in threads:
        thread.join()


def run_catch(argv: list[str]) -> int:
    """catch.co.kr 全量爬取入口。"""
    parser = argparse.ArgumentParser(description="catch.co.kr 全量爬取")
    parser.add_argument("--max-pages", type=int, default=0, help="列表最大页数")
    parser.add_argument("--max-items", type=int, default=0, help="详情/GMap/Snov 最大条数")
    parser.add_argument("--skip-list", action="store_true", help="跳过列表阶段")
    parser.add_argument("--skip-detail", action="store_true", help="跳过详情阶段")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 官网补齐阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过 Snov 阶段")
    parser.add_argument("--gmap-concurrency", type=int, default=DEFAULT_GMAP_CONCURRENCY, help="Google Maps 阶段并发数")
    parser.add_argument("--gmap-hl", default="ko", help="Google Maps 语言参数 hl")
    parser.add_argument("--gmap-gl", default="kr", help="Google Maps 地区参数 gl")
    parser.add_argument("--gmap-search-pb", default="", help="Google Maps 搜索 pb 参数")
    parser.add_argument("--snov-concurrency", type=int, default=2, help="Snov 阶段并发数")
    parser.add_argument("--snov-delay", type=float, default=1.0, help="Snov 单条查询后等待秒数")
    parser.add_argument("--skip-email-agent", action="store_true", default=True, help="跳过 Firecrawl+LLM 邮箱补全（韩国默认跳过）")
    parser.add_argument("--run-email-agent", action="store_true", help="强制启用 Firecrawl+LLM 邮箱补全")
    parser.add_argument("--serial", action="store_true", help="串行模式（默认并行）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args(argv)
    args.gmap_concurrency = max(1, args.gmap_concurrency)
    args.snov_concurrency = max(1, args.snov_concurrency)
    args.snov_delay = max(0.0, args.snov_delay)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = ROOT / "output" / "catch"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== catch.co.kr 协议爬虫启动 (流水线模式) ===")
    logger.info("输出目录: %s", output_dir)

    phase1_done = threading.Event()
    phase2_done = threading.Event()
    if args.skip_list:
        phase1_done.set()
    if args.skip_detail:
        phase2_done.set()

    try:
        if args.serial:
            if not args.skip_list:
                crawl_list(output_dir=output_dir, max_pages=args.max_pages)
            phase1_done.set()
            if not args.skip_detail:
                crawl_details(output_dir=output_dir, max_items=args.max_items)
            phase2_done.set()
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
                t1 = threading.Thread(
                    target=_phase1_list,
                    args=(output_dir, args.max_pages, phase1_done),
                    name="Phase1-List",
                    daemon=True,
                )
                threads.append(t1)
                t1.start()
            else:
                logger.info("跳过 Phase 1")

            if not args.skip_detail:
                t2 = threading.Thread(
                    target=_phase2_detail,
                    args=(output_dir, args.max_items, phase1_done, phase2_done),
                    name="Phase2-Detail",
                    daemon=True,
                )
                threads.append(t2)
                t2.start()
            else:
                logger.info("跳过 Phase 2")

            if not args.skip_snov:
                t3 = threading.Thread(
                    target=_phase3_snov,
                    args=(output_dir, args.max_items, phase2_done, args.snov_concurrency, args.snov_delay),
                    name="Phase3-Snov",
                    daemon=True,
                )
                threads.append(t3)
                t3.start()
            else:
                logger.info("跳过 Phase 3")

            for thread in threads:
                thread.join()

        _run_gmap_and_incremental_snov(output_dir, args)

        if args.run_email_agent:
            from korea_crawler.email_agent.pipeline import run_email_agent_pipeline
            logger.info("--- Phase 4: Firecrawl+LLM 邮箱补全 ---")
            run_email_agent_pipeline(output_dir=output_dir, project_root=ROOT)

        final_file = output_dir / "companies_with_emails.jsonl"
        if final_file.exists():
            logger.info("--- 域名去重 ---")
            deduped = deduplicate_by_domain(final_file)
            logger.info("去重完成: %d 条唯一记录", deduped)
    except KeyboardInterrupt:
        logger.warning("用户中断，已保存断点。下次 python run.py catch 自动续跑。")
        return 130

    logger.info("=== catch.co.kr 爬虫完毕 ===")
    return 0
