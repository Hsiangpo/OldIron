"""DART Open API 爬虫入口 — 三阶段流水线: corpCode下载→企业概况→Snov邮箱。

Phase 1: 下载全量 corpCode XML → corp_codes.jsonl（一次性，含所有公司编号）
Phase 2: 用多 Key 轮询查 company.json → companies.jsonl（公司名 + CEO + 官网）
Phase 3: 官网域名给 Snov 查邮箱 → companies_with_emails.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from korea_crawler.dart_client import (
    DartClient,
    DartKeyExhaustedError,
    DartKeyPool,
)
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

ROOT = Path(__file__).resolve().parent.parent.parent.parent

# 流水线轮询间隔（秒）
POLL_INTERVAL = 10
# Phase 2 并发数（受 API 限速约束，不宜过高）
DETAIL_CONCURRENCY = 5


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
            site_label="DART",
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
                site_label="DART",
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
    count = 0
    with filepath.open("r", encoding="utf-8") as fp:
        for line in fp:
            if line.strip():
                count += 1
    return count


# ──────────── Phase 1: 下载 corpCode ────────────

def _download_corp_codes(output_dir: Path, client: DartClient) -> int:
    """Phase 1: 下载全量公司编号，输出 corp_codes.jsonl。"""
    output_file = output_dir / "corp_codes.jsonl"

    # 已有 corpCode 且行数大于 0 则跳过
    if output_file.exists() and _count_lines(output_file) > 0:
        count = _count_lines(output_file)
        logger.info("Phase 1 跳过: corp_codes.jsonl 已存在 (%d 条)", count)
        return count

    corps = client.download_corp_codes(output_file)
    logger.info("Phase 1 完成: %d 家公司编号", len(corps))
    return len(corps)


# ──────────── Phase 2: 查询企业概况 ────────────

def _crawl_company_info(
    output_dir: Path,
    key_pool: DartKeyPool,
    max_items: int = 0,
) -> int:
    """
    Phase 2: 并发查询企业概况，输出 companies.jsonl。

    支持断点续跑 — 记录已处理的 corp_code。
    """
    input_file = output_dir / "corp_codes.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint_detail.json"

    if not input_file.exists():
        logger.warning("Phase 2: corp_codes.jsonl 不存在，请先运行 Phase 1")
        return 0

    # 断点恢复
    processed_codes: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed_codes = set(data.get("processed_codes", []))

    # 加载待处理
    all_corps: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                all_corps.append(json.loads(line))

    pending = [c for c in all_corps if c["corp_code"] not in processed_codes]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        logger.info("Phase 2: 无待处理数据（已全部完成）")
        return 0

    logger.info("Phase 2: 待处理 %d / %d 条，并发=%d",
                len(pending), len(all_corps), DETAIL_CONCURRENCY)
    logger.info("Key Pool 配额:\n%s", key_pool.summary())

    # 每个线程独立的 DartClient
    thread_local = threading.local()

    def _get_client() -> DartClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = DartClient(key_pool)
        return thread_local.client

    def _query_one(corp: dict) -> tuple[dict, dict | None]:
        """查询单个公司，返回 (corp, result_or_none)。"""
        client = _get_client()
        result = client.get_company_info(corp["corp_code"])
        return corp, result

    processed_count = 0
    found_count = 0
    no_data_count = 0
    write_lock = threading.Lock()

    try:
        with (
            output_file.open("a", encoding="utf-8") as fp,
            ThreadPoolExecutor(max_workers=DETAIL_CONCURRENCY) as executor,
        ):
            futures = {executor.submit(_query_one, c): c for c in pending}

            for fut in as_completed(futures):
                try:
                    corp, result = fut.result()
                    corp_code = corp["corp_code"]

                    with write_lock:
                        processed_codes.add(corp_code)
                        processed_count += 1

                        if result and result.get("status") == "000":
                            record = CompanyRecord(
                                comp_id=corp_code,
                                company_name=result.get("corp_name", ""),
                                ceo=result.get("ceo_nm", ""),
                                homepage=result.get("hm_url", ""),
                            )
                            fp.write(record.to_json_line() + "\n")
                            fp.flush()
                            found_count += 1

                            if record.homepage:
                                logger.debug(
                                    "[%d] %s | CEO=%s | %s",
                                    processed_count,
                                    record.company_name,
                                    record.ceo,
                                    record.homepage,
                                )
                        else:
                            no_data_count += 1

                        # 定期保存断点
                        if processed_count % 100 == 0:
                            checkpoint_file.write_text(
                                json.dumps(
                                    {"processed_codes": sorted(processed_codes)},
                                    ensure_ascii=False,
                                ),
                                encoding="utf-8",
                            )
                            remaining = key_pool.total_remaining()
                            logger.info(
                                "Phase 2 进度: %d/%d | 有数据: %d | 无数据: %d | Key剩余配额: ~%d",
                                processed_count, len(pending),
                                found_count, no_data_count, remaining,
                            )

                except DartKeyExhaustedError:
                    logger.error("所有 DART Key 已用尽当日配额，保存断点后停止")
                    # 取消剩余任务
                    for f in futures:
                        f.cancel()
                    break
                except Exception as exc:
                    logger.warning("Phase 2 worker 异常: %s", exc)

    except DartKeyExhaustedError:
        logger.error("DART Key 配额耗尽，Phase 2 中断。明天可续跑。")
    finally:
        # 最终保存断点
        checkpoint_file.write_text(
            json.dumps(
                {"processed_codes": sorted(processed_codes)},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logger.info(
            "Phase 2 本轮: 处理 %d | 有数据 %d | 无数据 %d",
            processed_count, found_count, no_data_count,
        )
        logger.info("Key 使用统计:\n%s", key_pool.summary())

    return found_count


# ──────────── 入口 ────────────

def run_dart(argv: list[str]) -> int:
    """DART Open API 全量爬取入口 — 三阶段流水线。"""
    parser = argparse.ArgumentParser(description="DART Open API 全量爬取")
    parser.add_argument("--max-items", type=int, default=0, help="详情/Snov最大条数")
    parser.add_argument("--skip-download", action="store_true", help="跳过 corpCode 下载")
    parser.add_argument("--skip-detail", action="store_true", help="跳过企业概况查询")
    parser.add_argument("--skip-snov", action="store_true", help="跳过 Snov 邮箱")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 官网补齐阶段")
    parser.add_argument("--gmap-concurrency", type=int, default=DEFAULT_GMAP_CONCURRENCY, help="Google Maps 阶段并发数")
    parser.add_argument("--gmap-hl", default="ko", help="Google Maps 语言参数 hl")
    parser.add_argument("--gmap-gl", default="kr", help="Google Maps 地区参数 gl")
    parser.add_argument("--gmap-search-pb", default="", help="Google Maps 搜索 pb 参数")
    parser.add_argument("--snov-concurrency", type=int, default=2, help="Snov 阶段并发数")
    parser.add_argument("--snov-delay", type=float, default=1.0, help="Snov 单条查询后等待秒数")
    parser.add_argument("--serial", action="store_true", help="串行模式（默认并行流水线）")
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

    # 加载 DART API Keys
    dart_keys: list[str] = []
    for i in range(1, 6):
        key = os.getenv(f"DART_API_KEY_{i}", "").strip()
        if key:
            dart_keys.append(key)

    if not dart_keys:
        logger.error("缺少 DART API Key。请在 .env 中配置 DART_API_KEY_1 ~ DART_API_KEY_5")
        return 1

    key_pool = DartKeyPool.from_env_keys(dart_keys)

    output_dir = ROOT / "output" / "dart"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== DART Open API 爬虫启动 ===")
    logger.info("输出目录: %s", output_dir)
    logger.info("API Key 数量: %d", len(dart_keys))

    phase1_done = threading.Event()
    phase2_done = threading.Event()

    if args.skip_download:
        phase1_done.set()
    if args.skip_detail:
        phase2_done.set()

    try:
        if args.serial:
            # 串行模式
            if not args.skip_download:
                client = DartClient(key_pool)
                _download_corp_codes(output_dir, client)
            phase1_done.set()

            if not args.skip_detail:
                _crawl_company_info(output_dir, key_pool, args.max_items)
            phase2_done.set()

            if not args.skip_snov:
                run_snov_pipeline(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=args.snov_concurrency,
                    request_delay=args.snov_delay,
                )
        else:
            # 并行流水线
            threads: list[threading.Thread] = []

            # Phase 1: 下载 corpCode（快速，一般几秒就完）
            if not args.skip_download:
                client = DartClient(key_pool)
                _download_corp_codes(output_dir, client)
            phase1_done.set()

            # Phase 2: 企业概况（在线程中运行）
            if not args.skip_detail:
                def _phase2():
                    try:
                        _crawl_company_info(output_dir, key_pool, args.max_items)
                    except Exception as exc:
                        logger.error("Phase 2 异常: %s", exc)
                    finally:
                        phase2_done.set()

                t2 = threading.Thread(target=_phase2, name="Phase2-Detail", daemon=True)
                threads.append(t2)
                t2.start()
            else:
                logger.info("跳过 Phase 2")

            # Phase 3: Snov 邮箱（流水线轮询）
            if not args.skip_snov:
                def _phase3():
                    companies_file = output_dir / "companies.jsonl"
                    total_found = 0

                    # 等 Phase 2 产出第一条数据
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
                        logger.info("Phase 3 (Snov) 总计找到邮箱: %d 条", total_found)

                t3 = threading.Thread(target=_phase3, name="Phase3-Snov", daemon=True)
                threads.append(t3)
                t3.start()
            else:
                logger.info("跳过 Phase 3")

            for t in threads:
                t.join()

        _run_gmap_and_incremental_snov(output_dir, args)

        # 域名去重
        final_file = output_dir / "companies_with_emails.jsonl"
        if final_file.exists():
            logger.info("--- 域名去重 ---")
            deduped = deduplicate_by_domain(final_file)
            logger.info("去重完成: %d 条唯一记录", deduped)

    except KeyboardInterrupt:
        logger.warning("用户中断，已保存断点。下次 python run.py dart 自动续跑。")
        return 130

    logger.info("=== DART 爬虫完毕 ===")
    return 0
