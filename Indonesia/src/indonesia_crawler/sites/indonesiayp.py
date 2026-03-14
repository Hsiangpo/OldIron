"""indonesiayp 站点入口：列表 → 详情 → AHU 法人 → Snov 邮箱。"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from pathlib import Path

from indonesia_crawler.ahu.client import AhuClient, AhuRateLimitError, CaptchaSolveError, normalize_ahu_query_name
from indonesia_crawler.dedup import deduplicate
from indonesia_crawler.indonesiayp.crawler import crawl_company_details, crawl_company_list
from indonesia_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
POLL_INTERVAL = 8


def _choose_best_search_result(query_name: str, results: list) -> object | None:
    """优先匹配公司名最接近的结果。"""
    if not results:
        return None

    target = "".join(ch for ch in query_name.upper() if ch.isalnum())
    for result in results:
        name = "".join(ch for ch in result.nama_korporasi.upper() if ch.isalnum())
        if target and target in name:
            return result
    return results[0]


def _load_processed_ids(checkpoint_file: Path, output_file: Path) -> set[str]:
    """读取并合并已处理记录。"""
    processed: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed = set(data.get("processed_ids", []))

    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    comp_id = str(json.loads(line).get("comp_id", "")).strip()
                except json.JSONDecodeError:
                    continue
                if comp_id:
                    processed.add(comp_id)
    return processed


def _count_lines(filepath: Path) -> int:
    """统计文件行数。"""
    if not filepath.exists():
        return 0
    with filepath.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp if _.strip())


def _ahu_enabled() -> bool:
    """检查 AHU 阶段是否具备运行条件。"""
    return bool(os.getenv("TWO_CAPTCHA_API_KEY", "").strip())


def enrich_ceo_with_ahu(output_dir: Path, max_items: int = 0) -> int:
    """使用 AHU 补全法人字段，输出 `companies_with_ceo.jsonl`。"""
    input_file = output_dir / "companies.jsonl"
    output_file = output_dir / "companies_with_ceo.jsonl"
    checkpoint_file = output_dir / "checkpoint_ahu.json"
    if not input_file.exists():
        logger.warning("AHU 阶段缺少输入文件，已跳过")
        return 0

    if not _ahu_enabled():
        logger.warning("缺少 TWO_CAPTCHA_API_KEY，跳过 AHU 法人补全")
        return 0

    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    processed = _load_processed_ids(checkpoint_file, output_file)
    pending = [record for record in records if str(record.get("comp_id", "")).strip() not in processed]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        logger.info("AHU 阶段无待处理记录")
        return 0

    written = 0
    updated = 0
    client = AhuClient()
    try:
        with output_file.open("a", encoding="utf-8") as fp:
            for record in pending:
                comp_id = str(record.get("comp_id", "")).strip()
                query_name = normalize_ahu_query_name(str(record.get("company_name", "")))

                if not query_name:
                    logger.debug("公司名为空，跳过 AHU: %s", comp_id)
                else:
                    try:
                        results = client.search(query_name)
                        selected = _choose_best_search_result(query_name, results)
                        if selected is not None:
                            detail = client.fetch_detail(selected.detail_id)
                            if detail.pemilik_manfaat:
                                record["ceo"] = detail.pemilik_manfaat[0].title()
                                updated += 1
                    except CaptchaSolveError as exc:
                        logger.warning("AHU 验证码求解失败，阶段提前停止: %s", exc)
                        break
                    except AhuRateLimitError as exc:
                        logger.warning("AHU 触发全局限流，等待 %.1fs 后暂停本轮，保留断点: %s", exc.retry_after, query_name)
                        time.sleep(exc.retry_after)
                        break
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("AHU 查询失败 %s: %s", query_name, exc)

                fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                fp.flush()
                if comp_id:
                    processed.add(comp_id)
                written += 1

                if written % 10 == 0:
                    checkpoint_file.write_text(
                        json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    logger.info("AHU 进度: %d/%d（更新法人 %d）", written, len(pending), updated)
    finally:
        client.close()
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
            encoding="utf-8",
        )

    logger.info("AHU 阶段完成：处理 %d 条，更新法人 %d 条", written, updated)
    return written


def _dedup_final(output_dir: Path) -> int:
    """对最终产物执行公司名去重。"""
    candidates = [
        output_dir / "companies_with_emails.jsonl",
        output_dir / "companies_with_ceo.jsonl",
        output_dir / "companies.jsonl",
    ]
    for file in candidates:
        if file.exists():
            return deduplicate(file)
    logger.warning("未找到可去重文件")
    return 0


def _merge_ahu_ceo_into_snov_output(output_dir: Path) -> None:
    """将 AHU 法人结果回填到 Snov 输出中。"""
    ahu_file = output_dir / "companies_with_ceo.jsonl"
    snov_file = output_dir / "companies_with_emails.jsonl"
    if not ahu_file.exists() or not snov_file.exists():
        return

    ceo_map: dict[str, str] = {}
    with ahu_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            comp_id = str(record.get("comp_id", "")).strip()
            ceo = str(record.get("ceo", "")).strip()
            if comp_id and ceo:
                ceo_map[comp_id] = ceo

    if not ceo_map:
        return

    merged: list[str] = []
    updated = 0
    with snov_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            raw = line.strip()
            if not raw:
                continue
            record = json.loads(raw)
            comp_id = str(record.get("comp_id", "")).strip()
            if comp_id in ceo_map and str(record.get("ceo", "")).strip() != ceo_map[comp_id]:
                record["ceo"] = ceo_map[comp_id]
                updated += 1
            merged.append(json.dumps(record, ensure_ascii=False))

    snov_file.write_text("\n".join(merged) + ("\n" if merged else ""), encoding="utf-8")
    logger.info("AHU 法人回填完成：更新 %d 条", updated)


def _run_serial(args: argparse.Namespace, output_dir: Path) -> None:
    """串行执行（调试模式）。"""
    if not args.skip_list:
        logger.info("--- 阶段1: 列表抓取 ---")
        crawl_company_list(output_dir, max_pages=args.max_pages)

    if not args.skip_detail:
        logger.info("--- 阶段2: 详情抓取 ---")
        crawl_company_details(output_dir, max_items=args.max_items)

    if not args.skip_ahu:
        logger.info("--- 阶段3: AHU 法人补全 ---")
        enrich_ceo_with_ahu(output_dir, max_items=args.max_items)

    if not args.skip_snov:
        logger.info("--- 阶段4: Snov 邮箱补全 ---")
        run_snov_pipeline(output_dir, max_items=args.max_items)

    if not args.skip_ahu and not args.skip_snov:
        _merge_ahu_ceo_into_snov_output(output_dir)


def _run_parallel(args: argparse.Namespace, output_dir: Path) -> None:
    """并行流水线执行（默认）。"""
    list_done = threading.Event()
    detail_done = threading.Event()
    ahu_done = threading.Event()

    if args.skip_list:
        list_done.set()
    if args.skip_detail:
        detail_done.set()
    if args.skip_ahu:
        ahu_done.set()

    def phase_list() -> None:
        try:
            logger.info("--- 阶段1: 列表抓取（并行）---")
            crawl_company_list(output_dir, max_pages=args.max_pages)
        finally:
            list_done.set()

    def phase_detail() -> None:
        ids_file = output_dir / "company_ids.jsonl"
        total = 0
        while _count_lines(ids_file) == 0 and not list_done.is_set():
            time.sleep(POLL_INTERVAL)
        try:
            while True:
                if args.max_items > 0 and total >= args.max_items:
                    break
                remaining = 0 if args.max_items == 0 else max(args.max_items - total, 0)
                count = crawl_company_details(output_dir, max_items=remaining)
                total += count

                if list_done.is_set() and count == 0:
                    break
                time.sleep(POLL_INTERVAL)
        finally:
            logger.info("阶段2结束：详情累计处理 %d 条", total)
            detail_done.set()

    def phase_ahu() -> None:
        input_file = output_dir / "companies.jsonl"
        total = 0
        while _count_lines(input_file) == 0 and not detail_done.is_set():
            time.sleep(POLL_INTERVAL)
        try:
            while True:
                if args.max_items > 0 and total >= args.max_items:
                    break
                remaining = 0 if args.max_items == 0 else max(args.max_items - total, 0)
                count = enrich_ceo_with_ahu(output_dir, max_items=remaining)
                total += count

                if detail_done.is_set() and count == 0:
                    break
                time.sleep(POLL_INTERVAL)
        finally:
            logger.info("阶段3结束：AHU 累计处理 %d 条", total)
            ahu_done.set()

    def phase_snov() -> None:
        input_file = output_dir / "companies.jsonl"
        upstream_done = detail_done
        total = 0

        while _count_lines(input_file) == 0 and not upstream_done.is_set():
            time.sleep(POLL_INTERVAL)
        try:
            while True:
                if args.max_items > 0 and total >= args.max_items:
                    break
                remaining = 0 if args.max_items == 0 else max(args.max_items - total, 0)
                count = run_snov_pipeline(output_dir, max_items=remaining, input_filename="companies.jsonl")
                total += count

                if upstream_done.is_set() and count == 0:
                    break
                time.sleep(POLL_INTERVAL)
        finally:
            logger.info("阶段4结束：Snov 累计处理 %d 条", total)

    threads: list[threading.Thread] = []
    if not args.skip_list:
        threads.append(threading.Thread(target=phase_list, name="Phase1-List", daemon=True))
    if not args.skip_detail:
        threads.append(threading.Thread(target=phase_detail, name="Phase2-Detail", daemon=True))
    if not args.skip_ahu:
        threads.append(threading.Thread(target=phase_ahu, name="Phase3-AHU", daemon=True))
    if not args.skip_snov:
        threads.append(threading.Thread(target=phase_snov, name="Phase4-Snov", daemon=True))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if not args.skip_ahu and not args.skip_snov:
        _merge_ahu_ceo_into_snov_output(output_dir)


def run_indonesiayp(argv: list[str]) -> int:
    """indonesiayp 全流程入口。"""
    parser = argparse.ArgumentParser(description="indonesiayp 爬虫")
    parser.add_argument("--max-pages", type=int, default=0, help="列表最大页数（默认全量）")
    parser.add_argument("--max-items", type=int, default=0, help="详情/AHU/Snov 最大条数（默认全量）")
    parser.add_argument("--skip-list", action="store_true", help="跳过列表阶段")
    parser.add_argument("--skip-detail", action="store_true", help="跳过详情阶段")
    parser.add_argument("--skip-ahu", action="store_true", help="跳过 AHU 法人阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过 Snov 邮箱阶段")
    parser.add_argument("--serial", action="store_true", help="串行执行（默认并行流水线）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = ROOT / "output" / "indonesiayp"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== indonesiayp 爬虫启动 ===")
    if args.serial:
        _run_serial(args, output_dir)
    else:
        logger.info("并行流水线模式已启用：列表→详情→AHU/Snov 流式衔接")
        _run_parallel(args, output_dir)

    logger.info("--- 阶段5: 公司名去重 ---")
    final_count = _dedup_final(output_dir)
    logger.info("去重后记录数: %d", final_count)
    logger.info("=== indonesiayp 全流程完成 ===")
    return 0
