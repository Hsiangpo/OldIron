"""GAPENSI 站点入口。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ..crawler import crawl_gapensi
from ..dedup import deduplicate

logger = logging.getLogger(__name__)


def run_gapensi(argv: list[str]) -> int:
    """运行 GAPENSI 爬虫全流程。"""
    parser = argparse.ArgumentParser(description="GAPENSI 爬虫")
    parser.add_argument("--max-pages", type=int, default=0, help="最大页数（默认全量）")
    parser.add_argument("--skip-crawl", action="store_true", help="跳过爬取阶段")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    root = Path(__file__).resolve().parents[3]
    output_dir = root / "output" / "gapensi"

    # 阶段 1: 爬取
    if not args.skip_crawl:
        logger.info("=== 阶段 1: 爬取 GAPENSI ===")
        count = crawl_gapensi(output_dir, max_pages=args.max_pages)
        logger.info("爬取完成: %d 条记录", count)
    else:
        logger.info("跳过爬取阶段")

    # 阶段 2: 去重
    companies_file = output_dir / "companies.jsonl"
    if companies_file.exists():
        logger.info("=== 阶段 2: 公司名去重 ===")
        final_count = deduplicate(companies_file)
        logger.info("去重完成: %d 条记录", final_count)
    else:
        logger.warning("未找到 companies.jsonl，跳过去重")

    logger.info("=== 全流程完成 ===")
    return 0
