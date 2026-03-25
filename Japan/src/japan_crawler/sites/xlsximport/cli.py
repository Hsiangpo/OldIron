"""xlsximport CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .pipeline import run_pipeline

logger = logging.getLogger("xlsximport.cli")
PROJECT_ROOT = Path(__file__).resolve().parents[4]  # Japan/


def run_xlsximport(argv: list[str] | None = None) -> int:
    """xlsximport 命令行入口。"""
    parser = argparse.ArgumentParser(description="xlsximport — xlsx 导入 + Protocol+LLM 提取")
    parser.add_argument("--concurrency", type=int, default=32, help="并发线程数 (默认 32)")
    parser.add_argument("--max-items", type=int, default=0, help="最大处理数 (0=全部)")
    parser.add_argument("--xlsx", type=str, default="", help="xlsx 文件路径 (默认 docs/日本.xlsx)")
    args = parser.parse_args(argv or [])

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    output_dir = PROJECT_ROOT / "output" / "xlsximport"
    xlsx_path = Path(args.xlsx) if args.xlsx else None

    logger.info("xlsximport 启动 (并发=%d)", args.concurrency)

    try:
        stats = run_pipeline(
            output_dir=output_dir,
            xlsx_path=xlsx_path,
            concurrency=args.concurrency,
            max_items=args.max_items,
        )
        logger.info("xlsximport 结束: %s", stats)
        return 0
    except KeyboardInterrupt:
        logger.info("用户中断")
        return 0
    except Exception as exc:
        logger.error("xlsximport 异常: %s", exc, exc_info=True)
        return 1
