"""Wiza CLI。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from .client import WizaUsageLimitError
from .pipeline import run_pipeline_list


SITE_ROOT = Path(__file__).resolve().parents[4]


def run_site(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Wiza 意大利网站列表采集")
    parser.add_argument("mode", nargs="?", default="list", choices=["list"])
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--proxy", type=str, default="")
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--list-workers", type=int, default=8)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    try:
        result = run_pipeline_list(
            output_dir=SITE_ROOT / "output" / "wiza",
            request_delay=args.delay,
            proxy=args.proxy or os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
            max_pages=args.max_pages,
            concurrency=args.list_workers,
        )
        print(f"\n完成: {{'pipeline1_list': {result}}}")
        return 0
    except WizaUsageLimitError as exc:
        print(f"Wiza 暂停：{exc}")
        print("当前会保留已有登录态和数据库；等 Wiza 恢复后，重跑同一条命令即可续上。")
        return 1
