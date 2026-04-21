"""Kompass CLI。"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .client import KompassChallengeError
from .pipeline import run_pipeline_list


SITE_ROOT = Path(__file__).resolve().parents[4]


def run_site(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Kompass 德国官网列表采集")
    parser.add_argument("mode", nargs="?", default="list", choices=["list"])
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--proxy", type=str, default="")
    parser.add_argument("--max-pages", type=int, default=0)
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
            output_dir=SITE_ROOT / "output" / "kompass",
            request_delay=args.delay,
            concurrency=args.concurrency,
            proxy=args.proxy,
            max_pages=args.max_pages,
        )
        print(f"\n完成: {{'pipeline1_list': {result}}}")
        return 0
    except KompassChallengeError as exc:
        print(f"Kompass 暂停：{exc}")
        return 1
