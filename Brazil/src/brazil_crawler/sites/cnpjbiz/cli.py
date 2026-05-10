"""CNPJ Biz CLI。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import CnpjBizConfig
from .pipeline import run_pipeline_all
from .pipeline import run_pipeline_detail_only
from .pipeline import run_pipeline_list_only


ROOT = Path(__file__).resolve().parents[4]


def run_cnpjbiz(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="CNPJ Biz 巴西企业采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "detail"])
    parser.add_argument("--list-workers", type=int, default=1)
    parser.add_argument("--detail-workers", type=int, default=8)
    parser.add_argument("--max-pages", type=int, default=0, help="最多抓多少个列表页（0=不限）")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    output_dir = ROOT / "output" / "cnpjbiz"
    output_dir.mkdir(parents=True, exist_ok=True)
    config = CnpjBizConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        list_workers=args.list_workers,
        detail_workers=args.detail_workers,
        max_pages=args.max_pages,
    )
    if args.mode == "list":
        print(run_pipeline_list_only(config=config))
        return 0
    if args.mode == "detail":
        print(run_pipeline_detail_only(config=config))
        return 0
    print(run_pipeline_all(config=config))
    return 0
