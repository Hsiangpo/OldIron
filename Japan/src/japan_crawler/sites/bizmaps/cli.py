"""bizmaps CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

SITE_ROOT = Path(__file__).resolve().parents[4]  # Japan/
PROJECT_ROOT = SITE_ROOT.parent  # OldIron/

# 把 shared 加入 sys.path
_shared = PROJECT_ROOT / "shared"
if str(_shared) not in sys.path:
    sys.path.insert(0, str(_shared))


def run_bizmaps(argv: list[str]) -> int:
    """bizmaps 站点执行入口。"""
    parser = argparse.ArgumentParser(description="biz-maps.com 日本企业信息采集")
    parser.add_argument(
        "mode", nargs="?", default="all",
        choices=["all", "list", "gmap", "email"],
        help="运行模式: all=顺序执行三个 Pipeline, list=只跑 P1, gmap=只跑 P2, email=只跑 P3",
    )
    parser.add_argument(
        "--delay", type=float, default=1.5,
        help="P1 请求间隔秒数（默认 1.5）",
    )
    parser.add_argument(
        "--proxy", type=str, default="",
        help="HTTP 代理地址（默认读 HTTP_PROXY 或 7897）",
    )
    parser.add_argument(
        "--max-prefs", type=int, default=0,
        help="P1 最大采集都道府県数（0=全部47个）",
    )
    parser.add_argument(
        "--max-items", type=int, default=0,
        help="P2/P3 最大处理公司数（0=全部）",
    )
    parser.add_argument(
        "--gmap-workers", type=int, default=16,
        help="P2 GMap 并发数（默认 16）",
    )
    parser.add_argument(
        "--email-workers", type=int, default=32,
        help="P3 邮箱提取并发数（默认 32）",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（默认 INFO）",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    output_dir = SITE_ROOT / "output" / "bizmaps"

    # 代理：命令行 > 环境变量 > 默认 7897
    proxy = args.proxy or os.getenv("HTTP_PROXY", "")
    if not proxy:
        proxy = "http://127.0.0.1:7897"
        logging.getLogger("bizmaps").info("未指定代理，默认使用 %s", proxy)

    try:
        results = {}

        if args.mode in ("all", "list"):
            from .pipeline import run_pipeline_list
            stats = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_prefs=args.max_prefs,
            )
            results["pipeline1_list"] = stats

        if args.mode in ("all", "gmap"):
            from .pipeline2_gmap import run_pipeline_gmap
            stats = run_pipeline_gmap(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.gmap_workers,
            )
            results["pipeline2_gmap"] = stats

        if args.mode in ("all", "email"):
            from .pipeline3_email import run_pipeline_email
            stats = run_pipeline_email(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.email_workers,
            )
            results["pipeline3_email"] = stats

        print(f"\n完成: {results}")
        return 0
    except KeyboardInterrupt:
        print("\n用户中断，已保存断点。")
        return 1
    except Exception as exc:
        logging.getLogger("bizmaps").error("执行失败: %s", exc, exc_info=True)
        return 1
