"""bizmaps CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

SITE_ROOT = Path(__file__).resolve().parents[4]  # Japan/


def run_bizmaps(argv: list[str]) -> int:
    """bizmaps 站点执行入口。"""
    parser = argparse.ArgumentParser(description="biz-maps.com 日本企业信息采集")
    parser.add_argument(
        "--delay", type=float, default=1.5,
        help="请求间隔秒数（默认 1.5）",
    )
    parser.add_argument(
        "--proxy", type=str, default="",
        help="HTTP 代理地址（默认读 HTTP_PROXY 环境变量）",
    )
    parser.add_argument(
        "--max-prefs", type=int, default=0,
        help="最大采集都道府県数（0=全部47个，调试用）",
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
        # 日本站点国内一般需要代理
        proxy = "http://127.0.0.1:7897"
        logging.getLogger("bizmaps").info("未指定代理，默认使用 %s", proxy)

    from .pipeline import run_pipeline_list

    try:
        stats = run_pipeline_list(
            output_dir=output_dir,
            request_delay=args.delay,
            proxy=proxy,
            max_prefs=args.max_prefs,
        )
        print(f"\n完成: {stats}")
        return 0
    except KeyboardInterrupt:
        print("\n用户中断，已保存断点。")
        return 1
    except Exception as exc:
        logging.getLogger("bizmaps").error("执行失败: %s", exc, exc_info=True)
        return 1
