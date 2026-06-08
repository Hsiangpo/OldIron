# -*- coding: utf-8 -*-
"""buffettcode CLI 入口：python run.py buffettcode [mode] [options]

数据源 buffett-code.com（バフェット・コード，日本上市+非上市企业库）。
按行业采集 公司名 + 代表者 + 官网（详情页直接解析，无需 LLM）。
反爬：blurpath 日本住宅 sticky IP + CapSolver 铸 aws-waf-token（见 waf.py）。
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from .export import export_per_industry
from .pipeline import run_pipeline
from .store import Store
from .waf import CapSolver, build_sessions

SITE_ROOT = Path(__file__).resolve().parents[4]  # Japan/
logger = logging.getLogger("buffettcode.cli")


def _build_cfg() -> dict | None:
    """从环境变量构建 blurpath 代理配置。"""
    key = os.getenv("CAPSOLVER_KEY", "").strip()
    if not key:
        print("缺少 CAPSOLVER_KEY，请在 Japan/.env 中配置。")
        return None
    return {
        "capsolver_key": key,
        "host": os.getenv("BLURPATH_HOST", "blurpath.net"),
        "port": os.getenv("BLURPATH_PORT", "15137"),
        "user": os.getenv("BLURPATH_USER", "").strip(),
        "password": os.getenv("BLURPATH_PASS", "").strip(),
        "region": os.getenv("BLURPATH_REGION", "JP"),
        "sticky_min": os.getenv("BLURPATH_STICKY_MIN", "30"),
        "impersonate": os.getenv("BUFFETT_IMPERSONATE", ""),  # 空=指纹池轮换
        "session_prefix": os.getenv("BLURPATH_SESSION_PREFIX", ""),
    }


def run_buffettcode(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="buffett-code 日本企业采集（公司名/代表者/官网，按行业）")
    parser.add_argument("mode", nargs="?", default="all",
                        choices=["all", "bootstrap", "crawl", "export"],
                        help="all=采集+导出, bootstrap=只拉行业清单, crawl=只采集, export=只导出")
    parser.add_argument("--detail-workers", type=int, default=6, help="详情线程数=并发日本IP数（默认 6，各占一个住宅IP）")
    parser.add_argument("--max-pages", type=int, default=0, help="每个行业最多翻几页（0=不限，用于测试）")
    parser.add_argument("--max-industries", type=int, default=0, help="最多采集几个行业（0=全部，用于测试）")
    parser.add_argument("--page-delay", type=float, default=0.8, help="列表翻页间隔秒（默认 0.8）")
    parser.add_argument("--worker-delay", type=float, default=0.3, help="详情请求间隔秒（默认 0.3）")
    parser.add_argument("--retry-failed", action="store_true",
                        help="启动时把之前 failed 的公司重置为 pending 并优先重爬（CapSolver 续费后用）")
    parser.add_argument("--verbose", action="store_true", help="打印每条公司详情（DEBUG 级别）")
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    log_path = SITE_ROOT / "output" / "buffettcode" / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(str(log_path), encoding="utf-8")],
    )

    cfg = _build_cfg()
    if cfg is None:
        return 1
    if not cfg["user"] or not cfg["password"]:
        print("缺少 BLURPATH_USER / BLURPATH_PASS，请在 Japan/.env 中配置。")
        return 1

    db_path = SITE_ROOT / "output" / "buffettcode" / "buffettcode.db"
    out_dir = SITE_ROOT / "output" / "buffettcode" / "csv"
    store = Store(str(db_path))

    capsolver = CapSolver(cfg["capsolver_key"])
    workers = max(1, args.detail_workers)
    sessions = build_sessions(cfg, capsolver, workers + 1)  # +1 给 lister
    logger.info("启动 buffettcode：mode=%s 并发IP=%d（lister 1 + 详情 %d）区域=%s",
                args.mode, len(sessions), workers, cfg["region"])

    if args.mode == "export":
        summary = export_per_industry(store, str(out_dir))
        logger.info("导出完成，共 %d 个行业 CSV", len(summary))
        return 0

    if args.mode == "bootstrap":
        from .pipeline import bootstrap_industries
        bootstrap_industries(store, sessions[0])
        return 0

    try:
        run_pipeline(
            store, sessions,
            max_pages_per_industry=args.max_pages,
            max_industries=args.max_industries,
            page_delay=args.page_delay,
            worker_delay=args.worker_delay,
            retry_failed=args.retry_failed,
        )
    except KeyboardInterrupt:
        logger.info("已停止。进度已保存，重跑同一条命令即从断点续跑。")
        return 0

    if args.mode == "all":
        summary = export_per_industry(store, str(out_dir))
        logger.info("导出完成，共 %d 个行业 CSV，输出目录 %s", len(summary), out_dir)
    return 0
