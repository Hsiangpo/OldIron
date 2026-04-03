"""hellowork CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path

SITE_ROOT = Path(__file__).resolve().parents[4]  # Japan/
PROJECT_ROOT = SITE_ROOT.parent  # OldIron/

_shared = PROJECT_ROOT / "shared"
if str(_shared) not in sys.path:
    sys.path.insert(0, str(_shared))

logger = logging.getLogger("hellowork.cli")


def run_hellowork(argv: list[str]) -> int:
    """hellowork 站点执行入口。"""
    parser = argparse.ArgumentParser(description="ハローワーク 日本企业信息采集")
    parser.add_argument(
        "mode", nargs="?", default="all",
        choices=["all", "list", "email"],
        help="运行模式: all=双 Pipeline 并发, list=只跑 P1, email=只跑 P2",
    )
    parser.add_argument("--delay", type=float, default=0.3, help="P1 请求间隔秒数（默认 0.3）")
    parser.add_argument("--proxy", type=str, default="", help="HTTP 代理地址")
    parser.add_argument("--max-prefs", type=int, default=0, help="P1 最大采集都道府県数（0=全部47个）")
    parser.add_argument("--max-items", type=int, default=0, help="P2 最大处理企业数（0=全部）")
    parser.add_argument("--detail-workers", type=int, default=16, help="P1 详情页并发数（默认 16）")
    parser.add_argument("--pref-workers", type=int, default=6, help="P1 跨县并发数（默认 6）")
    parser.add_argument("--email-workers", type=int, default=128, help="P2 邮箱提取并发数（默认 128）")
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    output_dir = SITE_ROOT / "output" / "hellowork"
    proxy = args.proxy or os.getenv("HTTP_PROXY", "")
    if not proxy:
        proxy = "http://127.0.0.1:7897"
        logger.info("未指定代理，默认使用 %s", proxy)

    try:
        if args.mode == "all":
            return _run_all_concurrent(output_dir, proxy, args)

        results = {}
        if args.mode == "list":
            from .pipeline import run_pipeline_list
            stats = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_prefs=args.max_prefs,
                detail_workers=args.detail_workers,
                pref_workers=args.pref_workers,
            )
            results["pipeline1_list"] = stats

        if args.mode == "email":
            from .pipeline2_email import run_pipeline_email
            stats = run_pipeline_email(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.email_workers,
            )
            results["pipeline2_email"] = stats

        print(f"\n完成: {results}")
        return 0
    except KeyboardInterrupt:
        print("\n用户中断，已保存断点。")
        return 1
    except Exception as exc:
        logger.error("执行失败: %s", exc, exc_info=True)
        return 1


# ── 并发调度 ──

_POLL_INTERVAL = 60
_MAX_IDLE_ROUNDS = 3


def _run_all_concurrent(output_dir: Path, proxy: str, args) -> int:
    """P1 + P2 并发执行。

    P1（列表/详情抓取）独立线程跑；
    P2（邮箱提取）独立线程，循环轮询 DB 获取新入库的有官网企业。
    """
    p1_done = threading.Event()
    results: dict[str, dict] = {}
    errors: list[str] = []

    def _p1_worker():
        try:
            from .pipeline import run_pipeline_list
            stats = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_prefs=args.max_prefs,
                detail_workers=args.detail_workers,
                pref_workers=args.pref_workers,
            )
            results["pipeline1_list"] = stats
            logger.info("P1（列表/详情）完成: %s", stats)
        except Exception as exc:
            logger.error("P1 异常: %s", exc, exc_info=True)
            errors.append(f"P1: {exc}")
        finally:
            p1_done.set()

    def _p2_worker():
        try:
            from .pipeline2_email import run_pipeline_email
            total_processed = 0
            total_found = 0
            idle_rounds = 0
            round_no = 0

            while True:
                round_no += 1
                logger.info("[P2 Email] 第 %d 轮扫描...", round_no)
                stats = run_pipeline_email(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=args.email_workers,
                )
                batch_processed = stats.get("processed", 0)
                total_processed += batch_processed
                total_found += stats.get("found", 0)

                if batch_processed == 0:
                    if p1_done.is_set():
                        idle_rounds += 1
                        if idle_rounds >= _MAX_IDLE_ROUNDS:
                            logger.info("[P2 Email] P1 已结束且连续 %d 轮无新数据，退出", idle_rounds)
                            break
                    logger.info("[P2 Email] 暂无新数据，%ds 后重试...", _POLL_INTERVAL)
                    time.sleep(_POLL_INTERVAL)
                else:
                    idle_rounds = 0

            results["pipeline2_email"] = {"processed": total_processed, "found": total_found}
            logger.info("P2（Email）完成: 总处理 %d, 找到邮箱 %d", total_processed, total_found)
        except Exception as exc:
            logger.error("P2 异常: %s", exc, exc_info=True)
            errors.append(f"P2: {exc}")

    threads = [
        threading.Thread(target=_p1_worker, name="P1-List", daemon=True),
        threading.Thread(target=_p2_worker, name="P2-Email", daemon=True),
    ]
    for t in threads:
        t.start()
        logger.info("已启动线程: %s", t.name)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info("用户中断，等待线程安全退出...")
        p1_done.set()
        for t in threads:
            t.join(timeout=10)

    print(f"\n全部完成: {results}")
    if errors:
        print(f"错误: {errors}")
        return 1
    return 0
