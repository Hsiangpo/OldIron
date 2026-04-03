"""bizmaps CLI 入口。"""

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

# 把 shared 加入 sys.path
_shared = PROJECT_ROOT / "shared"
if str(_shared) not in sys.path:
    sys.path.insert(0, str(_shared))

logger = logging.getLogger("bizmaps.cli")


def run_bizmaps(argv: list[str]) -> int:
    """bizmaps 站点执行入口。"""
    parser = argparse.ArgumentParser(description="biz-maps.com 日本企业信息采集")
    parser.add_argument(
        "mode", nargs="?", default="all",
        choices=["all", "list", "gmap", "email"],
        help="运行模式: all=三个 Pipeline 并发, list=只跑 P1, gmap=只跑 P2, email=只跑 P3",
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
        "--email-workers", type=int, default=128,
        help="P3 邮箱提取并发数（默认 128）",
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
        logger.info("未指定代理，默认使用 %s", proxy)

    try:
        if args.mode == "all":
            # 三个 Pipeline 并发执行
            return _run_all_concurrent(output_dir, proxy, args)

        results = {}
        if args.mode == "list":
            from .pipeline import run_pipeline_list
            stats = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_prefs=args.max_prefs,
            )
            results["pipeline1_list"] = stats

        if args.mode == "gmap":
            from .pipeline2_gmap import run_pipeline_gmap
            stats = run_pipeline_gmap(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.gmap_workers,
            )
            results["pipeline2_gmap"] = stats

        if args.mode == "email":
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
        logger.error("执行失败: %s", exc, exc_info=True)
        return 1


# ── 并发调度 ──

# P2/P3 轮询间隔（秒）：DB 中没有新数据时休眠一段时间再重试
_POLL_INTERVAL = 60
# P1 完成后给 P2/P3 的最大额外等待轮数（每轮 _POLL_INTERVAL 秒）
_MAX_IDLE_ROUNDS = 3


def _run_all_concurrent(output_dir: Path, proxy: str, args) -> int:
    """三个 Pipeline 并发执行。

    - P1（列表抓取）独立线程跑
    - P2（GMap 官网补全）独立线程，循环轮询 DB 获取新入库的无官网公司
    - P3（邮箱提取）独立线程，循环轮询 DB 获取新入库的有官网但无邮箱公司

    P2/P3 不等 P1 结束，而是边跑边消费，P1 结束后再做最后几轮扫尾。
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
            )
            results["pipeline1_list"] = stats
            logger.info("P1（列表抓取）完成: %s", stats)
        except Exception as exc:
            logger.error("P1 异常: %s", exc, exc_info=True)
            errors.append(f"P1: {exc}")
        finally:
            p1_done.set()

    def _p2_worker():
        """持续轮询 DB，补全没有官网的公司。"""
        try:
            from .pipeline2_gmap import run_pipeline_gmap
            total_processed = 0
            total_found = 0
            idle_rounds = 0
            round_no = 0

            while True:
                round_no += 1
                logger.info("[P2 GMap] 第 %d 轮扫描...", round_no)
                stats = run_pipeline_gmap(
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=args.gmap_workers,
                )
                batch_processed = stats.get("processed", 0)
                total_processed += batch_processed
                total_found += stats.get("found", 0)

                if batch_processed == 0:
                    # 本轮没有新数据
                    if p1_done.is_set():
                        idle_rounds += 1
                        if idle_rounds >= _MAX_IDLE_ROUNDS:
                            logger.info("[P2 GMap] P1 已结束且连续 %d 轮无新数据，退出", idle_rounds)
                            break
                    logger.info("[P2 GMap] 暂无新数据，%ds 后重试...", _POLL_INTERVAL)
                    time.sleep(_POLL_INTERVAL)
                else:
                    idle_rounds = 0  # 有数据则重置空闲计数

            results["pipeline2_gmap"] = {"processed": total_processed, "found": total_found}
            logger.info("P2（GMap）完成: 总处理 %d, 找到官网 %d", total_processed, total_found)
        except Exception as exc:
            logger.error("P2 异常: %s", exc, exc_info=True)
            errors.append(f"P2: {exc}")

    def _p3_worker():
        """持续轮询 DB，提取有官网但无邮箱的公司邮箱。"""
        try:
            from .pipeline3_email import run_pipeline_email
            total_processed = 0
            total_found = 0
            idle_rounds = 0
            round_no = 0

            while True:
                round_no += 1
                logger.info("[P3 Email] 第 %d 轮扫描...", round_no)
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
                            logger.info("[P3 Email] P1 已结束且连续 %d 轮无新数据，退出", idle_rounds)
                            break
                    logger.info("[P3 Email] 暂无新数据，%ds 后重试...", _POLL_INTERVAL)
                    time.sleep(_POLL_INTERVAL)
                else:
                    idle_rounds = 0

            results["pipeline3_email"] = {"processed": total_processed, "found": total_found}
            logger.info("P3（Email）完成: 总处理 %d, 找到邮箱 %d", total_processed, total_found)
        except Exception as exc:
            logger.error("P3 异常: %s", exc, exc_info=True)
            errors.append(f"P3: {exc}")

    # 启动三个线程
    threads = [
        threading.Thread(target=_p1_worker, name="P1-List", daemon=True),
        threading.Thread(target=_p2_worker, name="P2-GMap", daemon=True),
        threading.Thread(target=_p3_worker, name="P3-Email", daemon=True),
    ]
    for t in threads:
        t.start()
        logger.info("已启动线程: %s", t.name)

    # 等待所有线程结束
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info("用户中断，等待线程安全退出...")
        p1_done.set()  # 通知 P2/P3 尽快退出
        for t in threads:
            t.join(timeout=10)

    print(f"\n全部完成: {results}")
    if errors:
        print(f"错误: {errors}")
        return 1
    return 0
