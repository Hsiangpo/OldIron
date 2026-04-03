"""Mynavi CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path


SITE_ROOT = Path(__file__).resolve().parents[4]
PROJECT_ROOT = SITE_ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

LOGGER = logging.getLogger("mynavi.cli")
_POLL_INTERVAL = 60
_MAX_IDLE_ROUNDS = 3


def run_mynavi(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Mynavi 日本企业信息采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "gmap", "email"])
    parser.add_argument("--delay", type=float, default=1.0, help="P1 列表请求间隔秒数")
    parser.add_argument("--proxy", type=str, default="", help="HTTP 代理地址")
    parser.add_argument("--max-groups", type=int, default=0, help="P1 最大采集五十音分组数（0=全部）")
    parser.add_argument("--max-pages", type=int, default=0, help="P1 每个分组最大页数（0=全部）")
    parser.add_argument("--max-items", type=int, default=0, help="P2/P3 最大处理公司数（0=全部）")
    parser.add_argument("--detail-workers", type=int, default=12, help="P1 详情页并发数")
    parser.add_argument("--gmap-workers", type=int, default=16, help="P2 GMap 并发数")
    parser.add_argument("--email-workers", type=int, default=128, help="P3 邮箱提取并发数")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    output_dir = SITE_ROOT / "output" / "mynavi"
    proxy = args.proxy or os.getenv("HTTP_PROXY", "") or "http://127.0.0.1:7897"
    try:
        if args.mode == "all":
            return _run_all_concurrent(output_dir, proxy, args)
        if args.mode == "list":
            from .pipeline import run_pipeline_list

            print(
                run_pipeline_list(
                    output_dir=output_dir,
                    request_delay=args.delay,
                    proxy=proxy,
                    max_groups=args.max_groups,
                    max_pages=args.max_pages,
                    detail_workers=args.detail_workers,
                )
            )
            return 0
        if args.mode == "gmap":
            from .pipeline2_gmap import run_pipeline_gmap

            print(run_pipeline_gmap(output_dir=output_dir, max_items=args.max_items, concurrency=args.gmap_workers))
            return 0
        from .pipeline3_email import run_pipeline_email

        print(run_pipeline_email(output_dir=output_dir, max_items=args.max_items, concurrency=args.email_workers))
        return 0
    except KeyboardInterrupt:
        print("\n用户中断，已保存断点。")
        return 1
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("执行失败: %s", exc, exc_info=True)
        return 1


def _run_all_concurrent(output_dir: Path, proxy: str, args) -> int:
    p1_done = threading.Event()
    results: dict[str, dict] = {}
    errors: list[str] = []

    def _p1_worker() -> None:
        try:
            from .pipeline import run_pipeline_list

            results["pipeline1_list"] = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_groups=args.max_groups,
                max_pages=args.max_pages,
                detail_workers=args.detail_workers,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("P1 异常: %s", exc, exc_info=True)
            errors.append(f"P1: {exc}")
        finally:
            p1_done.set()

    def _loop_runner(name: str, runner, workers: int) -> None:
        try:
            total_processed = 0
            total_found = 0
            idle_rounds = 0
            while True:
                stats = runner(output_dir=output_dir, max_items=args.max_items, concurrency=workers)
                processed = int(stats.get("processed", 0))
                total_processed += processed
                total_found += int(stats.get("found", 0))
                if processed == 0:
                    if p1_done.is_set():
                        idle_rounds += 1
                        if idle_rounds >= _MAX_IDLE_ROUNDS:
                            break
                    time.sleep(_POLL_INTERVAL)
                    continue
                idle_rounds = 0
            results[name] = {"processed": total_processed, "found": total_found}
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("%s 异常: %s", name, exc, exc_info=True)
            errors.append(f"{name}: {exc}")

    from .pipeline2_gmap import run_pipeline_gmap
    from .pipeline3_email import run_pipeline_email

    threads = [
        threading.Thread(target=_p1_worker, name="mynavi-p1", daemon=True),
        threading.Thread(target=_loop_runner, args=("pipeline2_gmap", run_pipeline_gmap, args.gmap_workers), daemon=True),
        threading.Thread(target=_loop_runner, args=("pipeline3_email", run_pipeline_email, args.email_workers), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print(f"\n全部完成: {results}")
    if errors:
        print(f"错误: {errors}")
        return 1
    return 0

