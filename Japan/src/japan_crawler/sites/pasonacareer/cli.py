"""PasonaCareer CLI 入口。"""

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

LOGGER = logging.getLogger("pasonacareer.cli")
_POLL_INTERVAL = 60
_MAX_IDLE_ROUNDS = 3


def _p1_zero_progress(stats: dict[str, int]) -> bool:
    return int(stats.get("pages_done", 0)) <= 0 and int(stats.get("new_companies", 0)) <= 0


def _wait_for_next_round(stop_event: threading.Event, p1_failed: threading.Event) -> bool:
    deadline = time.monotonic() + _POLL_INTERVAL
    while time.monotonic() < deadline:
        if p1_failed.is_set():
            return False
        remaining = max(0.0, deadline - time.monotonic())
        if stop_event.wait(min(1.0, remaining)):
            return False
    return True


def run_pasonacareer(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="PasonaCareer 日本职位转公司采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "gmap", "email"])
    parser.add_argument("--delay", type=float, default=1.0, help="P1 列表请求间隔秒数")
    parser.add_argument("--proxy", type=str, default="", help="HTTP 代理地址")
    parser.add_argument("--max-pages", type=int, default=0, help="P1 最大采集页数（0=全部）")
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
    output_dir = SITE_ROOT / "output" / "pasonacareer"
    proxy = args.proxy or os.getenv("HTTP_PROXY", "") or "http://127.0.0.1:7897"
    from .store import PasonacareerStore

    store = PasonacareerStore(output_dir / "pasonacareer_store.db")
    purged = store.purge_placeholder_companies()
    if purged:
        LOGGER.warning("PasonaCareer 启动清理脏公司名记录：%d", purged)
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
    p1_failed = threading.Event()
    stop_event = threading.Event()
    results: dict[str, dict] = {}
    errors: list[str] = []

    def _p1_worker() -> None:
        try:
            from .pipeline import run_pipeline_list

            stats = run_pipeline_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_pages=args.max_pages,
                detail_workers=args.detail_workers,
            )
            results["pipeline1_list"] = stats
            if _p1_zero_progress(stats):
                p1_failed.set()
                stop_event.set()
                message = "P1 启动失败或零进展，停止空轮询。"
                LOGGER.error(message)
                errors.append(f"P1: {message}")
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("P1 异常: %s", exc, exc_info=True)
            errors.append(f"P1: {exc}")
            stop_event.set()
        finally:
            p1_done.set()

    def _loop_runner(name: str, runner, workers: int) -> None:
        try:
            total_processed = 0
            total_found = 0
            idle_rounds = 0
            round_no = 0
            while not stop_event.is_set():
                round_no += 1
                LOGGER.info("[%s] 第 %d 轮扫描...", name, round_no)
                stats = runner(output_dir=output_dir, max_items=args.max_items, concurrency=workers)
                processed = int(stats.get("processed", 0))
                total_processed += processed
                total_found += int(stats.get("found", 0))
                if processed == 0:
                    if p1_failed.is_set():
                        LOGGER.error("[%s] P1 零进展，停止轮询。", name)
                        break
                    if p1_done.is_set():
                        idle_rounds += 1
                        if idle_rounds >= _MAX_IDLE_ROUNDS:
                            break
                    if not _wait_for_next_round(stop_event, p1_failed):
                        break
                    continue
                idle_rounds = 0
            results[name] = {"processed": total_processed, "found": total_found}
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("%s 异常: %s", name, exc, exc_info=True)
            errors.append(f"{name}: {exc}")
            stop_event.set()

    from .pipeline2_gmap import run_pipeline_gmap
    from .pipeline3_email import run_pipeline_email

    threads = [
        threading.Thread(target=_p1_worker, name="pasona-p1", daemon=True),
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
