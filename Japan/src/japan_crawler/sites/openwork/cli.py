"""OpenWork CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import os
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from queue import Empty


SITE_ROOT = Path(__file__).resolve().parents[4]
PROJECT_ROOT = SITE_ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

LOGGER = logging.getLogger("openwork.cli")
_POLL_INTERVAL = 60
_MAX_IDLE_ROUNDS = 3
_HEARTBEAT_INTERVAL = 15


def _configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def _p1_zero_progress(stats: dict[str, int]) -> bool:
    return int(stats.get("pages_done", 0)) <= 0 and int(stats.get("new_companies", 0)) <= 0


def run_openwork(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="OpenWork 日本企业信息采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "gmap", "email", "auth"])
    parser.add_argument("--delay", type=float, default=1.2, help="P1 列表请求间隔秒数")
    parser.add_argument("--proxy", type=str, default="", help="HTTP 代理地址")
    parser.add_argument("--max-pages", type=int, default=0, help="P1 最大采集页数（0=全部）")
    parser.add_argument("--max-items", type=int, default=0, help="P2/P3 最大处理公司数（0=全部）")
    parser.add_argument("--detail-workers", type=int, default=12, help="P1 详情页并发数")
    parser.add_argument("--gmap-workers", type=int, default=16, help="P2 GMap 并发数")
    parser.add_argument("--email-workers", type=int, default=128, help="P3 邮箱提取并发数")
    parser.add_argument("--manual-wait-seconds", type=int, default=600, help="auth 模式人工验证码等待秒数")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    _configure_logging(args.log_level)
    output_dir = SITE_ROOT / "output" / "openwork"
    proxy = args.proxy or os.getenv("HTTP_PROXY", "") or "http://127.0.0.1:7897"
    try:
        if args.mode == "auth":
            return _run_auth(output_dir, proxy, args.manual_wait_seconds)
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


def _run_auth(output_dir: Path, proxy: str, manual_wait_seconds: int) -> int:
    from .browser_profile import OpenworkBrowserBlocked, OpenworkPersistentBrowser

    browser = OpenworkPersistentBrowser(
        user_data_dir=output_dir / "browser_profile",
        proxy_url=proxy,
        manual_wait_seconds=manual_wait_seconds,
    )
    try:
        browser.prepare_manual_auth()
    except OpenworkBrowserBlocked as exc:
        LOGGER.error("%s", exc)
        return 1
    print(f"OpenWork 浏览器 profile 已就绪：{output_dir / 'browser_profile'}")
    return 0


def _run_all_concurrent(output_dir: Path, proxy: str, args) -> int:
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    stop_event = ctx.Event()
    p1_done = ctx.Event()
    p1_failed = ctx.Event()

    processes = [
        ctx.Process(
            target=_openwork_p1_entry,
            args=(
                str(output_dir),
                proxy,
                float(args.delay),
                int(args.max_pages),
                int(args.detail_workers),
                str(args.log_level),
                result_queue,
                stop_event,
                p1_done,
                p1_failed,
            ),
            name="openwork-p1",
        ),
        ctx.Process(
            target=_openwork_loop_entry,
            args=(
                "pipeline2_gmap",
                str(output_dir),
                int(args.max_items),
                int(args.gmap_workers),
                str(args.log_level),
                result_queue,
                stop_event,
                p1_done,
                p1_failed,
            ),
            name="openwork-p2",
        ),
        ctx.Process(
            target=_openwork_loop_entry,
            args=(
                "pipeline3_email",
                str(output_dir),
                int(args.max_items),
                int(args.email_workers),
                str(args.log_level),
                result_queue,
                stop_event,
                p1_done,
                p1_failed,
            ),
            name="openwork-p3",
        ),
    ]
    for process in processes:
        process.start()
    LOGGER.info("OpenWork all 已启动：P1/P2/P3 多进程并行运行。")
    _wait_for_processes(processes, output_dir, _openwork_progress_snapshot)
    results, errors = _collect_process_results(result_queue)
    print(f"\n全部完成: {results}")
    if errors:
        print(f"错误: {errors}")
        return 1
    return 0


def _openwork_p1_entry(
    output_dir: str,
    proxy: str,
    delay: float,
    max_pages: int,
    detail_workers: int,
    log_level: str,
    result_queue,
    stop_event,
    p1_done,
    p1_failed,
) -> None:
    try:
        _configure_logging(log_level)
        LOGGER.info("[pipeline1_list] 子进程启动")
        from .pipeline import run_pipeline_list

        stats = run_pipeline_list(
            output_dir=Path(output_dir),
            request_delay=delay,
            proxy=proxy,
            max_pages=max_pages,
            detail_workers=detail_workers,
        )
        if _p1_zero_progress(stats):
            p1_failed.set()
            stop_event.set()
            result_queue.put(("pipeline1_list", stats, "P1: P1 启动失败或零进展，停止空轮询。"))
        else:
            result_queue.put(("pipeline1_list", stats, ""))
    except Exception as exc:  # noqa: BLE001
        result_queue.put(("pipeline1_list", {}, f"P1: {exc}\n{traceback.format_exc()}"))
        stop_event.set()
    finally:
        LOGGER.info("[pipeline1_list] 子进程结束")
        p1_done.set()


def _openwork_loop_entry(
    name: str,
    output_dir: str,
    max_items: int,
    workers: int,
    log_level: str,
    result_queue,
    stop_event,
    p1_done,
    p1_failed,
) -> None:
    try:
        _configure_logging(log_level)
        runner = _resolve_openwork_runner(name)
        total_processed = 0
        total_found = 0
        idle_rounds = 0
        round_no = 0
        while not stop_event.is_set():
            round_no += 1
            LOGGER.info("[%s] 第 %d 轮扫描...", name, round_no)
            stats = runner(output_dir=Path(output_dir), max_items=max_items, concurrency=workers)
            processed = int(stats.get("processed", 0))
            total_processed += processed
            total_found += int(stats.get("found", 0))
            if processed > 0:
                LOGGER.info("[%s] 本轮处理 %d 家，累计处理 %d 家。", name, processed, total_processed)
            if processed == 0:
                if p1_failed.is_set():
                    break
                if p1_done.is_set():
                    idle_rounds += 1
                    if idle_rounds >= _MAX_IDLE_ROUNDS:
                        LOGGER.info("[%s] 连续空轮询 %d 次，结束。", name, idle_rounds)
                        break
                if stop_event.wait(_POLL_INTERVAL):
                    break
                continue
            idle_rounds = 0
        LOGGER.info("[%s] 子进程结束：processed=%d found=%d", name, total_processed, total_found)
        result_queue.put((name, {"processed": total_processed, "found": total_found}, ""))
    except Exception as exc:  # noqa: BLE001
        result_queue.put((name, {}, f"{name}: {exc}\n{traceback.format_exc()}"))
        stop_event.set()


def _resolve_openwork_runner(name: str):
    if name == "pipeline2_gmap":
        from .pipeline2_gmap import run_pipeline_gmap

        return run_pipeline_gmap
    from .pipeline3_email import run_pipeline_email

    return run_pipeline_email


def _wait_for_processes(processes: list[mp.Process], output_dir: Path, snapshotter) -> None:
    next_heartbeat = time.monotonic() + _HEARTBEAT_INTERVAL
    while True:
        alive = False
        for process in processes:
            process.join(timeout=0.5)
            alive = alive or process.is_alive()
        if not alive:
            return
        if time.monotonic() < next_heartbeat:
            continue
        LOGGER.info(
            "OpenWork all 运行中：%s | 进程=%s",
            snapshotter(output_dir),
            _format_process_states(processes),
        )
        next_heartbeat = time.monotonic() + _HEARTBEAT_INTERVAL


def _format_process_states(processes: list[mp.Process]) -> str:
    states: list[str] = []
    for process in processes:
        if process.is_alive():
            state = "运行中"
        elif process.exitcode is None:
            state = "未启动"
        else:
            state = f"退出({process.exitcode})"
        states.append(f"{process.name}:{state}")
    return ", ".join(states)


def _openwork_progress_snapshot(output_dir: Path) -> str:
    db_path = output_dir / "openwork_store.db"
    if not db_path.exists():
        return "数据库尚未创建"
    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as conn:
            checkpoint = conn.execute(
                """
                SELECT
                    COUNT(*) AS scope_count,
                    SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_count,
                    COALESCE(SUM(last_page), 0) AS finished_pages,
                    COALESCE(SUM(total_pages), 0) AS total_pages
                FROM checkpoints
                WHERE scope LIKE 'company_list%'
                """
            ).fetchone()
            company_count, updated_at = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM companies"
            ).fetchone()
            email_todo = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE coalesce(email_status, 'pending') != 'done'"
            ).fetchone()[0]
            gmap_todo = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE (website IS NULL OR website = '') AND coalesce(gmap_status, 'pending') != 'done'"
            ).fetchone()[0]
    except sqlite3.Error as exc:
        return f"读取进度失败: {exc}"
    if checkpoint is None or int(checkpoint[0] or 0) <= 0:
        return f"P1 尚未落盘 | 公司={company_count}"
    return (
        f"P1范围={int(checkpoint[1] or 0)}/{int(checkpoint[0] or 0)} | "
        f"页进度={int(checkpoint[2] or 0)}/{int(checkpoint[3] or 0)} | 公司={company_count} | "
        f"邮箱待补={email_todo} | 官网待补={gmap_todo} | 最新更新={updated_at or '-'}"
    )


def _collect_process_results(result_queue, expected_count: int = 3) -> tuple[dict[str, dict], list[str]]:
    results: dict[str, dict] = {}
    errors: list[str] = []
    received = 0
    while received < expected_count:
        try:
            name, stats, error = result_queue.get(timeout=5)
        except Empty:
            errors.append(f"子进程结果回收不完整：{received}/{expected_count}")
            break
        received += 1
        if stats:
            results[name] = stats
        if error:
            errors.append(str(error))
    return results, errors
