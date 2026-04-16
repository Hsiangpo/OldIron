"""阿联酋站点统一 CLI。"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import os
import sqlite3
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .pipelines import run_pipeline_email
from .pipelines import run_pipeline_gmap
from .store import UaeCompanyStore


LOGGER = logging.getLogger("uae.common.cli")
POLL_INTERVAL = 20
MAX_IDLE_ROUNDS = 3
P1_RETRY_LIMIT = 6
P1_RETRY_DELAY_SECONDS = 15
P1_USAGE_LIMIT_RETRY_DELAY_SECONDS = 60
P1_TRANSIENT_RETRY_DELAY_SECONDS = 30
P1_CHALLENGE_RETRY_DELAY_SECONDS = 300
P2P3_BATCH_TIMEOUT_SECONDS = 180
P2P3_TIMEOUT_BACKOFF_SECONDS = 5


class BatchFatalError(RuntimeError):
    """子进程已经确认是硬错误，父进程应立即收口。"""


def run_site_cli(
    *,
    site_name: str,
    description: str,
    output_dir: Path,
    argv: list[str],
    run_list: Callable[..., dict[str, int]],
    run_email: Callable[..., dict[str, int]] = run_pipeline_email,
    enable_gmap: bool = True,
) -> int:
    """统一站点入口。"""
    _ensure_stdio_utf8()
    mode_choices = ["all", "list", "email"]
    if enable_gmap:
        mode_choices.insert(2, "gmap")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("mode", nargs="?", default="all", choices=mode_choices)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--proxy", type=str, default="")
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--list-workers", type=int, default=8)
    parser.add_argument("--max-items", type=int, default=0)
    if enable_gmap:
        parser.add_argument("--gmap-workers", type=int, default=64)
    else:
        parser.set_defaults(gmap_workers=0)
    parser.add_argument("--email-workers", type=int, default=64)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    proxy = args.proxy or os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")
    if args.mode == "all":
        return _run_all_concurrent(
            site_name,
            output_dir,
            proxy,
            args,
            run_list,
            run_email=run_email,
            enable_gmap=enable_gmap,
        )
    results: dict[str, dict[str, int]] = {}
    if args.mode == "list":
        results["pipeline1_list"] = run_list(
            output_dir=output_dir,
            request_delay=args.delay,
            proxy=proxy,
            max_pages=args.max_pages,
            concurrency=args.list_workers,
        )
    if args.mode == "gmap":
        results["pipeline2_gmap"] = run_pipeline_gmap(
            output_dir=output_dir,
            max_items=args.max_items,
            concurrency=args.gmap_workers,
        )
    if args.mode == "email":
        results["pipeline3_email"] = run_email(
            output_dir=output_dir,
            max_items=args.max_items,
            concurrency=args.email_workers,
        )
    print(f"\n完成: {results}")
    return 0


def _run_all_concurrent(
    site_name: str,
    output_dir: Path,
    proxy: str,
    args,
    run_list: Callable[..., dict[str, int]],
    *,
    run_email: Callable[..., dict[str, int]],
    enable_gmap: bool,
) -> int:
    p1_done = threading.Event()
    stop_event = threading.Event()
    results: dict[str, dict[str, int]] = {}
    errors: list[str] = []
    errors_lock = threading.Lock()

    def _record_error(message: str) -> None:
        with errors_lock:
            errors.append(message)

    def _p1_worker() -> None:
        try:
            result, error = _run_p1_with_resume(site_name, output_dir, proxy, args, run_list)
            if result is not None:
                results["pipeline1_list"] = result
            if error:
                _record_error(error)
        finally:
            p1_done.set()

    def _loop_worker(kind: str, runner: Callable[..., dict[str, int]], workers: int) -> None:
        idle_rounds = 0
        total_processed = 0
        total_found = 0
        try:
            while not stop_event.is_set():
                stats = _run_batch_with_timeout(
                    kind=kind,
                    runner=runner,
                    output_dir=output_dir,
                    max_items=args.max_items,
                    concurrency=workers,
                    fatal_error_types=_fatal_error_types(site_name, kind),
                )
                total_processed += int(stats.get("processed", 0))
                total_found += int(stats.get("found", 0))
                if int(stats.get("processed", 0)) == 0:
                    if p1_done.is_set():
                        if _has_pending_work(output_dir, kind, site_name=site_name):
                            LOGGER.info("%s 仍有待处理记录，继续轮询。", kind)
                            idle_rounds = 0
                        else:
                            idle_rounds += 1
                            if idle_rounds >= MAX_IDLE_ROUNDS:
                                break
                    time.sleep(POLL_INTERVAL)
                    continue
                idle_rounds = 0
        except BatchFatalError as exc:
            stop_event.set()
            _record_error(f"{kind}: {exc}")
            LOGGER.error("%s 出现硬错误，停止当前站点 all 模式：%s", kind, exc)
            return
        except Exception as exc:  # noqa: BLE001
            stop_event.set()
            _record_error(f"{kind}: {exc}")
            LOGGER.error("%s 线程异常，停止当前站点 all 模式：%s", kind, exc, exc_info=True)
            return
        if not stop_event.is_set():
            results[kind] = {"processed": total_processed, "found": total_found}

    threads = [threading.Thread(target=_p1_worker, name=f"{site_name}-p1", daemon=True)]
    if enable_gmap:
        threads.append(
            threading.Thread(
                target=_loop_worker,
                args=("pipeline2_gmap", run_pipeline_gmap, args.gmap_workers),
                name=f"{site_name}-p2",
                daemon=True,
            )
        )
    threads.append(
        threading.Thread(
            target=_loop_worker,
            args=("pipeline3_email", run_email, args.email_workers),
            name=f"{site_name}-p3",
            daemon=True,
        )
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    total = UaeCompanyStore(output_dir / "companies.db").get_company_count()
    _print_all_summary(results, total, errors)
    return 1 if errors else 0


def _run_p1_with_resume(
    site_name: str,
    output_dir: Path,
    proxy: str,
    args,
    run_list: Callable[..., dict[str, int]],
) -> tuple[dict[str, int] | None, str]:
    last_error = ""
    attempt = 0
    usage_limit_rounds = 0
    while True:
        try:
            result = run_list(
                output_dir=output_dir,
                request_delay=args.delay,
                proxy=proxy,
                max_pages=args.max_pages,
                concurrency=args.list_workers,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if _looks_like_usage_limit_error(exc):
                usage_limit_rounds += 1
                LOGGER.warning(
                    "%s P1 命中站点额度限制，等待 %ds 后按断点继续：round=%d error=%s",
                    site_name,
                    P1_USAGE_LIMIT_RETRY_DELAY_SECONDS,
                    usage_limit_rounds,
                    exc,
                )
                time.sleep(P1_USAGE_LIMIT_RETRY_DELAY_SECONDS)
                continue
            if _looks_like_challenge_hold_error(exc):
                LOGGER.warning(
                    "%s P1 命中 fk/challenge，等待 %ds 后按断点继续：error=%s",
                    site_name,
                    P1_CHALLENGE_RETRY_DELAY_SECONDS,
                    exc,
                )
                time.sleep(P1_CHALLENGE_RETRY_DELAY_SECONDS)
                continue
            if _looks_like_transient_p1_error(exc):
                LOGGER.warning(
                    "%s P1 命中临时上游异常，等待 %ds 后按断点继续：error=%s",
                    site_name,
                    P1_TRANSIENT_RETRY_DELAY_SECONDS,
                    exc,
                )
                time.sleep(P1_TRANSIENT_RETRY_DELAY_SECONDS)
                continue
            attempt += 1
            LOGGER.error(
                "%s P1 异常，准备按断点重试：attempt=%d/%d error=%s",
                site_name,
                attempt,
                P1_RETRY_LIMIT,
                exc,
                exc_info=True,
            )
            if attempt >= P1_RETRY_LIMIT:
                return None, last_error
            time.sleep(P1_RETRY_DELAY_SECONDS)
            continue
        if usage_limit_rounds > 0 or attempt > 0:
            LOGGER.info(
                "%s P1 已恢复：normal_retry=%d usage_limit_round=%d",
                site_name,
                attempt,
                usage_limit_rounds,
            )
        return result, ""


def _run_batch_with_timeout(
    *,
    kind: str,
    runner: Callable[..., dict[str, int]],
    output_dir: Path,
    max_items: int,
    concurrency: int,
    fatal_error_types: tuple[str, ...] = (),
) -> dict[str, int]:
    """把 P2/P3 批次放到子进程里，避免整条线被慢站点永久卡死。"""
    ctx = mp.get_context("spawn")
    queue: mp.Queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_batch_worker_entry,
        args=(runner, output_dir, max_items, concurrency, queue),
        daemon=True,
    )
    process.start()
    process.join(P2P3_BATCH_TIMEOUT_SECONDS)
    if process.is_alive():
        LOGGER.warning(
            "%s 批次超过 %ds 未返回，终止本批并稍后续跑。",
            kind,
            P2P3_BATCH_TIMEOUT_SECONDS,
        )
        process.terminate()
        process.join(timeout=3)
        _close_batch_queue(queue)
        time.sleep(P2P3_TIMEOUT_BACKOFF_SECONDS)
        return {"processed": 0, "found": 0}
    payload = _read_batch_payload(queue)
    _close_batch_queue(queue)
    if payload.get("ok"):
        stats = payload.get("stats")
        if isinstance(stats, dict):
            return stats
    error_type = str(payload.get("error_type") or "").strip()
    error_text = str(payload.get("error") or f"exitcode={process.exitcode}")
    if error_type and error_type in fatal_error_types:
        raise BatchFatalError(error_text)
    LOGGER.warning("%s 批次执行失败，稍后续跑：%s", kind, error_text)
    time.sleep(P2P3_TIMEOUT_BACKOFF_SECONDS)
    return {"processed": 0, "found": 0}


def _batch_worker_entry(
    runner: Callable[..., dict[str, int]],
    output_dir: Path,
    max_items: int,
    concurrency: int,
    queue: mp.Queue,
) -> None:
    """子进程真正执行一批任务，并把结果回传给父进程。"""
    try:
        stats = runner(output_dir=output_dir, max_items=max_items, concurrency=concurrency)
    except Exception as exc:  # noqa: BLE001
        queue.put({"ok": False, "error": f"{type(exc).__name__}: {exc}", "error_type": type(exc).__name__})
        return
    queue.put({"ok": True, "stats": stats})


def _read_batch_payload(queue: mp.Queue) -> dict[str, object]:
    """安全读取子进程回传结果。"""
    try:
        if queue.empty():
            return {}
        payload = queue.get_nowait()
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _close_batch_queue(queue: mp.Queue) -> None:
    """释放 multiprocessing 队列句柄，避免句柄越积越多。"""
    try:
        queue.close()
    except Exception:  # noqa: BLE001
        pass
    try:
        queue.join_thread()
    except Exception:  # noqa: BLE001
        pass


def _has_pending_work(output_dir: Path, kind: str, *, site_name: str = "") -> bool:
    """只有数据库里确实没有 pending，空轮询才允许退出。"""
    db_path = output_dir / "companies.db"
    if not db_path.exists():
        return False
    sql_by_kind = {
        "pipeline2_gmap": """
            SELECT 1
            FROM companies
            WHERE (website = '' OR website IS NULL)
              AND coalesce(gmap_status, 'pending') != 'done'
            LIMIT 1
        """,
        "pipeline3_email": _email_pending_sql(site_name),
    }
    sql = sql_by_kind.get(kind)
    if not sql:
        return False
    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as conn:
            row = conn.execute(sql).fetchone()
    except sqlite3.Error as exc:
        LOGGER.warning("%s 检查 pending 失败，按仍有任务处理：%s", kind, exc)
        return True
    return row is not None


def _email_pending_sql(site_name: str) -> str:
    if site_name == "wiza":
        return """
            SELECT 1
            FROM companies
            WHERE coalesce(email_status, 'pending') != 'done'
            LIMIT 1
        """
    return """
        SELECT 1
        FROM companies
        WHERE website != '' AND website IS NOT NULL
          AND coalesce(email_status, 'pending') != 'done'
        LIMIT 1
    """


def _fatal_error_types(site_name: str, kind: str) -> tuple[str, ...]:
    if site_name == "wiza" and kind == "pipeline3_email":
        return ("SnovAuthError", "SnovQuotaError", "SnovPermissionError")
    return ()


def _print_all_summary(results: dict[str, dict[str, int]], total: int, errors: list[str]) -> None:
    """输出总结时兜住 Windows 非 UTF-8 控制台。"""
    if errors:
        _safe_print(f"\n部分完成: {results} | companies={total} | errors={errors}")
        return
    _safe_print(f"\n完成: {results} | companies={total}")


def _ensure_stdio_utf8() -> None:
    """尽量把标准输出切到 UTF-8，避免中文总结在 Windows 上崩溃。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            continue


def _safe_print(message: str) -> None:
    """在 reconfigure 不生效时，回退到底层 buffer 输出。"""
    try:
        print(message)
        return
    except UnicodeEncodeError:
        pass
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is None:
        raise
    buffer.write((message + "\n").encode("utf-8", errors="replace"))
    buffer.flush()


def _looks_like_usage_limit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc or "").lower()
    return "usagelimit" in name or "usage limit" in message or "额度已用尽" in message


def _looks_like_transient_p1_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc or "").lower()
    if name in {"jsondecodeerror", "sslerror", "connectionerror", "toomanyredirects", "timeout", "timeouterror"}:
        return True
    transient_markers = (
        "expecting value: line 1 column 1",
        "connection timed out",
        "operation timed out",
        "empty reply from server",
        "maximum (30) redirects followed",
        "tls connect error",
        "invalid library",
        "curl: (28)",
        "curl: (35)",
        "curl: (47)",
        "curl: (52)",
    )
    return any(marker in message for marker in transient_markers)


def _looks_like_challenge_hold_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    markers = (
        "cf cookie 已失效",
        "sorry, you have been blocked",
        "attention required! | cloudflare",
        "performing security verification",
        "browser refresh",
        "target page, context or browser has been closed",
    )
    return any(marker in message for marker in markers)
