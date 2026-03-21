"""丹麦 Virk CLI 入口。"""

from __future__ import annotations

import argparse
import atexit
import ctypes
import json
import logging
import os
import time
from pathlib import Path

from denmark_crawler.virk.browser_cookie import VirkCookieProvider
from denmark_crawler.virk.client import VirkClient
from denmark_crawler.virk.config import VirkDenmarkConfig
from denmark_crawler.virk.pipeline import run_virk_pipeline


ROOT = Path(__file__).resolve().parents[3]


def _configure_logging(output_dir: Path, log_level: str) -> Path:
    log_path = output_dir / "run.log"
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode="w", encoding="utf-8")],
        force=True,
    )
    return log_path


def _get_process_created_at(pid: int) -> float | None:
    if pid <= 0 or os.name != "nt":
        return None
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return None
    try:
        created = ctypes.c_ulonglong()
        exited = ctypes.c_ulonglong()
        kernel = ctypes.c_ulonglong()
        user = ctypes.c_ulonglong()
        ok = kernel32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user))
        if not ok:
            return None
        return (created.value / 10_000_000.0) - 11644473600.0
    finally:
        kernel32.CloseHandle(handle)


def _pid_exists(pid: int, *, created_at: float | None = None) -> bool:
    if pid <= 0:
        return False
    created = _get_process_created_at(pid)
    if created is not None:
        return created_at is None or abs(created - float(created_at)) <= 5.0
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def _acquire_run_lock(output_dir: Path) -> Path:
    lock_path = output_dir / "run.lock"
    payload = {"pid": os.getpid(), "created_at": time.time()}
    for _ in range(8):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                current = json.loads(lock_path.read_text(encoding="utf-8"))
            except Exception:
                current = {}
            lock_pid = int(current.get("pid", 0) or 0)
            lock_created_at = float(current.get("created_at", 0) or 0)
            if lock_pid and _pid_exists(lock_pid, created_at=lock_created_at):
                raise RuntimeError(f"已有运行中的丹麦 Virk 进程: PID={lock_pid}")
            try:
                lock_path.unlink()
            except OSError:
                time.sleep(0.2)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
        return lock_path
    raise RuntimeError("无法获取丹麦 Virk 运行锁，请稍后重试。")


def _release_run_lock(lock_path: Path) -> None:
    if not lock_path.exists():
        return
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if int(payload.get("pid", 0) or 0) not in {0, os.getpid()}:
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="丹麦 Virk 协议爬虫")
    parser.add_argument("--output-dir", default="", help="输出目录")
    parser.add_argument("--max-companies", type=int, default=0, help="最大公司数")
    parser.add_argument("--skip-virk", action="store_true", help="跳过 Virk 抓取阶段")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 阶段")
    parser.add_argument("--skip-firecrawl", action="store_true", help="跳过 Firecrawl 阶段")
    parser.add_argument("--search-workers", type=int, default=2, help="Virk 搜索并发数")
    parser.add_argument("--detail-workers", type=int, default=2, help="Virk 详情并发数")
    parser.add_argument("--gmap-workers", type=int, default=32, help="Google Maps 并发数")
    parser.add_argument("--firecrawl-workers", type=int, default=96, help="Firecrawl 并发数")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def run_virk(argv: list[str]) -> int:
    args = _build_parser().parse_args(argv)
    output_dir = Path(args.output_dir).resolve() if str(args.output_dir).strip() else ROOT / "output" / "virk"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = _configure_logging(output_dir, args.log_level)
    logging.getLogger(__name__).info("运行日志已落盘：%s", log_path)
    lock_path = _acquire_run_lock(output_dir)
    atexit.register(_release_run_lock, lock_path)
    config = VirkDenmarkConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        max_companies=max(int(args.max_companies or 0), 0),
        search_workers=max(int(args.search_workers or 1), 1),
        detail_workers=max(int(args.detail_workers or 1), 1),
        gmap_workers=max(int(args.gmap_workers or 1), 1),
        firecrawl_workers=max(int(args.firecrawl_workers or 1), 1),
    )
    cookie_provider = VirkCookieProvider(project_root=ROOT, logger=logging.getLogger(__name__), allow_env_fallback=False)
    cookie_header = cookie_provider.get(force_refresh=True)
    if not cookie_header:
        raise RuntimeError("丹麦 Virk 启动失败：9222 浏览器未提供 Virk cookie。")
    client = VirkClient(
        cookie_header=cookie_header,
        cookie_provider=cookie_provider,
        base_url=config.virk_base_url,
        timeout_seconds=config.virk_timeout_seconds,
        proxy_url=config.virk_proxy_url,
    )
    client.min_interval_seconds = config.virk_min_interval_seconds
    client.rate_limit_retry_seconds = config.virk_rate_limit_retry_seconds
    try:
        run_virk_pipeline(
            config=config,
            client=client,
            skip_virk=bool(args.skip_virk),
            skip_gmap=bool(args.skip_gmap),
            skip_firecrawl=bool(args.skip_firecrawl),
        )
        return 0
    finally:
        _release_run_lock(lock_path)
