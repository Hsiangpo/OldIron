"""IEATPE CLI。"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import time
from pathlib import Path

from .config import IeatpeConfig
from .pipeline import run_pipeline


SITE_ROOT = Path(__file__).resolve().parents[4]
logger = logging.getLogger("ieatpe.cli")


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
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
            if lock_pid and _pid_exists(lock_pid):
                raise RuntimeError(f"已有运行中的 Taiwan IEATPE 进程: PID={lock_pid}")
            try:
                lock_path.unlink()
            except OSError:
                time.sleep(0.2)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
        return lock_path
    raise RuntimeError("无法获取 Taiwan IEATPE 运行锁，请稍后重试。")


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


def run_ieatpe(argv: list[str]) -> int:
    """IEATPE 站点执行入口。"""
    parser = argparse.ArgumentParser(description="IEATPE 会员资料查询采集")
    parser.add_argument(
        "mode",
        nargs="?",
        default="all",
        choices=["all", "list", "detail"],
        help="运行模式: all=列表+详情并发, list=只跑字母列表, detail=只跑详情补全",
    )
    parser.add_argument("--letters", default="", help="字母列表，如 A,B,C；默认 A-Z")
    parser.add_argument("--flow", default="12", help="查询 flow，默认 12")
    parser.add_argument("--list-workers", type=int, default=4, help="列表并发数（默认 4）")
    parser.add_argument("--detail-workers", type=int, default=12, help="详情并发数（默认 12）")
    parser.add_argument("--delay", type=float, default=0.2, help="请求间隔秒数（默认 0.2）")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（默认 INFO）",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = IeatpeConfig.from_env(
        project_root=SITE_ROOT,
        output_dir=SITE_ROOT / "output" / "ieatpe",
        letters=args.letters,
        flow=args.flow,
        list_workers=args.list_workers,
        detail_workers=args.detail_workers,
        request_delay=args.delay,
    )
    config.output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = _acquire_run_lock(config.output_dir)
    atexit.register(_release_run_lock, lock_path)
    logger.info("IEATPE 启动：letters=%s flow=%s", ",".join(config.letters), config.flow)
    results = run_pipeline(
        config=config,
        only_list=args.mode == "list",
        only_detail=args.mode == "detail",
    )
    print(f"\n完成: {results}")
    return 0
