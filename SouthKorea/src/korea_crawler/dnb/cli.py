"""韩国 DNB CLI 入口。"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import time
from pathlib import Path

from korea_crawler.dnb.browser_cookie import DnbCookieProvider
from korea_crawler.dnb.browser_cookie import resolve_dnb_cookie_header
from korea_crawler.dnb.client import DnbClient
from korea_crawler.dnb.config import DnbKoreaConfig
from korea_crawler.dnb.pipeline import run_dnbkorea_pipeline


ROOT = Path(__file__).resolve().parents[3]


def _configure_logging(output_dir: Path, log_level: str) -> Path:
    log_path = output_dir / "run.log"
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return log_path


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _acquire_run_lock(output_dir: Path) -> Path:
    lock_path = output_dir / "run.lock"
    pid = os.getpid()
    payload = {"pid": pid, "created_at": time.time()}
    for _ in range(8):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                current = json.loads(lock_path.read_text(encoding="utf-8"))
            except Exception:
                current = {}
            lock_pid = int(current.get("pid", 0) or 0)
            if lock_pid and lock_pid != pid and _pid_exists(lock_pid):
                raise RuntimeError(f"已有运行中的 DNB Korea 进程: PID={lock_pid}")
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except PermissionError:
                time.sleep(0.2)
                continue
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
        return lock_path
    raise RuntimeError("无法获取 DNB Korea 运行锁，请稍后重试。")


def _release_run_lock(lock_path: Path) -> None:
    if not lock_path.exists():
        return
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    lock_pid = int(payload.get("pid", 0) or 0)
    if lock_pid and lock_pid != os.getpid():
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="韩国 DNB Construction 协议爬虫")
    parser.add_argument("--max-companies", type=int, default=0, help="最大 DNB 公司数")
    parser.add_argument("--skip-dnb", action="store_true", help="跳过 DNB 生产阶段")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 韩文名与官网阶段")
    parser.add_argument("--skip-site-name", action="store_true", help="跳过 Firecrawl 官网韩文名阶段")
    parser.add_argument("--skip-snov", action="store_true", help="跳过 Snov 邮箱阶段")
    parser.add_argument("--dnb-pipeline-workers", type=int, default=8, help="DNB pipeline 并发数")
    parser.add_argument("--dnb-workers", type=int, default=8, help="DNB 详情并发数")
    parser.add_argument("--gmap-workers", type=int, default=32, help="Google Maps 并发数")
    parser.add_argument("--site-workers", type=int, default=16, help="Firecrawl 官网韩文名并发数")
    parser.add_argument("--snov-workers", type=int, default=6, help="Snov 并发数")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def run_dnbkorea(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_dir = ROOT / "output" / "dnbkorea"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = _configure_logging(output_dir, args.log_level)
    logging.getLogger(__name__).info("运行日志已落盘：%s", log_path)
    lock_path = _acquire_run_lock(output_dir)
    atexit.register(_release_run_lock, lock_path)
    config = DnbKoreaConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        max_companies=max(int(args.max_companies or 0), 0),
        dnb_pipeline_workers=max(int(args.dnb_pipeline_workers or 1), 1),
        dnb_workers=max(int(args.dnb_workers or 1), 1),
        gmap_workers=max(int(args.gmap_workers or 1), 1),
        site_workers=max(int(args.site_workers or 1), 1),
        snov_workers=max(int(args.snov_workers or 1), 1),
    )
    cookie_provider = DnbCookieProvider(project_root=ROOT, logger=logging.getLogger(__name__))
    cookie_header = cookie_provider.get(force_refresh=True)
    client = DnbClient(cookie_header=cookie_header, cookie_provider=cookie_provider)
    try:
        run_dnbkorea_pipeline(
            config=config,
            client=client,
            skip_dnb=bool(args.skip_dnb),
            skip_gmap=bool(args.skip_gmap),
            skip_site_name=bool(args.skip_site_name),
            skip_snov=bool(args.skip_snov),
        )
        return 0
    finally:
        _release_run_lock(lock_path)
