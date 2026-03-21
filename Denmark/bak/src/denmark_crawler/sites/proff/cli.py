"""Proff CLI。"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import time
from pathlib import Path

from denmark_crawler.backend.cli import ensure_services_started
from denmark_crawler.backend.cli import stop_service_names
from denmark_crawler.sites.proff.client import ProffClient
from denmark_crawler.sites.proff.config import ProffDenmarkConfig
from denmark_crawler.sites.proff.config import resolve_query_file
from denmark_crawler.sites.proff.pipeline import run_proff_pipeline


ROOT = Path(__file__).resolve().parents[4]


def _configure_logging(output_dir: Path, log_level: str) -> Path:
    log_path = output_dir / "run.log"
    logging.basicConfig(
        level=getattr(logging, str(log_level or "INFO").upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode="w", encoding="utf-8")],
        force=True,
    )
    return log_path


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
                raise RuntimeError(f"已有运行中的丹麦 Proff 进程: PID={lock_pid}")
            try:
                lock_path.unlink()
            except OSError:
                time.sleep(0.2)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
        return lock_path
    raise RuntimeError("无法获取丹麦 Proff 运行锁，请稍后重试。")


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
    parser = argparse.ArgumentParser(description="丹麦 Proff 搜索爬虫")
    parser.add_argument("--output-dir", default="", help="输出目录")
    parser.add_argument("--query-file", default="", help="自定义 query 文件，每行一个关键词")
    parser.add_argument("--query", action="append", default=[], help="直接追加 query，可重复传参")
    parser.add_argument("--max-pages-per-query", type=int, default=400, help="每个 query 最大抓取页数")
    parser.add_argument("--max-companies", type=int, default=0, help="最大公司数")
    parser.add_argument("--search-workers", type=int, default=16, help="搜索页并发数")
    parser.add_argument("--gmap-workers", type=int, default=16, help="Google Maps 并发数")
    parser.add_argument("--firecrawl-workers", type=int, default=64, help="Firecrawl 并发数")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 补官网阶段")
    parser.add_argument("--skip-firecrawl", action="store_true", help="跳过 Firecrawl 补邮箱阶段")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def run_proff(argv: list[str]) -> int:
    args = _build_parser().parse_args(argv)
    output_dir = Path(args.output_dir).resolve() if str(args.output_dir).strip() else ROOT / "output" / "proff"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = _configure_logging(output_dir, args.log_level)
    logging.getLogger(__name__).info("运行日志已落盘：%s", log_path)
    lock_path = _acquire_run_lock(output_dir)
    atexit.register(_release_run_lock, lock_path)
    query_file = resolve_query_file(ROOT, str(args.query_file or "").strip())
    config = ProffDenmarkConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        query_file=query_file,
        inline_queries=[str(item).strip() for item in list(args.query or []) if str(item).strip()],
        max_pages_per_query=max(int(args.max_pages_per_query or 1), 1),
        max_companies=max(int(args.max_companies or 0), 0),
        search_workers=max(int(args.search_workers or 1), 1),
        gmap_workers=max(int(args.gmap_workers or 1), 1),
        firecrawl_workers=max(int(args.firecrawl_workers or 1), 1),
    )
    config.validate(skip_firecrawl=bool(args.skip_firecrawl))
    auto_started = _auto_start_go_backends(
        config=config,
        skip_gmap=bool(args.skip_gmap),
        skip_firecrawl=bool(args.skip_firecrawl),
    )
    if auto_started:
        atexit.register(stop_service_names, auto_started, quiet=True)
    client = ProffClient(
        base_url=config.base_url,
        timeout_seconds=config.timeout_seconds,
        proxy_url=config.proxy_url,
        min_interval_seconds=config.min_interval_seconds,
    )
    try:
        run_proff_pipeline(
            config,
            client,
            skip_gmap=bool(args.skip_gmap),
            skip_firecrawl=bool(args.skip_firecrawl),
        )
        return 0
    finally:
        if auto_started:
            stop_service_names(auto_started, quiet=True)
        _release_run_lock(lock_path)


def _auto_start_go_backends(
    *,
    config: ProffDenmarkConfig,
    skip_gmap: bool,
    skip_firecrawl: bool,
) -> list[str]:
    if not config.prefer_go_backends:
        return []
    services: list[str] = []
    if not skip_gmap:
        services.append("gmap")
    if not skip_firecrawl:
        services.append("firecrawl")
    if not services:
        return []
    started = ensure_services_started(services, quiet=True)
    if started:
        logging.getLogger(__name__).info("已自动启动 Go 后端：%s", ", ".join(started))
    return started
