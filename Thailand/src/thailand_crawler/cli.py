"""CLI 入口。"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import time
from pathlib import Path

from thailand_crawler.client import DnbClient
from thailand_crawler.config import DEFAULT_DETAIL_CONCURRENCY
from thailand_crawler.config import DEFAULT_GMAP_CONCURRENCY
from thailand_crawler.config import DEFAULT_SNOV_CONCURRENCY
from thailand_crawler.streaming.pipeline import run_stream_pipeline


ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger(__name__)


def _configure_logging(output_dir: Path, log_level: str) -> Path:
    log_path = output_dir / 'run.log'
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, mode='w', encoding='utf-8'),
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
    lock_path = output_dir / 'run.lock'
    pid = os.getpid()
    payload = {'pid': pid, 'created_at': time.time()}
    for _ in range(8):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                current = json.loads(lock_path.read_text(encoding='utf-8'))
            except Exception:
                current = {}
            lock_pid = int(current.get('pid', 0) or 0)
            if lock_pid and lock_pid != pid and _pid_exists(lock_pid):
                raise RuntimeError(f'已有运行中的 DNB 进程: PID={lock_pid}')
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except PermissionError:
                time.sleep(0.2)
                continue
            continue
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fp:
                json.dump(payload, fp, ensure_ascii=False)
        except Exception:
            try:
                lock_path.unlink()
            except Exception:
                pass
            raise
        return lock_path
    raise RuntimeError('无法获取 DNB 运行锁，请稍后重试。')


def _release_run_lock(lock_path: Path) -> None:
    if not lock_path.exists():
        return
    try:
        payload = json.loads(lock_path.read_text(encoding='utf-8'))
    except Exception:
        payload = {}
    lock_pid = int(payload.get('pid', 0) or 0)
    if lock_pid and lock_pid != os.getpid():
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Thailand D&B 协议爬虫')
    parser.add_argument('site', choices=['dnb'], help='站点名')
    parser.add_argument('--max-companies', type=int, default=0, help='最大 D&B 公司数')
    parser.add_argument('--max-items', type=int, default=0, help='兼容旧参数，映射到 --max-companies')
    parser.add_argument('--skip-dnb', action='store_true', help='跳过 D&B 生产阶段')
    parser.add_argument('--skip-website', action='store_true', help='跳过官网解析阶段')
    parser.add_argument('--skip-site', action='store_true', help='跳过 site 公司名抽取阶段')
    parser.add_argument('--skip-snov', action='store_true', help='跳过 Snov 邮箱阶段')
    parser.add_argument('--skip-segments', action='store_true', help='兼容旧参数，映射到 --skip-dnb')
    parser.add_argument('--skip-list', action='store_true', help='兼容旧参数，映射到 --skip-dnb')
    parser.add_argument('--skip-detail', action='store_true', help='兼容旧参数，映射到 --skip-dnb')
    parser.add_argument('--skip-gmap', action='store_true', help='兼容旧参数，映射到 --skip-website')
    parser.add_argument('--dnb-workers', type=int, default=0, help='D&B 详情并发数')
    parser.add_argument('--website-workers', type=int, default=0, help='官网解析并发数')
    parser.add_argument('--site-workers', type=int, default=16, help='site 抽取并发数')
    parser.add_argument('--snov-workers', type=int, default=0, help='Snov 并发数')
    parser.add_argument('--detail-concurrency', type=int, default=DEFAULT_DETAIL_CONCURRENCY, help='兼容旧参数，映射到 --dnb-workers')
    parser.add_argument('--gmap-concurrency', type=int, default=DEFAULT_GMAP_CONCURRENCY, help='兼容旧参数，映射到 --website-workers')
    parser.add_argument('--snov-concurrency', type=int, default=DEFAULT_SNOV_CONCURRENCY, help='兼容旧参数，映射到 --snov-workers')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    return parser


def run_cli(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_dir = ROOT / 'output' / 'dnb_stream'
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = _configure_logging(output_dir, args.log_level)
    logging.getLogger(__name__).info('运行日志已落盘：%s', log_path)
    lock_path = _acquire_run_lock(output_dir)
    atexit.register(_release_run_lock, lock_path)

    max_companies = max(int(args.max_companies or args.max_items or 0), 0)
    dnb_workers = max(int(args.dnb_workers or args.detail_concurrency or 1), 1)
    website_workers = max(int(args.website_workers or args.gmap_concurrency or 1), 1)
    site_workers = max(int(args.site_workers or 1), 1)
    snov_workers = max(int(args.snov_workers or args.snov_concurrency or 1), 1)
    skip_dnb = bool(args.skip_dnb or args.skip_segments or args.skip_list or args.skip_detail)
    skip_website = bool(args.skip_website or args.skip_gmap)
    skip_site = bool(args.skip_site)
    skip_snov = bool(args.skip_snov)

    client = DnbClient(cookie_header=os.getenv('DNB_COOKIE_HEADER', ''))
    try:
        run_stream_pipeline(
            project_root=ROOT,
            output_dir=output_dir,
            client=client,
            max_companies=max_companies,
            dnb_workers=dnb_workers,
            website_workers=website_workers,
            site_workers=site_workers,
            snov_workers=snov_workers,
            skip_dnb=skip_dnb,
            skip_website=skip_website,
            skip_site=skip_site,
            skip_snov=skip_snov,
        )
        return 0
    finally:
        _release_run_lock(lock_path)
