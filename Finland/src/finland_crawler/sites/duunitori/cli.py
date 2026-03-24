"""Duunitori CLI。"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import signal
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

from finland_crawler.sites.duunitori.client import DuunitoriClient
from finland_crawler.sites.duunitori.config import DuunitoriConfig
from finland_crawler.sites.duunitori.pipeline import run_duunitori_pipeline


ROOT = Path(__file__).resolve().parents[4]
VERSATILE_ROOT = ROOT.parent / "VersatileBackend"
BACKEND_OUTPUT = ROOT / "output" / "backend"

GMAP_DEF = {
    "cmd": ["go", "run", "./cmd/gmap-service"],
    "addr": "http://127.0.0.1:8082",
    "log": "gmap-service.log",
    "pid": "gmap-service.pid",
    "env": {"GMAP_SERVICE_ADDR": ":8082"},
}

LOGGER = logging.getLogger(__name__)


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
                raise RuntimeError(f"已有运行中的 Duunitori 进程: PID={lock_pid}")
            try:
                lock_path.unlink()
            except OSError:
                time.sleep(0.2)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
        return lock_path
    raise RuntimeError("无法获取 Duunitori 运行锁")


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


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _gmap_is_running() -> bool:
    pid_path = BACKEND_OUTPUT / GMAP_DEF["pid"]
    if not pid_path.exists():
        return False
    try:
        payload = json.loads(pid_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    pid = int(payload.get("pid", 0) or 0)
    return pid > 0 and _pid_exists(pid)


def _gmap_health_ok() -> bool:
    try:
        resp = urlopen(f"{GMAP_DEF['addr']}/healthz", timeout=3)  # noqa: S310
        return resp.status == 200
    except Exception:
        return False


def _start_gmap_backend() -> bool:
    if _gmap_is_running() and _gmap_health_ok():
        LOGGER.info("Go GMap 后端已在运行")
        return False
    BACKEND_OUTPUT.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    for key, value in _load_env_file(VERSATILE_ROOT / ".env").items():
        env.setdefault(key, value)
    proxy = env.get("PROXY_URL", "").strip() or "http://127.0.0.1:7897"
    env.setdefault("HTTP_PROXY", proxy)
    env.setdefault("HTTPS_PROXY", proxy)
    for key, value in GMAP_DEF["env"].items():
        env[key] = value
    log_path = BACKEND_OUTPUT / GMAP_DEF["log"]
    pid_path = BACKEND_OUTPUT / GMAP_DEF["pid"]
    LOGGER.info("正在启动 Go GMap 后端...")
    with log_path.open("ab") as log_fp:
        process = subprocess.Popen(  # noqa: S603
            GMAP_DEF["cmd"], cwd=str(VERSATILE_ROOT), env=env, stdout=log_fp, stderr=log_fp,
        )
    pid_path.write_text(json.dumps({"pid": process.pid}), encoding="utf-8")
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if _gmap_health_ok():
            LOGGER.info("Go GMap 后端已启动：PID=%s", process.pid)
            return True
        time.sleep(0.5)
    LOGGER.warning("Go GMap 后端启动超时")
    return True


def _stop_gmap_backend() -> None:
    pid_path = BACKEND_OUTPUT / GMAP_DEF["pid"]
    if not pid_path.exists():
        return
    try:
        payload = json.loads(pid_path.read_text(encoding="utf-8"))
    except Exception:
        return
    pid = int(payload.get("pid", 0) or 0)
    if pid <= 0 or not _pid_exists(pid):
        pid_path.unlink(missing_ok=True)
        return
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(20):
            if not _pid_exists(pid):
                break
            time.sleep(0.25)
        else:
            os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    pid_path.unlink(missing_ok=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="芬兰 Duunitori 爬虫")
    parser.add_argument("--output-dir", default="", help="输出目录")
    parser.add_argument("--detail-workers", type=int, default=8)
    parser.add_argument("--gmap-workers", type=int, default=64)
    parser.add_argument("--email-workers", dest="firecrawl_workers", type=int, default=8)
    parser.add_argument("--skip-gmap", action="store_true")
    parser.add_argument("--skip-email", dest="skip_firecrawl", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser


def run_duunitori(argv: list[str]) -> int:
    """Duunitori CLI 主入口。"""
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(65536, hard)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

    args = _build_parser().parse_args(argv)
    output_dir = Path(args.output_dir).resolve() if str(args.output_dir).strip() else ROOT / "output" / "duunitori"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = _configure_logging(output_dir, args.log_level)
    LOGGER.info("运行日志：%s", log_path)
    lock_path = _acquire_run_lock(output_dir)
    atexit.register(_release_run_lock, lock_path)

    gmap_auto_started = False
    if not args.skip_gmap:
        gmap_auto_started = _start_gmap_backend()
        if gmap_auto_started:
            atexit.register(_stop_gmap_backend)

    config = DuunitoriConfig.from_env(
        project_root=ROOT, output_dir=output_dir,
        detail_workers=max(int(args.detail_workers or 1), 1),
        gmap_workers=max(int(args.gmap_workers or 1), 1),
        firecrawl_workers=max(int(args.firecrawl_workers or 1), 1),
    )
    client = DuunitoriClient(timeout_seconds=config.timeout_seconds, proxy_url=config.proxy_url)

    try:
        run_duunitori_pipeline(config, client, skip_gmap=bool(args.skip_gmap), skip_firecrawl=bool(args.skip_firecrawl))
        return 0
    finally:
        if gmap_auto_started:
            _stop_gmap_backend()
        _release_run_lock(lock_path)
