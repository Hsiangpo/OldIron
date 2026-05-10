"""CNPJ Biz 后台监控与自恢复。"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .clash_controller import ClashUnixController
from .config import CnpjBizConfig
from .pipeline import _START_URL
from .store import CnpjBizProgress
from .store import CnpjBizStore


LOGGER = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[4]


@dataclass(slots=True)
class SupervisorSettings:
    project_root: Path
    output_dir: Path
    poll_seconds: float = 30.0
    log_interval_seconds: float = 120.0
    detail_workers: int = 8
    log_level: str = "INFO"


def run_supervisor(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="CNPJ Biz 后台监控")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    parser.add_argument("--log-interval-seconds", type=float, default=120.0)
    parser.add_argument("--detail-workers", type=int, default=8)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    settings = SupervisorSettings(
        project_root=ROOT,
        output_dir=ROOT / "output" / "cnpjbiz",
        poll_seconds=max(float(args.poll_seconds or 1.0), 5.0),
        log_interval_seconds=max(float(args.log_interval_seconds or 1.0), 10.0),
        detail_workers=max(int(args.detail_workers or 1), 1),
        log_level=args.log_level,
    )
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    supervisor = CnpjBizSupervisor(settings)
    supervisor.run_forever()
    return 0


class CnpjBizSupervisor:
    """监控 CNPJ Biz 工作进程，必要时自动重启。"""

    def __init__(self, settings: SupervisorSettings) -> None:
        self._settings = settings
        self._store = CnpjBizStore(settings.output_dir / "cnpjbiz_store.db")
        self._pid_path = settings.output_dir / "run.pid"
        self._stop_requested = False
        self._last_clash_choice = ""
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def run_forever(self) -> None:
        last_log_at = 0.0
        while not self._stop_requested:
            pid = _read_pid(self._pid_path)
            alive = _is_pid_alive(pid)
            progress = self._store.progress()
            if not alive:
                self._store.requeue_running_tasks()
                progress = self._store.progress()
                mode = _choose_run_mode(progress)
                if mode == "all" and not self._homepage_accessible():
                    self._rotate_clash_if_enabled()
                    mode = "wait"
                if mode == "all" and _is_fully_drained(progress):
                    self._seed_new_home_cycle()
                    progress = self._store.progress()
                if mode in {"all", "detail"}:
                    new_pid = self._start_worker(mode)
                    LOGGER.info("CNPJ Biz supervisor 已启动子进程：mode=%s pid=%s", mode, new_pid)
                else:
                    LOGGER.info("CNPJ Biz supervisor 暂不启动：等待首页恢复。")
            now = time.monotonic()
            if now - last_log_at >= self._settings.log_interval_seconds:
                self._log_progress(self._store.progress())
                last_log_at = now
            time.sleep(self._settings.poll_seconds)
        self._store.close()

    def _seed_new_home_cycle(self) -> None:
        self._store.reset_list_queue_for_new_cycle()
        self._store.seed_start_page(_START_URL)

    def _homepage_accessible(self) -> bool:
        config = CnpjBizConfig.from_env(
            project_root=self._settings.project_root,
            output_dir=self._settings.output_dir,
            list_workers=1,
            detail_workers=1,
            max_pages=0,
        )
        from .client import CnpjBizClient

        client = CnpjBizClient(config)
        try:
            page = client.fetch_list_page(_START_URL)
            return bool(page.records)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz supervisor 首页探活失败：%s", exc)
            return False
        finally:
            client.close()

    def _rotate_clash_if_enabled(self) -> None:
        config = CnpjBizConfig.from_env(
            project_root=self._settings.project_root,
            output_dir=self._settings.output_dir,
            list_workers=1,
            detail_workers=1,
            max_pages=0,
        )
        if not config.clash_rotate_enabled:
            return
        try:
            controller = ClashUnixController(config.clash_unix_socket_path)
            picked = controller.cycle_selector(
                config.clash_selector_name,
                ignore={"DIRECT"},
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz supervisor Clash 轮换失败：%s", exc)
            return
        if picked != self._last_clash_choice:
            LOGGER.info("CNPJ Biz supervisor 已切换 Clash 节点：selector=%s choice=%s", config.clash_selector_name, picked)
            self._last_clash_choice = picked

    def _start_worker(self, mode: str) -> int:
        log_path = self._settings.output_dir / "runtime.log"
        log_fp = log_path.open("a", encoding="utf-8")
        proc = subprocess.Popen(  # noqa: S603
            [
                str(self._settings.project_root / ".venv" / "bin" / "python"),
                "run.py",
                "cnpjbiz",
                mode,
                "--detail-workers",
                str(self._settings.detail_workers),
                "--log-level",
                self._settings.log_level,
            ],
            cwd=str(self._settings.project_root),
            stdin=subprocess.DEVNULL,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
        self._pid_path.write_text(str(proc.pid), encoding="utf-8")
        return proc.pid

    def _log_progress(self, progress: CnpjBizProgress) -> None:
        LOGGER.info(
            "CNPJ Biz supervisor 进度：list_pending=%d list_running=%d list_done=%d detail_pending=%d detail_running=%d companies=%d final=%d",
            progress.list_pending,
            progress.list_running,
            progress.list_done,
            progress.detail_pending,
            progress.detail_running,
            progress.companies_total,
            progress.final_total,
        )

    def _handle_stop(self, signum, frame) -> None:  # noqa: ANN001
        _ = signum, frame
        self._stop_requested = True


def _read_pid(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    text = str(pid_path.read_text(encoding="utf-8", errors="ignore") or "").strip()
    return int(text) if text.isdigit() else None


def _is_pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _choose_run_mode(progress: CnpjBizProgress) -> str:
    if progress.detail_pending > 0 or progress.detail_running > 0:
        return "detail"
    if progress.list_pending > 0 or progress.list_running > 0:
        return "all"
    return "all"


def _is_fully_drained(progress: CnpjBizProgress) -> bool:
    return (
        progress.list_pending == 0
        and progress.list_running == 0
        and progress.detail_pending == 0
        and progress.detail_running == 0
    )
