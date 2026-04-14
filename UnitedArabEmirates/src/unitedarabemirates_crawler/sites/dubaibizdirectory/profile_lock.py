"""Chrome profile 锁清理。"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from pathlib import Path


_LOCK_NAMES = ("SingletonLock", "SingletonCookie", "SingletonSocket")


def cleanup_profile_runtime(user_data_dir: Path) -> None:
    """清理 crawler 专用 profile 的残留浏览器进程和锁文件。"""
    user_data_dir = Path(user_data_dir)
    for pid in _find_profile_process_ids(user_data_dir):
        _terminate_process(pid)
    cleanup_stale_profile_locks(user_data_dir)
    for name in _LOCK_NAMES:
        _safe_unlink(user_data_dir / name)


def cleanup_stale_profile_locks(user_data_dir: Path) -> None:
    """清理已失效的 Chrome profile 锁。"""
    user_data_dir = Path(user_data_dir)
    lock_path = user_data_dir / "SingletonLock"
    if not lock_path.exists():
        return
    target = _read_lock_target(lock_path)
    if not target:
        return
    pid = _extract_pid(target)
    host = _extract_host(target)
    if pid is None or not _is_local_host(host) or _process_alive(pid):
        return
    for name in _LOCK_NAMES:
        _safe_unlink(user_data_dir / name)


def _read_lock_target(lock_path: Path) -> str:
    try:
        if lock_path.is_symlink():
            return str(os.readlink(lock_path)).strip()
        return lock_path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _extract_pid(target: str) -> int | None:
    tail = str(target or "").rsplit("-", 1)[-1].strip()
    return int(tail) if tail.isdigit() else None


def _extract_host(target: str) -> str:
    parts = str(target or "").rsplit("-", 1)
    return parts[0].strip() if parts else ""


def _is_local_host(host: str) -> bool:
    host = str(host or "").strip().lower()
    if not host:
        return False
    local_names = {
        socket.gethostname().lower(),
        socket.getfqdn().lower(),
        "localhost",
    }
    return host in local_names


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _safe_unlink(path: Path) -> None:
    try:
        if path.exists() or path.is_symlink():
            path.unlink()
    except FileNotFoundError:
        return


def _find_profile_process_ids(user_data_dir: Path) -> list[int]:
    """找到正在占用当前 crawler profile 的 Chrome 进程。"""
    marker = f"--user-data-dir={Path(user_data_dir).resolve()}"
    current_pid = os.getpid()
    try:
        completed = subprocess.run(
            ["ps", "-Ao", "pid=,command="],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except Exception:
        return []
    process_ids: list[int] = []
    for line in str(completed.stdout or "").splitlines():
        if marker not in line:
            continue
        pieces = line.strip().split(None, 1)
        if not pieces or not pieces[0].isdigit():
            continue
        pid = int(pieces[0])
        if pid != current_pid and pid not in process_ids:
            process_ids.append(pid)
    return process_ids


def _terminate_process(pid: int) -> None:
    """优先温和结束残留进程，必要时强杀。"""
    if not _process_alive(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        return
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if not _process_alive(pid):
            return
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        return
