"""交付目录回收站工具。"""

from __future__ import annotations

import ctypes
import logging
import shutil
import sys
from ctypes import wintypes
from datetime import datetime
from pathlib import Path


LOGGER = logging.getLogger(__name__)
_FO_DELETE = 3
_FOF_SILENT = 0x0004
_FOF_NOCONFIRMATION = 0x0010
_FOF_ALLOWUNDO = 0x0040
_FOF_NOERRORUI = 0x0400


class _SHFILEOPSTRUCTW(ctypes.Structure):
    """Windows 回收站删除结构。"""

    _fields_ = [
        ("hwnd", wintypes.HWND),
        ("wFunc", wintypes.UINT),
        ("pFrom", wintypes.LPCWSTR),
        ("pTo", wintypes.LPCWSTR),
        ("fFlags", ctypes.c_uint16),
        ("fAnyOperationsAborted", wintypes.BOOL),
        ("hNameMappings", ctypes.c_void_p),
        ("lpszProgressTitle", wintypes.LPCWSTR),
    ]


def move_path_to_recycle_bin(target: Path) -> None:
    """将路径移动到系统回收站/废纸篓，而不是直接物理删除。"""
    path = Path(target)
    if not path.exists():
        return
    if _try_send2trash(path):
        return
    if sys.platform.startswith("win"):
        try:
            _move_to_windows_recycle_bin(path)
            return
        except OSError as exc:
            LOGGER.warning("Windows 回收站接口失败，回退用户 Trash 目录：%s | %s", path, exc)
    _move_to_user_trash(path)


def _try_send2trash(target: Path) -> bool:
    """优先使用现成库，避免自己处理平台细节。"""
    try:
        from send2trash import send2trash
    except ModuleNotFoundError:
        return False
    try:
        send2trash(str(target))
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("send2trash 失败，回退平台兜底逻辑：%s | %s", target, exc)
        return False


def _move_to_windows_recycle_bin(target: Path) -> None:
    """Windows 下调用系统回收站接口。"""
    operation = _SHFILEOPSTRUCTW()
    operation.wFunc = _FO_DELETE
    operation.pFrom = str(target) + "\0\0"
    operation.fFlags = _FOF_ALLOWUNDO | _FOF_NOCONFIRMATION | _FOF_NOERRORUI | _FOF_SILENT
    result = ctypes.windll.shell32.SHFileOperationW(ctypes.byref(operation))
    if result != 0:
        raise OSError(f"移动到回收站失败，系统错误码 {result}")
    if operation.fAnyOperationsAborted:
        raise OSError("移动到回收站被系统中止。")


def _move_to_user_trash(target: Path) -> None:
    """非 Windows 平台回退到用户废纸篓目录。"""
    trash_dir = _detect_user_trash_dir()
    trash_dir.mkdir(parents=True, exist_ok=True)
    destination = _build_unique_destination(trash_dir, target.name)
    shutil.move(str(target), str(destination))


def _detect_user_trash_dir() -> Path:
    """根据平台返回用户废纸篓目录。"""
    home = Path.home()
    if sys.platform.startswith("win"):
        return home / ".Trash" / "files"
    if sys.platform == "darwin":
        return home / ".Trash"
    return home / ".local" / "share" / "Trash" / "files"


def _build_unique_destination(trash_dir: Path, name: str) -> Path:
    """避免废纸篓内同名覆盖。"""
    candidate = trash_dir / name
    if not candidate.exists():
        return candidate
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = Path(name).stem
    suffix = Path(name).suffix
    return trash_dir / f"{stem}_{stamp}{suffix}"
