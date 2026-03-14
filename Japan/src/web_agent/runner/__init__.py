from __future__ import annotations

import asyncio
import codecs
import os
import sys
import threading
import time
from pathlib import Path
from typing import Iterable

_JOB_LOG_MAX_LINES = 20000
_JOB_LOG_TRIM_INTERVAL = 2.0
_JOB_LOG_LOCK = threading.Lock()
_JOB_LOG_LAST_TRIM: dict[str, float] = {}


def _is_frozen_exe() -> bool:
    return bool(getattr(sys, "frozen", False)) or bool(getattr(sys, "_MEIPASS", None))

def _decode_subprocess_line(data: bytes) -> str:
    """
    子进程输出在 Windows 上可能是 UTF-8 或本地代码页（如 GBK）。
    这里优先按 UTF-8 解码，失败则退回 GBK，尽量避免日志里出现 ��/锟斤拷。
    """
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return data.decode("gbk")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")


def _mask_args(args: list[str]) -> list[str]:
    masked: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            masked.append("***")
            skip_next = False
            continue
        lower = arg.lower()
        if lower in (
            "--cookie",
            "--llm-api-key",
            "--search-pb",
            "--search-sourceurl",
            "--snov-extension-token",
            "--snov-extension-selector",
            "--snov-extension-fingerprint",
        ):
            masked.append(arg)
            skip_next = True
            continue
        if lower.startswith("sk-") and len(arg) > 12:
            masked.append("sk-***")
            continue
        masked.append(arg)
    return masked


async def run_subprocess(
    args: list[str],
    log_path: Path,
    label: str,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    # 强制子进程输出 UTF-8，避免 Windows 默认代码页导致的乱码。
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    if extra_env:
        env.update(extra_env)

    proc: asyncio.subprocess.Process | None = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(cwd) if cwd else None,
            env=env,
        )

        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            _append_log(log_path, _decode_subprocess_line(line))
        code = await proc.wait()
        return int(code)
    except asyncio.CancelledError:
        _append_log(log_path, f"[web] 已取消：{label}\n")
        if proc and proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass
        raise


def python_module_args(module: str, extra: Iterable[str]) -> list[str]:
    # 打包成 .exe 后（PyInstaller），sys.executable 不再是 python 解释器，无法使用 `-m`。
    # 此时通过同一个 exe 的子命令模式来执行 gmap/site。
    if _is_frozen_exe():
        if module == "gmap_agent":
            return [sys.executable, "gmap", *list(extra)]
        if module == "site_agent":
            return [sys.executable, "site", *list(extra)]
    return [sys.executable, "-m", module, *list(extra)]

def _trim_log(path: Path) -> None:
    if _JOB_LOG_MAX_LINES <= 0:
        return
    now = time.time()
    key = str(path)
    last = _JOB_LOG_LAST_TRIM.get(key)
    if last and (now - last) < _JOB_LOG_TRIM_INTERVAL:
        return
    _JOB_LOG_LAST_TRIM[key] = now
    try:
        data = path.read_bytes()
    except Exception:
        return
    if not data:
        return
    line_count = data.count(b"\n")
    if data and not data.endswith(b"\n"):
        line_count += 1
    if line_count <= _JOB_LOG_MAX_LINES:
        return
    bom = b""
    if data.startswith(codecs.BOM_UTF8):
        bom = codecs.BOM_UTF8
        data = data[len(codecs.BOM_UTF8) :]
    lines = data.splitlines(keepends=True)
    if len(lines) <= _JOB_LOG_MAX_LINES:
        return
    try:
        path.write_bytes(bom + b"".join(lines[-_JOB_LOG_MAX_LINES:]))
    except Exception:
        return


def _append_log(path: Path, text: str) -> None:
    # ?? utf-8-sig????????? BOM??? Windows ????? UTF-8?
    # ?????????? BOM?
    with _JOB_LOG_LOCK:
        with path.open("a", encoding="utf-8-sig") as f:
            f.write(text)
        _trim_log(path)

