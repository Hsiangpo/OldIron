"""跨平台单键读取：把原始按键归一化成语义键或可见字符。

Windows 用 msvcrt（打包 exe 的主战场，最稳）；POSIX 用 termios 原始模式，
方向键尽力解析。read_key 接受可注入的 reader，便于单测菜单逻辑而不碰真实终端。
"""

from __future__ import annotations

import sys
from typing import Callable

UP = "UP"
DOWN = "DOWN"
LEFT = "LEFT"
RIGHT = "RIGHT"
ENTER = "ENTER"
ESC = "ESC"
BACKSPACE = "BACKSPACE"
CTRL_C = "CTRL_C"

_WIN_ARROWS = {"H": UP, "P": DOWN, "K": LEFT, "M": RIGHT}
_POSIX_ARROWS = {"A": UP, "B": DOWN, "D": LEFT, "C": RIGHT}


def read_key(reader: Callable[[], str] | None = None) -> str:
    """读取一次按键。reader 注入用于测试（直接返回语义键或字符）。"""
    if reader is not None:
        return reader()
    if sys.platform == "win32":
        return _read_key_windows()
    return _read_key_posix()


def _normalize(ch: str) -> str:
    if ch in ("\r", "\n"):
        return ENTER
    if ch == "\x1b":
        return ESC
    if ch == "\x03":
        return CTRL_C
    if ch in ("\b", "\x7f"):
        return BACKSPACE
    return ch


def _read_key_windows() -> str:
    import msvcrt

    ch = msvcrt.getwch()
    if ch in ("\x00", "\xe0"):
        return _WIN_ARROWS.get(msvcrt.getwch(), "")
    return _normalize(ch)


def _read_key_posix() -> str:
    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch != "\x1b":
            return _normalize(ch)
        # 方向键是 ESC [ A/B/C/D；仅当后面确有序列时再读，避免单独 ESC 卡住。
        if not select.select([sys.stdin], [], [], 0.05)[0]:
            return ESC
        if sys.stdin.read(1) != "[":
            return ESC
        if not select.select([sys.stdin], [], [], 0.05)[0]:
            return ESC
        return _POSIX_ARROWS.get(sys.stdin.read(1), ESC)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
