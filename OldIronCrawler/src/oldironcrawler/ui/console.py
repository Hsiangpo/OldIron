"""共享 rich.Console 单例 + 发丝线渲染原语。

整套 UI 只用这一个 Console（唯一输出主），保证主题统一、Live 不被多 console 干扰。
这里只放跨界面复用的小渲染件（字标、发丝线、键值块）；
界面专属的渲染逻辑放在各自模块（menu / crawl_view）。
"""

from __future__ import annotations

import os

from rich.console import Console, Group, RenderableType
from rich.padding import Padding
from rich.table import Table
from rich.text import Text

from oldironcrawler.ui.theme import DOT, HAIR, UI_THEME

# 内容最大宽度：发丝线/留白不再铺满整个终端，避免宽窗口里换行（会把 Live 帧高算错而堆叠），
# 也消除大段尾部空格，左对齐更"克制"。
_MAX_WIDTH = 72

_console: Console | None = None


def get_console() -> Console:
    """返回全局唯一 Console。"""
    global _console
    if _console is None:
        _console = Console(theme=UI_THEME, highlight=False, soft_wrap=False, emoji=False)
    return _console


def content_width() -> int:
    return max(min(get_console().width - 4, _MAX_WIDTH), 8)


def clear_screen() -> None:
    """硬清屏：连滚动历史一起清掉。

    rich 的 console.clear()（\x1b[2J）在部分 Windows conhost 上从信息屏切回菜单时
    擦不干净、会留下上一屏残影；用 cls/clear 与旧版一致地彻底清屏。
    """
    os.system("cls" if os.name == "nt" else "clear")


def hairline() -> Text:
    """限定宽度的发丝细线（不铺满终端）。"""
    return Text(HAIR * content_width(), style="hair")


def wordmark(title: str, subtitle: str = "") -> Group:
    """品牌字标 + 副标题。"""
    parts: list[RenderableType] = [Text(title, style="wordmark")]
    if subtitle:
        parts.append(Text(subtitle, style="label"))
    return Group(*parts)


def kv_block(rows: list[tuple[str, str]], *, value_style: str = "value") -> Table:
    """左标签右值的对齐键值块（发丝线规格表的主体）。"""
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="label", justify="left", no_wrap=True)
    grid.add_column(style=value_style, justify="left", overflow="fold")
    for label, value in rows:
        grid.add_row(label, value)
    return grid


def inline_stats(pairs: list[tuple[str, str]]) -> Text:
    """把若干 (标签, 值) 串成一行：标签 值 · 标签 值 · …"""
    text = Text()
    for index, (label, value) in enumerate(pairs):
        if index:
            text.append(f"  {DOT} ", style="hair")
        text.append(f"{label} ", style="label")
        text.append(value, style="value.strong")
    return text


def screen(*renderables: RenderableType, pad: tuple[int, int] = (1, 2)) -> Padding:
    """给整屏内容统一加留白外边距；expand=False 让块只占内容宽度，不铺满终端。"""
    return Padding(Group(*renderables), (pad[0], pad[1], pad[0], pad[1]), expand=False)
