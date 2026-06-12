"""共享 rich.Console 单例 + 发丝线渲染原语。

整套 UI 只用这一个 Console（唯一输出主），保证主题统一、Live 不被多 console 干扰。
这里只放跨界面复用的小渲染件（字标、发丝线、键值块）；
界面专属的渲染逻辑放在各自模块（menu / crawl_view）。
"""

from __future__ import annotations

from rich.console import Console, Group, RenderableType
from rich.padding import Padding
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from oldironcrawler.ui.theme import DOT, HAIR, UI_THEME

_console: Console | None = None


def get_console() -> Console:
    """返回全局唯一 Console。"""
    global _console
    if _console is None:
        _console = Console(theme=UI_THEME, highlight=False, soft_wrap=False, emoji=False)
    return _console


def clear_screen() -> None:
    get_console().clear()


def hairline() -> Rule:
    """整行发丝细线。"""
    return Rule(characters=HAIR, style="hair")


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
    """给整屏内容统一加留白外边距，营造'呼吸感'。"""
    return Padding(Group(*renderables), (pad[0], pad[1], pad[0], pad[1]))
