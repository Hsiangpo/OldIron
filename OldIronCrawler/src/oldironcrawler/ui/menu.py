"""方向键菜单组件。

MenuController 是纯状态机（吃语义键、吐动作），不碰终端，便于单测。
run_menu 负责用 rich.Live 全屏渲染 + 读键循环。所有静态界面都用一个 MenuSpec 描述。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from rich.console import Group, RenderableType
from rich.text import Text

from oldironcrawler.ui import key_input as keys
from oldironcrawler.ui.console import get_console, hairline, screen, wordmark
from oldironcrawler.ui.theme import CURSOR


@dataclass(frozen=True)
class MenuItem:
    value: str          # 选中后返回的标识
    label: str          # 显示文本
    shortcut: str = ""  # 数字直达键，如 "1"；留空则用序号


@dataclass
class MenuSpec:
    title: str
    subtitle: str = ""
    status_block: RenderableType | None = None  # 标题与菜单之间的状态区，由调用方自由组合
    items: list[MenuItem] = field(default_factory=list)
    hint: str = "↑↓ 移动   Enter 确认   数字直达   Esc 返回"
    back_value: str | None = None  # Esc 返回时回传的值


class MenuController:
    """纯逻辑：吃语义键，吐 (action, payload)。action ∈ move/choose/back/noop。"""

    def __init__(self, items: list[MenuItem]) -> None:
        self._items = items
        self.index = 0

    def move(self, delta: int) -> None:
        count = len(self._items)
        if count:
            self.index = (self.index + delta) % count

    def current_value(self) -> str | None:
        if not self._items:
            return None
        return self._items[self.index].value

    def _match_shortcut(self, char: str) -> str | None:
        for offset, item in enumerate(self._items):
            shortcut = item.shortcut or str(offset + 1)
            if shortcut == char:
                self.index = offset
                return item.value
        return None

    def on_key(self, key: str) -> tuple[str, str | None]:
        if key == keys.CTRL_C:
            raise KeyboardInterrupt
        if key == keys.UP:
            self.move(-1)
            return ("move", None)
        if key == keys.DOWN:
            self.move(1)
            return ("move", None)
        if key == keys.ENTER:
            return ("choose", self.current_value())
        if key == keys.ESC:
            return ("back", None)
        if len(key) == 1 and key.isdigit():
            matched = self._match_shortcut(key)
            if matched is not None:
                return ("choose", matched)
        return ("noop", None)


def _items_renderable(items: list[MenuItem], index: int) -> Group:
    rows: list[RenderableType] = []
    for offset, item in enumerate(items):
        selected = offset == index
        number = item.shortcut or str(offset + 1)
        line = Text()
        line.append(f"{CURSOR}  " if selected else "   ", style="cursor")
        if selected:
            line.append(f"{number}  ", style="accent")
            line.append(item.label, style="value.strong")
        else:
            line.append(f"{number}  ", style="label")
            line.append(item.label, style="value")
        rows.append(line)
    return Group(*rows)


def render_menu(spec: MenuSpec, index: int) -> RenderableType:
    blocks: list[RenderableType] = [wordmark(spec.title, spec.subtitle)]
    if spec.status_block is not None:
        blocks += [Text(), hairline(), Text(), spec.status_block]
    blocks += [
        Text(),
        hairline(),
        Text(),
        _items_renderable(spec.items, index),
        Text(),
        hairline(),
        Text(spec.hint, style="hint"),
    ]
    return screen(*blocks)


def run_menu(spec: MenuSpec, *, reader=None) -> str | None:
    """清屏后重画菜单并读键，返回选中项的 value（Esc 返回 back_value）。

    用"每次按键清屏重画"而非 rich.Live：彻底免疫宽窗口换行把 Live 帧高算错导致的堆叠。
    """
    console = get_console()
    controller = MenuController(spec.items)
    while True:
        console.clear()
        console.print(render_menu(spec, controller.index))
        action, value = controller.on_key(keys.read_key(reader))
        if action == "choose":
            return value
        if action == "back":
            return spec.back_value
