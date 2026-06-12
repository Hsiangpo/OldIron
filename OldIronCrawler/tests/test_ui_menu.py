"""MenuController 纯状态机单测：方向键 / 数字直达 / Enter / Esc / Ctrl+C。"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rich.console import Console  # noqa: E402

from oldironcrawler.ui import key_input as keys  # noqa: E402
from oldironcrawler.ui.menu import MenuController, MenuItem, MenuSpec, render_menu  # noqa: E402
from oldironcrawler.ui.theme import UI_THEME  # noqa: E402


def _items():
    return [
        MenuItem("start", "开始抓取"),
        MenuItem("websites", "打开 websites"),
        MenuItem("output", "打开 output"),
        MenuItem("config", "系统配置"),
        MenuItem("quit", "退出"),
    ]


def test_arrow_down_wraps_around():
    controller = MenuController(_items())
    for _ in range(5):
        controller.on_key(keys.DOWN)
    assert controller.index == 0


def test_arrow_up_from_top_wraps_to_bottom():
    controller = MenuController(_items())
    controller.on_key(keys.UP)
    assert controller.index == 4


def test_enter_chooses_current_item():
    controller = MenuController(_items())
    controller.on_key(keys.DOWN)
    controller.on_key(keys.DOWN)
    action, value = controller.on_key(keys.ENTER)
    assert action == "choose"
    assert value == "output"


def test_digit_shortcut_jumps_and_chooses():
    controller = MenuController(_items())
    action, value = controller.on_key("4")
    assert action == "choose"
    assert value == "config"
    assert controller.index == 3


def test_unmapped_digit_is_noop():
    controller = MenuController(_items())
    action, value = controller.on_key("9")
    assert action == "noop"
    assert value is None


def test_esc_returns_back():
    controller = MenuController(_items())
    action, _ = controller.on_key(keys.ESC)
    assert action == "back"


def test_ctrl_c_raises_keyboard_interrupt():
    controller = MenuController(_items())
    with pytest.raises(KeyboardInterrupt):
        controller.on_key(keys.CTRL_C)


def test_explicit_shortcut_overrides_ordinal():
    controller = MenuController([MenuItem("a", "甲", "2"), MenuItem("b", "乙", "1")])
    action, value = controller.on_key("1")
    assert (action, value) == ("choose", "b")


def test_render_menu_smoke_renders_cursor_and_items():
    spec = MenuSpec(title="OLDIRONCRAWLER", subtitle="公司官网采集", items=_items())
    buf = io.StringIO()
    console = Console(theme=UI_THEME, file=buf, force_terminal=True, width=60, color_system="truecolor")
    console.print(render_menu(spec, index=0))
    output = buf.getvalue()
    assert "开始抓取" in output
    assert "退出" in output
    assert "\x1b[" in output  # 有 ANSI 颜色
