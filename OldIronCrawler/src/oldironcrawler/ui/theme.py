"""发丝线主题：唯一配色 / 字形真相源。

想整体换风格，只改 ACCENT 一处即可；别处不要再写死颜色或符号。
配色走克制路线：大面积中性灰 + 单一暖琥珀强调色；绿/红只用于极小的状态字形。
"""

from __future__ import annotations

from rich.theme import Theme

# —— 强调色：暖琥珀（金属暖调，呼应 OldIron 品牌）——
ACCENT = "#d6943b"

# —— 字形（全局统一）——
CURSOR = "❯"   # 当前选中行光标
BULLET = "●"   # 就绪 / 状态圆点
CHECK = "✓"    # 成功
CROSS = "✗"    # 失败
DOT = "·"      # 分隔点
ARROW = "›"    # 次级提示箭头
HAIR = "─"     # 发丝分隔线
BAR_FULL = "█"  # 进度条实心
BAR_EMPTY = "░"  # 进度条空心

# —— 样式表 ——
UI_THEME = Theme(
    {
        "accent": ACCENT,
        "wordmark": f"bold {ACCENT}",
        "cursor": f"bold {ACCENT}",
        "hair": "grey37",          # 发丝线
        "label": "grey58",         # 次要标签
        "value": "grey85",         # 主要值
        "value.strong": "bold white",
        "hint": "grey42",          # 底部提示
        "ready": "#6db073",        # 克制绿（仅就绪 / 成功）
        "fail": "#c96a6a",         # 克制红（仅失败）
        "dim": "grey42",
    }
)
