"""抓取实时视图：滚动结果卡片 + 底部常驻进度条。

设计要点：
- reduce(model, event) 是纯函数（事件→新模型），可单测。
- CrawlView 是上下文管理器，包住 run_crawl_session；用 rich.Live 钉一条底部进度条，
  每条完成的站点用 live.console.print 滚进历史（可回看）。
- 模块级 _active 充当"当前活跃视图"接收器；reporter 调 emit_*；无活跃视图 / 非 TTY 时
  自动降级成与旧版一致的纯文本 print，保证管道、CI、重定向场景行为不变。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from oldironcrawler.ui.console import content_width, get_console, hairline
from oldironcrawler.ui.theme import BAR_EMPTY, BAR_FULL, CHECK, CROSS, DOT


# —— 事件 ——
@dataclass(frozen=True)
class SiteResultEvent:
    completed_index: int
    total: int
    website: str
    company_name: str = ""
    representative: str = ""
    emails: str = ""
    searched_representative: str = ""
    phones: str = ""
    reason: str = ""
    stage_timing: str = ""
    stage_counts: str = ""
    show_emails: bool = True
    show_phones: bool = True
    show_representative: bool = True
    show_searched_representative: bool = True


@dataclass(frozen=True)
class ProgressEvent:
    total: int
    done: int
    running: int
    dropped: int
    pending: int


# —— 模型 + 纯 reduce ——
@dataclass(frozen=True)
class CrawlModel:
    total: int = 0
    completed: int = 0
    done: int = 0
    running: int = 0
    dropped: int = 0
    pending: int = 0


def reduce(model: CrawlModel, event: object) -> CrawlModel:
    if isinstance(event, SiteResultEvent):
        return replace(
            model,
            total=event.total or model.total,
            completed=max(model.completed, event.completed_index),
        )
    if isinstance(event, ProgressEvent):
        return replace(
            model,
            total=event.total or model.total,
            done=event.done,
            running=event.running,
            dropped=event.dropped,
            pending=event.pending,
            completed=max(model.completed, event.done + event.dropped),
        )
    return model


# —— 渲染 ——
def _display(value: str) -> str:
    text = str(value or "").strip()
    return text if text else "未找到"


def build_card(event: SiteResultEvent) -> RenderableType:
    found = bool(str(event.company_name or "").strip())
    glyph = Text(f"{CHECK} " if found else f"{CROSS} ", style="ready" if found else "fail")
    title = Text(str(event.company_name or "").strip() or event.website, style="value.strong")
    index = Text(f"{event.completed_index}/{event.total}", style="hint")
    # 序号靠右对齐到内容宽度（手算间距），避免 grid expand 在宽终端里把序号推到极右、留大空隙。
    gap = max(content_width() - glyph.cell_len - title.cell_len - index.cell_len, 1)
    header = Text.assemble(glyph, title, " " * gap, index)

    rows: list[RenderableType] = [header, Text(f"   {event.website}", style="label")]
    detail = Table.grid(padding=(0, 2))
    detail.add_column(style="label", justify="left", no_wrap=True)
    detail.add_column(style="value", overflow="fold")
    if event.show_representative:
        detail.add_row("   代表人", _display(event.representative))
    if event.show_emails:
        detail.add_row("   邮箱", _display(event.emails))
    if event.show_searched_representative:
        detail.add_row("   搜索代表人", _display(event.searched_representative))
    if event.show_phones:
        detail.add_row("   电话", _display(event.phones))
    rows.append(detail)

    meta_bits = [bit for bit in (event.stage_timing, event.stage_counts) if str(bit or "").strip()]
    if meta_bits:
        rows.append(Text("   " + "   ".join(meta_bits), style="hint"))
    if str(event.reason or "").strip():
        rows.append(Text(f"   原因 {event.reason.strip()}", style="hint"))
    rows.append(Text(""))
    return Group(*rows)


def build_footer(model: CrawlModel, width: int) -> RenderableType:
    counters = Text()
    counters.append(f"{CHECK} ", style="ready")
    for index, (label, value) in enumerate(
        [("完成", model.done), ("进行", model.running), ("跳过", model.dropped), ("待处理", model.pending)]
    ):
        if index:
            counters.append(f"  {DOT} ", style="hair")
        counters.append(f"{label} ", style="label")
        counters.append(str(value), style="value.strong")

    total = max(model.total, 1)
    ratio = min(max(model.completed / total, 0.0), 1.0)
    bar_width = max(10, min(width - 12, 48))
    filled = int(round(ratio * bar_width))
    bar = Text()
    bar.append(BAR_FULL * filled, style="accent")
    bar.append(BAR_EMPTY * (bar_width - filled), style="hair")
    bar.append(f"  {ratio * 100:.0f}%", style="value")
    return Group(hairline(), counters, bar)


# —— 活跃视图接收器（reporter 经由模块级 emit_* 投递）——
_active: "CrawlView | None" = None


class CrawlView:
    def __init__(self, console=None) -> None:
        self._console = console or get_console()
        self._model = CrawlModel()
        self._live = None

    def __enter__(self) -> "CrawlView":
        global _active
        from rich.live import Live

        self._live = Live(
            build_footer(self._model, self._console.width),
            console=self._console,
            auto_refresh=False,
            transient=False,
        )
        self._live.__enter__()
        _active = self
        return self

    def __exit__(self, *exc) -> bool:
        global _active
        _active = None
        if self._live is not None:
            self._live.__exit__(*exc)
            self._live = None
        self._print_summary()
        return False

    def _refresh(self) -> None:
        if self._live is not None:
            self._live.update(build_footer(self._model, self._console.width))
            self._live.refresh()

    def _print_summary(self) -> None:
        text = Text()
        text.append("本次完成 ", style="label")
        text.append(f"{self._model.completed}/{max(self._model.total, self._model.completed)}", style="value.strong")
        self._console.print(text)

    def site_result(self, event: SiteResultEvent) -> None:
        self._model = reduce(self._model, event)
        if self._live is not None:
            self._live.console.print(build_card(event))
        self._refresh()

    def progress(self, event: ProgressEvent) -> None:
        self._model = reduce(self._model, event)
        self._refresh()

    def log(self, message: str) -> None:
        if self._live is not None:
            self._live.console.print(Text(str(message), style="hint"))


def emit_site_result(**kwargs) -> None:
    event = SiteResultEvent(**kwargs)
    if _active is not None:
        _active.site_result(event)
    else:
        _plain_site_result(event)


def emit_progress(**kwargs) -> None:
    event = ProgressEvent(**kwargs)
    if _active is not None:
        _active.progress(event)
    else:
        _plain_progress(event)


def emit_log(message: str) -> None:
    if _active is not None:
        _active.log(message)
    else:
        print(message, flush=True)


# —— 非 TTY / 无活跃视图时的纯文本降级（与旧版输出保持一致）——
def _plain_site_result(event: SiteResultEvent) -> None:
    print(f"[{event.completed_index}/{event.total}] {event.website}", flush=True)
    print(f"  公司名: {_display(event.company_name)}", flush=True)
    if event.show_representative:
        print(f"  代表人: {_display(event.representative)}", flush=True)
    if event.show_emails:
        print(f"  邮箱: {_display(event.emails)}", flush=True)
    if event.show_searched_representative:
        print(f"  搜索现役最大代表人: {_display(event.searched_representative)}", flush=True)
    if event.show_phones:
        print(f"  电话: {_display(event.phones)}", flush=True)
    if str(event.stage_timing or "").strip():
        print(f"  阶段耗时: {event.stage_timing}", flush=True)
    if str(event.stage_counts or "").strip():
        print(f"  页面统计: {event.stage_counts}", flush=True)
    if str(event.reason or "").strip():
        print(f"  原因: {event.reason.strip()}", flush=True)
    print("  ------------------------------------------------------------", flush=True)


def _plain_progress(event: ProgressEvent) -> None:
    print(
        f"[进度] total={event.total} done={event.done} running={event.running} "
        f"dropped={event.dropped} pending={event.pending}",
        flush=True,
    )
