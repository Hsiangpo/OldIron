"""crawl_view 纯逻辑单测：reduce 模型 + 非 TTY 纯文本降级 + 渲染冒烟。"""

from __future__ import annotations

import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rich.console import Console  # noqa: E402

from oldironcrawler.ui import crawl_view  # noqa: E402
from oldironcrawler.ui.crawl_view import (  # noqa: E402
    CrawlModel,
    ProgressEvent,
    SiteResultEvent,
    build_card,
    build_footer,
    reduce,
)
from oldironcrawler.ui.theme import UI_THEME  # noqa: E402


def test_site_result_advances_completed_and_total():
    model = reduce(CrawlModel(), SiteResultEvent(completed_index=5, total=320, website="a.com"))
    assert model.completed == 5
    assert model.total == 320


def test_progress_sets_counters_and_derives_completed():
    model = reduce(CrawlModel(), ProgressEvent(total=320, done=38, running=8, dropped=1, pending=273))
    assert (model.done, model.running, model.dropped, model.pending) == (38, 8, 1, 273)
    assert model.completed == 39  # done + dropped


def test_completed_never_regresses():
    model = reduce(CrawlModel(), SiteResultEvent(completed_index=40, total=320, website="a.com"))
    model = reduce(model, ProgressEvent(total=320, done=5, running=1, dropped=0, pending=314))
    assert model.completed == 40  # 站点事件已经到 40，心跳的小计数不应让它回退


def test_emit_site_result_without_active_view_plain_prints(capsys):
    crawl_view._active = None
    crawl_view.emit_site_result(
        completed_index=1, total=2, website="acme.com",
        company_name="Acme", representative="Jane", emails="j@acme.com",
    )
    out = capsys.readouterr().out
    assert "[1/2] acme.com" in out
    assert "公司名: Acme" in out


def test_emit_progress_without_active_view_plain_prints(capsys):
    crawl_view._active = None
    crawl_view.emit_progress(total=10, done=3, running=2, dropped=0, pending=5)
    out = capsys.readouterr().out
    assert "total=10 done=3 running=2 dropped=0 pending=5" in out


def test_build_footer_renders_bar_and_percent():
    buf = io.StringIO()
    console = Console(theme=UI_THEME, file=buf, force_terminal=True, width=60, color_system="truecolor")
    console.print(build_footer(CrawlModel(total=100, completed=13, done=12, running=8, dropped=1, pending=79), 60))
    out = buf.getvalue()
    assert "13%" in out
    assert "完成" in out and "待处理" in out


def test_build_card_hides_disabled_fields():
    buf = io.StringIO()
    console = Console(theme=UI_THEME, file=buf, force_terminal=True, width=60, color_system="truecolor")
    event = SiteResultEvent(
        completed_index=1, total=1, website="acme.com", company_name="Acme",
        representative="Jane", emails="j@acme.com", phones="123",
        show_phones=False, show_searched_representative=False,
    )
    console.print(build_card(event))
    out = buf.getvalue()
    assert "Acme" in out
    assert "邮箱" in out
    assert "电话" not in out  # 关掉电话则该行不出现
