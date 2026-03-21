from __future__ import annotations

from site_agent.models import LinkItem, PageContent
from site_agent.pipeline import _ensure_key_pages_in_selection


def test_ensure_key_pages_prioritizes_rep_page() -> None:
    selected: list[str] = []
    remaining = [
        LinkItem(url="https://example.com/about", text="会社概要"),
        LinkItem(url="https://example.com/message", text="代表メッセージ"),
    ]
    visited = {
        "https://example.com/": PageContent(
            url="https://example.com/", markdown="", raw_html="", success=True
        )
    }
    result = _ensure_key_pages_in_selection(
        selected=selected,
        remaining=remaining,
        missing_fields=["representative"],
        max_select=1,
        visited=visited,
        memory={},
    )
    assert result[0] == "https://example.com/message"
