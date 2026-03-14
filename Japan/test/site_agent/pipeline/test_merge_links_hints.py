from __future__ import annotations

from site_agent.models import PageContent
from site_agent.pipeline import _merge_links


def test_merge_links_includes_hints() -> None:
    visited = {
        "https://example.com": PageContent(
            url="https://example.com",
            markdown="home",
            links=[],
            success=True,
        )
    }
    memory = {"hints": ["https://example.com/company"]}
    links = _merge_links(visited, memory, allow_pdf=False)
    assert any(link.url == "https://example.com/company" for link in links)
