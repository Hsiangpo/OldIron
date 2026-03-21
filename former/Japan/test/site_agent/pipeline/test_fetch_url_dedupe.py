from __future__ import annotations

from site_agent.models import PageContent
from site_agent.pipeline.crawl import _filter_fetch_urls


def test_filter_fetch_urls_skips_normalized_visited_urls() -> None:
    visited = {
        "https://example.com/about": PageContent(
            url="https://example.com/about", markdown="", raw_html="", success=True
        )
    }
    memory = {"failed": []}
    urls = [
        "https://example.com/about/",
        "https://example.com/about#team",
        "https://example.com/contact",
    ]
    result = _filter_fetch_urls(urls, visited, memory, max_pages=10)
    assert result == ["https://example.com/contact"]


def test_filter_fetch_urls_skips_failed_url_variants() -> None:
    visited: dict[str, PageContent] = {}
    memory = {"failed": ["http://example.com/contact"]}
    urls = [
        "https://example.com/contact",
        "https://example.com/recruit",
    ]
    result = _filter_fetch_urls(urls, visited, memory, max_pages=10)
    assert result == ["https://example.com/recruit"]
