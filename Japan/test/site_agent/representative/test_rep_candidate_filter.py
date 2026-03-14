from __future__ import annotations

from site_agent.pipeline import _filter_rep_candidate_urls


def test_filter_rep_candidate_urls_removes_contact_pages() -> None:
    urls = [
        "https://example.com/contact",
        "https://example.com/contact/index.html",
        "https://example.com/company/profile",
    ]
    filtered = _filter_rep_candidate_urls(urls)
    assert "https://example.com/company/profile" in filtered
    assert all("contact" not in url for url in filtered)


def test_filter_rep_candidate_urls_skips_time_greeting() -> None:
    urls = [
        "https://example.com/2026%E5%B9%B4_%E6%96%B0%E5%B9%B4%E3%81%AE%E3%81%94%E6%8C%A8%E6%8B%B6/",
        "https://example.com/company/message/",
    ]
    filtered = _filter_rep_candidate_urls(urls)
    assert "https://example.com/company/message/" in filtered
    assert all("2026" not in url for url in filtered)
