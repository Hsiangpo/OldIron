from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.email_rules import (
    merge_ai_emails_for_website,
    select_emails_present_in_pages,
)
from oldironcrawler.extractor.llm_client import LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.extractor.service import _extract_ai_emails_or_empty


_CONTACT_HTML = """
<html><body>
<p>Contact us: <a href="mailto:info@acme.example">info@acme.example</a></p>
<p>Sales: sales [at] acme [dot] example</p>
<footer>Kenan: kenan@acme.example</footer>
</body></html>
"""

_PAGES = [("https://acme.example/contact", _CONTACT_HTML)]


def test_select_emails_present_keeps_evidenced_drops_hallucinated() -> None:
    kept = select_emails_present_in_pages(
        ["info@acme.example", "sales@acme.example", "ceo@hallucinated.com"],
        _PAGES,
    )
    assert "info@acme.example" in kept          # 正文里明写
    assert "sales@acme.example" in kept         # [at]/[dot] 混淆，去混淆后命中
    assert "ceo@hallucinated.com" not in kept   # 页面里没有，挡掉编造


def test_select_emails_present_empty_pages_returns_empty() -> None:
    assert select_emails_present_in_pages(["info@acme.example"], []) == []


def test_merge_ai_emails_unions_and_dedupes() -> None:
    merged = merge_ai_emails_for_website(
        "https://acme.example",
        ["info@acme.example"],
        ["info@acme.example", "sales@acme.example", "fake@nowhere.com"],
        _PAGES,
    )
    assert "info@acme.example" in merged          # 规则原有
    assert "sales@acme.example" in merged         # AI 补的、有证据
    assert "fake@nowhere.com" not in merged       # AI 编的、无证据
    assert merged.count("info@acme.example") == 1  # 去重


def test_merge_ai_emails_empty_ai_keeps_rule_only() -> None:
    merged = merge_ai_emails_for_website(
        "https://acme.example", ["info@acme.example"], [], _PAGES
    )
    assert merged == ["info@acme.example"]


def _email_client() -> WebsiteLlmClient:
    return WebsiteLlmClient(
        api_key="k",
        base_url="http://llm.example/v1",
        model="m",
        api_style="chat",
        reasoning_effort="low",
        proxy_url="",
        timeout_seconds=5.0,
        concurrency_limit=1,
    )


def test_extract_emails_from_pages_parses_and_dedupes(monkeypatch) -> None:
    client = _email_client()
    monkeypatch.setattr(
        client,
        "_call_json",
        lambda prompt, **kwargs: {
            "emails": ["Info@Acme.Example", "info@acme.example", "sales@acme.example"]
        },
    )
    try:
        emails = client.extract_emails_from_pages(
            homepage="https://acme.example",
            pages=[{"url": "https://acme.example/contact", "html": _CONTACT_HTML}],
        )
    finally:
        client.close()
    assert emails == ["info@acme.example", "sales@acme.example"]  # 小写 + 去重


def test_extract_emails_from_pages_handles_bad_payload(monkeypatch) -> None:
    client = _email_client()
    monkeypatch.setattr(client, "_call_json", lambda prompt, **kwargs: {"oops": 1})
    try:
        assert client.extract_emails_from_pages(homepage="x", pages=[]) == []
    finally:
        client.close()


def test_pick_email_urls_keeps_only_given_candidates(monkeypatch) -> None:
    client = _email_client()
    candidates = [
        "https://acme.example/central-relacionamento",
        "https://acme.example/products",
    ]
    monkeypatch.setattr(
        client,
        "_call_json",
        lambda prompt, **kwargs: {
            "selected_urls": [
                "https://acme.example/central-relacionamento",
                "https://evil.example/contact",
                "https://acme.example/made-up",
                "https://acme.example/central-relacionamento",
            ]
        },
    )
    try:
        urls = client.pick_email_urls(
            homepage="https://acme.example",
            candidate_urls=candidates,
            existing_email_urls=[],
            target_count=3,
        )
    finally:
        client.close()

    assert urls == ["https://acme.example/central-relacionamento"]


class _FakeEmailLlm:
    def __init__(self, *, result=None, exc=None):
        self.result = result or []
        self.exc = exc
        self.calls = []

    def extract_emails_from_pages(self, *, homepage, pages, deadline_monotonic=None):
        self.calls.append((homepage, pages))
        if self.exc is not None:
            raise self.exc
        return self.result


def test_extract_ai_emails_or_empty_builds_pages_and_returns() -> None:
    llm = _FakeEmailLlm(result=["info@acme.example"])
    out = _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<html>info@acme.example</html>")],
        deadline_monotonic=None,
    )
    assert out == ["info@acme.example"]
    # (url, html) 元组要转成 LLM 期望的 {"url","html"} dict
    assert llm.calls[0][1] == [
        {"url": "https://acme.example/contact", "html": "<html>info@acme.example</html>"}
    ]


def test_extract_ai_emails_or_empty_skips_llm_when_no_pages() -> None:
    llm = _FakeEmailLlm(result=["x@y.com"])
    assert _extract_ai_emails_or_empty(
        llm_client=llm, homepage="x", email_rule_pages=[], deadline_monotonic=None
    ) == []
    assert llm.calls == []  # 没页面就不调 LLM，省一次调用


def test_extract_ai_emails_or_empty_swallows_generic_error() -> None:
    llm = _FakeEmailLlm(exc=ValueError("boom"))
    assert _extract_ai_emails_or_empty(
        llm_client=llm, homepage="x", email_rule_pages=[("u", "h")], deadline_monotonic=None
    ) == []


def test_extract_ai_emails_or_empty_propagates_llm_temporary_error() -> None:
    llm = _FakeEmailLlm(exc=LlmTemporaryError("429"))
    with pytest.raises(LlmTemporaryError):
        _extract_ai_emails_or_empty(
            llm_client=llm, homepage="x", email_rule_pages=[("u", "h")], deadline_monotonic=None
        )
