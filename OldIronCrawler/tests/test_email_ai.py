from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor import email_rules as email_rules_module
from oldironcrawler.extractor.email_rules import (
    collect_emails_for_pages,
    merge_ai_emails_for_website,
    select_emails_present_in_pages,
)
from oldironcrawler.extractor import service as service_module
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


def test_llm_client_disables_sdk_internal_retries() -> None:
    client = _email_client()
    try:
        assert getattr(client._client, "max_retries", None) == 0
    finally:
        client.close()


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


def test_extract_emails_from_pages_sends_compact_contact_context(monkeypatch) -> None:
    client = _email_client()
    seen_prompts: list[str] = []
    noisy_html = "\n".join(
        [
            "<html><body>",
            *[f"<p>product catalog filler {index}</p>" for index in range(2500)],
            "<section>Contact: sales [at] acme [dot] example</section>",
            *[f"<p>news archive filler {index}</p>" for index in range(2500)],
            "</body></html>",
        ]
    )

    def fake_call_json(prompt, **kwargs):
        seen_prompts.append(prompt)
        return {"emails": ["sales@acme.example"]}

    monkeypatch.setattr(client, "_call_json", fake_call_json)
    try:
        emails = client.extract_emails_from_pages(
            homepage="https://acme.example",
            pages=[{"url": "https://acme.example/contact", "html": noisy_html}],
        )
    finally:
        client.close()

    assert emails == ["sales@acme.example"]
    assert seen_prompts
    assert "sales [at] acme [dot] example" in seen_prompts[0]
    assert len(seen_prompts[0]) < 30_000


def test_extract_emails_from_pages_handles_bad_payload(monkeypatch) -> None:
    client = _email_client()
    monkeypatch.setattr(client, "_call_json", lambda prompt, **kwargs: {"oops": 1})
    try:
        assert client.extract_emails_from_pages(homepage="x", pages=[]) == []
    finally:
        client.close()


def test_extract_emails_from_pages_uses_single_fast_attempt(monkeypatch) -> None:
    client = _email_client()
    captured_kwargs: list[dict] = []

    def fake_call_json(_prompt, **kwargs):
        captured_kwargs.append(kwargs)
        return {"emails": []}

    monkeypatch.setattr(client, "_call_json", fake_call_json)
    try:
        emails = client.extract_emails_from_pages(
            homepage="https://acme.example",
            pages=[{"url": "https://acme.example/contact", "html": "<html>contact</html>"}],
        )
    finally:
        client.close()

    assert emails == []
    assert captured_kwargs[0]["max_retries"] == 1


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


def test_pick_email_urls_uses_single_fast_attempt(monkeypatch) -> None:
    client = _email_client()
    candidates = ["https://acme.example/contact"]
    captured_kwargs: list[dict] = []

    def fake_call_json(_prompt, **kwargs):
        captured_kwargs.append(kwargs)
        return {"selected_urls": candidates}

    monkeypatch.setattr(client, "_call_json", fake_call_json)
    try:
        urls = client.pick_email_urls(
            homepage="https://acme.example",
            candidate_urls=candidates,
            existing_email_urls=[],
            target_count=1,
        )
    finally:
        client.close()

    assert urls == candidates
    assert captured_kwargs[0]["max_retries"] == 1


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


def test_extract_ai_emails_or_empty_uses_runtime_concurrency_limit(monkeypatch) -> None:
    llm = _FakeEmailLlm(result=["info@acme.example"])
    seen_limits: list[int] = []

    class FakeSemaphore:
        def acquire(self, timeout=None):
            return True

        def release(self):
            return None

    def fake_get_ai_email_semaphore(limit: int):
        seen_limits.append(limit)
        return FakeSemaphore()

    monkeypatch.setattr(service_module, "_get_ai_email_semaphore", fake_get_ai_email_semaphore)

    out = _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<html>info@acme.example</html>")],
        deadline_monotonic=None,
        ai_email_concurrency=32,
    )

    assert out == ["info@acme.example"]
    assert seen_limits == [32]


def test_extract_ai_emails_or_empty_skips_llm_when_no_pages() -> None:
    llm = _FakeEmailLlm(result=["x@y.com"])
    assert _extract_ai_emails_or_empty(
        llm_client=llm, homepage="x", email_rule_pages=[], deadline_monotonic=None
    ) == []
    assert llm.calls == []  # 没页面就不调 LLM，省一次调用


def test_extract_ai_emails_or_empty_skips_llm_without_email_evidence() -> None:
    llm = _FakeEmailLlm(result=["info@acme.example"])

    out = _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<form><label>E-mail</label><input></form>")],
        deadline_monotonic=None,
    )

    assert out == []
    assert llm.calls == []


def test_extract_ai_emails_or_empty_runs_with_obfuscated_email_evidence() -> None:
    llm = _FakeEmailLlm(result=["sales@acme.example"])

    out = _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "Sales: sales (at) acme (dot) example")],
        deadline_monotonic=None,
    )

    assert out == ["sales@acme.example"]
    assert llm.calls


def test_extract_ai_emails_or_empty_swallows_generic_error() -> None:
    llm = _FakeEmailLlm(exc=ValueError("boom"))
    assert _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="x",
        email_rule_pages=[("u", "Contact: info@acme.example")],
        deadline_monotonic=None,
    ) == []


def test_extract_ai_emails_or_empty_swallows_llm_temporary_error() -> None:
    llm = _FakeEmailLlm(exc=LlmTemporaryError("429"))
    assert _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="x",
        email_rule_pages=[("u", "Contact: info@acme.example")],
        deadline_monotonic=None,
    ) == []


def test_collect_emails_skips_embedded_scan_after_direct_same_domain_hit(monkeypatch) -> None:
    def fail_embedded_scan(*_args, **_kwargs):
        raise AssertionError("direct same-domain hit should avoid the second full-page scan")

    monkeypatch.setattr(
        email_rules_module,
        "extract_same_domain_emails_from_embedded_content",
        fail_embedded_scan,
    )

    emails, page_hits = collect_emails_for_pages(
        "https://acme.example",
        [("https://acme.example/contact", "<html>info@acme.example</html>")],
    )

    assert emails == ["info@acme.example"]
    assert page_hits == {"https://acme.example/contact": ["info@acme.example"]}
