from __future__ import annotations

from concurrent.futures import Future
import sys
import threading
import time
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


def test_extract_emails_from_pages_short_ai_deadline_still_calls_llm(monkeypatch) -> None:
    client = _email_client()
    calls: list[dict] = []

    def fake_chat_call(kwargs, **call_kwargs):
        calls.append({"kwargs": kwargs, "call_kwargs": call_kwargs})
        return '{"emails":["info@acme.example"]}'

    monkeypatch.setattr(client, "_call_chat_with_retry", fake_chat_call)
    try:
        emails = client.extract_emails_from_pages(
            homepage="https://acme.example",
            pages=[{"url": "https://acme.example/contact", "html": "<html>info at acme dot example</html>"}],
            deadline_monotonic=time.monotonic() + 2.0,
        )
    finally:
        client.close()

    assert emails == ["info@acme.example"]
    assert calls


def test_call_json_enforces_hard_deadline(monkeypatch) -> None:
    client = _email_client()
    release = threading.Event()

    def blocking_call(*_args, **_kwargs):
        release.wait(timeout=0.35)
        return '{"emails":[]}'

    monkeypatch.setattr(client, "_call_with_retry", blocking_call)
    begin = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            client._call_json("{}", deadline_monotonic=time.monotonic() + 0.05)
    finally:
        release.set()
        client.close()

    assert time.monotonic() - begin < 0.2


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


def test_extract_ai_emails_or_empty_runs_even_without_email_evidence() -> None:
    llm = _FakeEmailLlm(result=["info@acme.example"])

    out = _extract_ai_emails_or_empty(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<form><label>E-mail</label><input></form>")],
        deadline_monotonic=None,
    )

    assert out == ["info@acme.example"]
    assert llm.calls


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


def test_ai_email_future_starts_but_rule_hit_returns_without_waiting() -> None:
    started = threading.Event()
    release = threading.Event()

    class SlowLlm:
        def __init__(self) -> None:
            self.calls = 0

        def extract_emails_from_pages(self, **_kwargs):
            self.calls += 1
            started.set()
            release.wait(timeout=2.0)
            return ["ai@acme.example"]

    llm = SlowLlm()
    future = service_module._start_ai_email_future(
        llm_client=llm,
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<html>AI: ai at acme dot example</html>")],
        deadline_monotonic=time.monotonic() + 10,
        ai_email_concurrency=1,
        ai_email_timeout_seconds=5.0,
    )
    assert future is not None
    assert started.wait(timeout=1.0)

    begin = time.monotonic()
    try:
        emails = service_module._merge_ai_email_future(
            website="https://acme.example",
            rule_emails=["info@acme.example"],
            email_rule_pages=[("https://acme.example/contact", "<html>AI: ai at acme dot example</html>")],
            ai_email_future=future,
            metrics=service_module.SiteStageMetrics(),
            deadline_monotonic=time.monotonic() + 10,
        )
    finally:
        release.set()

    assert emails == ["info@acme.example"]
    assert time.monotonic() - begin < 0.2
    assert llm.calls == 1


def test_ai_email_future_waits_for_ai_when_rule_misses() -> None:
    started = threading.Event()
    release = threading.Event()

    class SlowLlm:
        def extract_emails_from_pages(self, **_kwargs):
            started.set()
            release.wait(timeout=1.0)
            return ["ai@acme.example"]

    future = service_module._start_ai_email_future(
        llm_client=SlowLlm(),
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/contact", "<html>AI: ai at acme dot example</html>")],
        deadline_monotonic=time.monotonic() + 10,
        ai_email_concurrency=1,
        ai_email_timeout_seconds=5.0,
    )
    assert future is not None
    assert started.wait(timeout=1.0)

    timer = threading.Timer(0.05, release.set)
    timer.start()

    begin = time.monotonic()
    try:
        emails = service_module._merge_ai_email_future(
            website="https://acme.example",
            rule_emails=[],
            email_rule_pages=[("https://acme.example/contact", "<html>AI: ai at acme dot example</html>")],
            ai_email_future=future,
            metrics=service_module.SiteStageMetrics(),
            deadline_monotonic=time.monotonic() + 10,
        )
    finally:
        release.set()
        timer.cancel()

    elapsed = time.monotonic() - begin
    assert emails == ["ai@acme.example"]
    assert elapsed >= 0.03
    assert elapsed < 0.5


def test_ai_email_future_uses_shorter_wait_for_pages_without_email_evidence(monkeypatch) -> None:
    release = threading.Event()

    class SlowLlm:
        def extract_emails_from_pages(self, **_kwargs):
            release.wait(timeout=0.35)
            return ["ai@acme.example"]

    monkeypatch.setattr(service_module, "_AI_EMAIL_LOW_SIGNAL_TIMEOUT_SECONDS", 0.05, raising=False)

    future = service_module._start_ai_email_future(
        llm_client=SlowLlm(),
        homepage="https://acme.example",
        email_rule_pages=[("https://acme.example/about", "<html>plain company page</html>")],
        deadline_monotonic=time.monotonic() + 10,
        ai_email_concurrency=1,
        ai_email_timeout_seconds=5.0,
    )
    assert future is not None

    begin = time.monotonic()
    try:
        emails = service_module._merge_ai_email_future(
            website="https://acme.example",
            rule_emails=[],
            email_rule_pages=[("https://acme.example/about", "<html>plain company page</html>")],
            ai_email_future=future,
            metrics=service_module.SiteStageMetrics(),
            deadline_monotonic=time.monotonic() + 10,
        )
    finally:
        release.set()

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed >= 0.03
    assert elapsed < 0.2


def test_ai_email_future_join_uses_stored_wait_cap_when_deadline_is_far() -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 10.0,
        max_wait_seconds=0.05,
    )

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 10.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed >= 0.03
    assert elapsed < 0.2


def test_ai_email_future_join_has_merge_hard_cap(monkeypatch) -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 10.0,
        max_wait_seconds=0.2,
    )
    monkeypatch.setattr(service_module, "_AI_EMAIL_MERGE_WAIT_HARD_CAP_SECONDS", 0.05, raising=False)

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 10.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed >= 0.03
    assert elapsed < 0.12


def test_ai_email_future_default_join_wait_is_tiny() -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 10.0,
        max_wait_seconds=5.0,
    )

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 10.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed < 0.15


def test_ai_email_future_join_clamps_helper_wait_to_hard_cap(monkeypatch) -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 60.0,
        max_wait_seconds=60.0,
    )
    monkeypatch.setattr(service_module, "_AI_EMAIL_MERGE_WAIT_HARD_CAP_SECONDS", 0.05, raising=False)
    monkeypatch.setattr(service_module, "_remaining_ai_email_wait_seconds", lambda *_args, **_kwargs: 0.2)

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 60.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed >= 0.03
    assert elapsed < 0.12


def test_ai_email_future_join_does_not_trust_wait_timeout(monkeypatch) -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 60.0,
        max_wait_seconds=0.05,
    )
    monkeypatch.setattr(service_module, "_AI_EMAIL_MERGE_WAIT_HARD_CAP_SECONDS", 0.05, raising=False)

    def blocking_wait(_futures, timeout=None):
        time.sleep(0.35)
        return set(), set()

    monkeypatch.setattr(service_module, "wait_for_futures", blocking_wait)

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 60.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert elapsed >= 0.03
    assert elapsed < 0.12


def test_ai_email_future_join_does_not_trust_blocking_result_timeout() -> None:
    class BlockingResultFuture:
        def __init__(self) -> None:
            self.cancelled_flag = False

        def done(self) -> bool:
            return False

        def cancel(self) -> bool:
            self.cancelled_flag = True
            return True

        def result(self, timeout=None):
            time.sleep(0.35)
            return ["ai@acme.example"]

    blocking_future = BlockingResultFuture()
    ai_email_future = service_module.AiEmailFuture(
        future=blocking_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 10.0,
        max_wait_seconds=0.05,
    )

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 10.0,
    )

    assert emails == []
    assert not blocking_future.cancelled_flag
    assert time.monotonic() - begin < 0.2


def test_ai_email_future_join_leaves_pending_ai_running() -> None:
    pending_future = Future()
    ai_email_future = service_module.AiEmailFuture(
        future=pending_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 10.0,
        max_wait_seconds=0.05,
    )

    begin = time.monotonic()
    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "<html>plain contact page</html>")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 10.0,
    )

    elapsed = time.monotonic() - begin
    assert emails == []
    assert not pending_future.cancelled()
    assert elapsed >= 0.03
    assert elapsed < 0.2


def test_ai_email_future_merge_uses_ready_ai_result() -> None:
    ready_future = Future()
    ready_future.set_result(["sales@acme.example"])
    ai_email_future = service_module.AiEmailFuture(
        future=ready_future,
        started_monotonic=time.monotonic(),
        deadline_monotonic=time.monotonic() + 60.0,
        max_wait_seconds=60.0,
    )

    emails = service_module._merge_ai_email_future(
        website="https://acme.example",
        rule_emails=[],
        email_rule_pages=[("https://acme.example/contact", "Sales: sales at acme dot example")],
        ai_email_future=ai_email_future,
        metrics=service_module.SiteStageMetrics(),
        deadline_monotonic=time.monotonic() + 60.0,
    )

    assert emails == ["sales@acme.example"]


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
