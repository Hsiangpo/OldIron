from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_DIR = ROOT.parent / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))


from oldiron_core.fc_email.client import HtmlPageResult  # noqa: E402
from oldiron_core.fc_email.email_service import FirecrawlEmailService  # noqa: E402
from oldiron_core.fc_email.email_service import FirecrawlEmailSettings  # noqa: E402
from oldiron_core.fc_email.llm_client import EmailUrlLlmClient  # noqa: E402


class _FakeCrawler:
    def map_site(self, start_url: str, limit: int = 200):
        _ = limit
        return [
            f"{start_url.rstrip('/')}/contact",
            f"{start_url.rstrip('/')}/about",
        ]

    def scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        return [
            HtmlPageResult(
                url=urls[0],
                html="""
                <html><body>
                  <a href="mailto:hello@sampleco.co.jp">hello@sampleco.co.jp</a>
                  <p>Support: support@sampleco.co.jp</p>
                </body></html>
                """,
            )
        ]


class _EmptyCrawler:
    def map_site(self, start_url: str, limit: int = 200):
        _ = start_url, limit
        return []

    def scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        _ = urls
        return []


class _NoEmailCrawler:
    def map_site(self, start_url: str, limit: int = 200):
        _ = limit
        return [f"{start_url.rstrip('/')}/about"]

    def scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        return [
            HtmlPageResult(
                url=urls[0],
                html="""
                <html><body>
                  <h1>About Example Co</h1>
                  <p>Chief Executive Officer Jane Doe</p>
                </body></html>
                """,
            )
        ]


class _FailIfCalledLlm:
    def pick_candidate_urls(self, **kwargs):
        raise AssertionError(f"pick_candidate_urls 不应被调用: {kwargs}")

    def extract_contacts_from_html(self, **kwargs):
        raise AssertionError(f"extract_contacts_from_html 不应被调用: {kwargs}")


class _SuccessLlm:
    def pick_candidate_urls(self, **kwargs):
        return [kwargs["candidate_urls"][0]]

    def extract_contacts_from_html(self, **kwargs):
        from oldiron_core.fc_email.llm_client import HtmlContactExtraction

        return HtmlContactExtraction(
            company_name="Example Co",
            representative="Jane Doe",
            emails=["team@sampleco.co.jp"],
            evidence_url=kwargs["pages"][0]["url"],
            evidence_quote="Jane Doe",
        )


class _RepresentativeOnlyLlm:
    def __init__(self) -> None:
        self.last_need_emails = None

    def pick_candidate_urls(self, **kwargs):
        return [kwargs["candidate_urls"][0]]

    def extract_contacts_from_html(self, **kwargs):
        from oldiron_core.fc_email.llm_client import HtmlContactExtraction

        self.last_need_emails = kwargs.get("need_emails")
        return HtmlContactExtraction(
            company_name="Example Co",
            representative="Jane Doe",
            emails=["should-not-be-used@sampleco.co.jp"],
            evidence_url=kwargs["pages"][0]["url"],
            evidence_quote="Jane Doe",
        )


class _EmailFallbackLlm:
    def __init__(self) -> None:
        self.last_need_emails = None

    def pick_candidate_urls(self, **kwargs):
        return [kwargs["candidate_urls"][0]]

    def extract_contacts_from_html(self, **kwargs):
        from oldiron_core.fc_email.llm_client import HtmlContactExtraction

        self.last_need_emails = kwargs.get("need_emails")
        return HtmlContactExtraction(
            company_name="Example Co",
            representative="",
            emails=["team@sampleco.co.jp"],
            evidence_url=kwargs["pages"][0]["url"],
            evidence_quote="Contact Example Co",
        )


class FcEmailTests(unittest.TestCase):
    def test_auto_falls_back_from_responses_to_chat(self) -> None:
        client = EmailUrlLlmClient(
            api_key="x",
            base_url="https://example.com/v1",
            model="claude-sonnet-4-6",
            fallback_model="claude-sonnet-4-6",
            reasoning_effort="",
            api_style="auto",
            timeout_seconds=20,
        )
        client._call_responses_json_with_model = lambda model, prompt: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("messages must not be empty (2013)")
        )
        client._call_chat_json_with_model = lambda model, prompt: '{"ok": true, "channel": "chat"}'  # type: ignore[method-assign]
        data = client._call_json_with_model("claude-sonnet-4-6", "test prompt")
        self.assertEqual({"ok": True, "channel": "chat"}, data)

    def test_rule_email_extraction_skips_llm_when_representative_exists(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_FakeCrawler())
        service._llm = _FailIfCalledLlm()  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                existing_representative="Jane Doe",
            )
        finally:
            service.close()
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual(["hello@sampleco.co.jp", "support@sampleco.co.jp"], result.emails)

    def test_dash_representative_is_treated_as_missing_and_uses_llm(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_FakeCrawler())
        service._llm = _SuccessLlm()  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                existing_representative="-",
            )
        finally:
            service.close()
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual(["hello@sampleco.co.jp", "support@sampleco.co.jp"], result.emails)

    def test_unsupported_asset_url_is_dropped_before_crawl(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_FakeCrawler())
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://tpc.googlesyndication.com/simgad/123/logo.jpg",
            )
        finally:
            service.close()
        self.assertEqual([], result.emails)
        self.assertEqual("", result.representative)

    def test_empty_pages_do_not_trigger_llm(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_EmptyCrawler())
        service._llm = _FailIfCalledLlm()  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
            )
        finally:
            service.close()
        self.assertEqual([], result.emails)
        self.assertEqual("", result.representative)

    def test_rule_email_found_then_llm_only_extracts_representative(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_FakeCrawler())
        llm = _RepresentativeOnlyLlm()
        service._llm = llm  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                existing_representative="",
            )
        finally:
            service.close()
        self.assertFalse(llm.last_need_emails)
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual(["hello@sampleco.co.jp", "support@sampleco.co.jp"], result.emails)

    def test_secondary_email_lookup_runs_before_llm_email_extraction(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_NoEmailCrawler())
        llm = _RepresentativeOnlyLlm()
        service._llm = llm  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                secondary_email_lookup=lambda **kwargs: ["fallback@example.com"],  # noqa: ARG005
            )
        finally:
            service.close()
        self.assertFalse(llm.last_need_emails)
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual(["fallback@example.com"], result.emails)

    def test_existing_representative_still_allows_llm_email_fallback(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_NoEmailCrawler())
        llm = _EmailFallbackLlm()
        service._llm = llm  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                existing_representative="Jane Doe",
            )
        finally:
            service.close()
        self.assertTrue(llm.last_need_emails)
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual(["team@sampleco.co.jp"], result.emails)

    def test_disable_llm_email_extraction_keeps_llm_from_returning_emails(self) -> None:
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )
        service = FirecrawlEmailService(settings, firecrawl_client=_NoEmailCrawler())
        llm = _RepresentativeOnlyLlm()
        service._llm = llm  # type: ignore[assignment]
        try:
            result = service.discover_emails(
                company_name="Example Co",
                homepage="https://example.com",
                allow_llm_email_extraction=False,
            )
        finally:
            service.close()
        self.assertFalse(llm.last_need_emails)
        self.assertEqual("Jane Doe", result.representative)
        self.assertEqual([], result.emails)

    def test_llm_5xx_retries_until_success(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)

        class _FakeResponses:
            def __init__(self) -> None:
                self.calls = 0

            def create(self, **kwargs):
                _ = kwargs
                self.calls += 1
                if self.calls < 3:
                    raise RuntimeError("503 Service Unavailable")
                return type("Resp", (), {"output_text": '{"ok": true}'})()

        fake_responses = _FakeResponses()
        client._client = type("FakeClient", (), {"responses": fake_responses})()
        client._extract_chat_output_text = lambda response: ""  # type: ignore[method-assign]

        with patch("time.sleep", return_value=None):
            result = client._call_api_with_retry(
                channel="responses",
                kwargs={"model": "x", "input": "y"},
            )
        self.assertEqual('{"ok": true}', result)
        self.assertEqual(3, fake_responses.calls)

    def test_prompt_is_truncated_to_250k_chars(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        seen: dict[str, str] = {}

        def _fake_responses(model: str, prompt: str) -> str:
            seen["model"] = model
            seen["prompt"] = prompt
            return '{"ok": true}'

        client._api_style = "responses"
        client._call_responses_json_with_model = _fake_responses  # type: ignore[method-assign]
        client._call_chat_json_with_model = lambda model, prompt: '{"ok": true}'  # type: ignore[method-assign]

        data = client._call_json_with_model("claude-sonnet-4-6", "x" * 260_000)
        self.assertEqual({"ok": True}, data)
        self.assertEqual(250_000, len(seen["prompt"]))
