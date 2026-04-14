"""邮箱服务生命周期测试。"""

from __future__ import annotations

import sys
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.oldiron_core.fc_email.email_service import FirecrawlEmailService
from shared.oldiron_core.fc_email.email_service import FirecrawlEmailSettings
from shared.oldiron_core.fc_email.email_service import extract_domain
from shared.oldiron_core.fc_email.client import HtmlPageResult
from shared.oldiron_core.fc_email.llm_client import EmailUrlLlmClient
from shared.oldiron_core.fc_email.llm_client import HtmlContactExtraction
from shared.oldiron_core.fc_email.normalization import split_emails


class _DummyCrawler:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _DummyKeyPool:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _DummyLlm:
    def __init__(self, result: HtmlContactExtraction) -> None:
        self._result = result
        self.closed = 0

    def extract_contacts_from_html(self, **_: object) -> HtmlContactExtraction:
        return self._result

    def close(self) -> None:
        self.closed += 1


class _CaptureLlm:
    def __init__(self) -> None:
        self.last_pages: list[dict[str, str]] | None = None

    def extract_contacts_from_html(self, **kwargs: object) -> HtmlContactExtraction:
        self.last_pages = list(kwargs.get("pages") or [])
        return HtmlContactExtraction(
            company_name="Example",
            representative="山田 太郎",
            emails=["info@alpha.co.jp"],
            evidence_url="https://example.co.jp/contact",
            evidence_quote="山田 太郎",
        )


class _FailIfCalledLlm:
    def extract_contacts_from_html(self, **_: object) -> HtmlContactExtraction:
        raise AssertionError("不应该触发 LLM 代表人抽取")


class _CaptureFullHtmlCrawler:
    def __init__(self, html: str) -> None:
        self._html = html
        self.last_truncate_html: bool | None = None

    def scrape_html_pages(self, urls: list[str], *, truncate_html: bool = True) -> list[HtmlPageResult]:
        self.last_truncate_html = truncate_html
        return [HtmlPageResult(url=str(urls[0]), html=self._html)]


class _FakeStreamResponse:
    def __init__(self, lines: list[str | bytes]) -> None:
        self._lines = lines

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_lines(self, decode_unicode: bool = True):  # noqa: ARG002
        return iter(self._lines)


class _FakeHttpStreamClient:
    def __init__(self, lines: list[str | bytes]) -> None:
        self._lines = lines
        self.last_method: str | None = None
        self.last_url: str | None = None
        self.last_headers: dict[str, str] | None = None
        self.last_json: dict[str, object] | None = None
        self.closed = 0

    def stream(self, method: str, url: str, *, headers: dict[str, str], json: dict[str, object]) -> _FakeStreamResponse:
        self.last_method = method
        self.last_url = url
        self.last_headers = dict(headers)
        self.last_json = dict(json)
        return _FakeStreamResponse(self._lines)

    def close(self) -> None:
        self.closed += 1


class FirecrawlEmailServiceLifecycleTests(unittest.TestCase):
    def test_llm_client_falls_back_to_streaming_when_responses_output_text_is_empty(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        client._client = SimpleNamespace(
            responses=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(output_text="")),
        )
        client._api_key = "x"
        client._base_url = "https://cc.gpteam.top/v1"
        client._timeout_seconds = 30.0
        stream_lines = [
            'data: {"type":"response.output_text.delta","delta":"o"}',
            'data: {"type":"response.output_text.delta","delta":"k"}',
            'data: {"type":"response.output_text.done","text":"ok"}',
        ]
        client._http_client = _FakeHttpStreamClient(stream_lines)
        text = client._call_api_with_retry(
            channel="responses",
            kwargs={"model": "gpt-5.1-codex-mini", "input": "Say ok"},
        )
        self.assertEqual("ok", text)
        self.assertEqual("POST", client._http_client.last_method)
        self.assertEqual("https://cc.gpteam.top/v1/responses", client._http_client.last_url)
        self.assertEqual("Mozilla/5.0", client._http_client.last_headers["User-Agent"])

    def test_llm_client_prefers_streaming_responses_directly_for_cc_provider(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        client._client = SimpleNamespace(
            responses=SimpleNamespace(create=lambda **kwargs: (_ for _ in ()).throw(AssertionError("不该先走 SDK responses"))),
        )
        client._api_key = "x"
        client._base_url = "https://cc.gpteam.top/v1"
        client._timeout_seconds = 30.0
        client._reasoning_effort = "medium"
        with patch.object(client, "_call_responses_streaming_api", return_value='{"status":"ok"}') as stream_mock:
            text = client._call_responses_json_with_model("gpt-5.1-codex-mini", "Say ok")
        self.assertEqual('{"status":"ok"}', text)
        stream_mock.assert_called_once()

    def test_llm_client_streaming_response_decodes_utf8_sse_lines(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        client._api_key = "x"
        client._base_url = "https://cc.gpteam.top/v1"
        client._timeout_seconds = 30.0
        client._http_client = _FakeHttpStreamClient(
            [
                (
                    "data: "
                    + json.dumps(
                        {"type": "response.output_text.done", "text": "榛葉 稔"},
                        ensure_ascii=False,
                    )
                ).encode("utf-8")
            ]
        )
        text = client._call_responses_streaming_api({"model": "gpt-5.1-codex-mini", "input": "Say ok"})
        self.assertEqual("榛葉 稔", text)

    def test_llm_client_retries_transient_ssl_eof(self) -> None:
        calls = {"count": 0}

        def _create(**kwargs: object) -> object:  # noqa: ARG001
            calls["count"] += 1
            if calls["count"] < 3:
                raise RuntimeError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol (_ssl.c:1006)")
            return SimpleNamespace(output_text='{"status":"ok"}')

        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        client._client = SimpleNamespace(
            responses=SimpleNamespace(create=_create),
        )
        client._api_key = "x"
        client._base_url = "https://api.example.com/v1"
        client._timeout_seconds = 30.0
        client._reasoning_effort = "medium"

        with patch("time.sleep", return_value=None):
            text = client._call_api_with_retry(
                channel="responses",
                kwargs={"model": "gpt-5.1-codex-mini", "input": "Say ok"},
            )

        self.assertEqual('{"status":"ok"}', text)
        self.assertEqual(3, calls["count"])

    def test_llm_client_close_closes_http_client(self) -> None:
        client = EmailUrlLlmClient.__new__(EmailUrlLlmClient)
        client._http_client = _FakeHttpStreamClient([])

        client.close()

        self.assertEqual(1, client._http_client.closed)

    def test_close_does_not_close_injected_crawler_or_key_pool(self) -> None:
        crawler = _DummyCrawler()
        key_pool = _DummyKeyPool()
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=key_pool,
            firecrawl_client=crawler,
        )
        service.close()
        self.assertEqual(0, crawler.closed)
        self.assertEqual(0, key_pool.closed)

    def test_close_closes_owned_llm_client(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        llm = _DummyLlm(
            HtmlContactExtraction(
                company_name="Example",
                representative="山田 太郎",
                emails=[],
                evidence_url="https://example.co.jp/contact",
                evidence_quote="山田 太郎",
            )
        )
        service._llm = llm  # type: ignore[assignment]

        service.close()

        self.assertEqual(1, llm.closed)

    def test_extract_domain_uses_registrable_domain_for_subdomain(self) -> None:
        self.assertEqual("aig.com", extract_domain("http://www-154.aig.com/"))
        self.assertEqual("abc.co.jp", extract_domain("https://recruit.abc.co.jp/about"))

    def test_extract_rule_emails_keeps_personal_mail_when_real(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        pages = [
            HtmlPageResult(
                url="https://example.co.jp/contact",
                html="<html>info@alpha.co.jp ceo.personal@gmail.com</html>",
            )
        ]
        emails = service._extract_rule_emails("https://example.co.jp", pages)
        self.assertEqual(["info@alpha.co.jp", "ceo.personal@gmail.com"], emails)

    def test_extract_rule_emails_skips_directory_like_noise_page(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        html = """
        <html>
          a@alpha.co.jp b@beta.co.jp c@gamma.co.jp d@delta.co.jp
          e@epsilon.co.jp f@zeta.co.jp g@eta.co.jp h@theta.co.jp
        </html>
        """
        pages = [HtmlPageResult(url="https://example.co.jp/member-list", html=html)]
        emails = service._extract_rule_emails("https://example.co.jp", pages)
        self.assertEqual([], emails)

    def test_extract_rule_emails_ignores_script_like_pseudo_emails(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        html = """
        <html>
          <body>
            <a href="mailto:contact@mazda.co.jp">contact@mazda.co.jp</a>
            <script>
              const a = "n@mockconsole.prototype";
              const b = "n@t.prototype.render";
            </script>
          </body>
        </html>
        """
        pages = [HtmlPageResult(url="https://example.co.jp/contact", html=html)]
        emails = service._extract_rule_emails("https://example.co.jp", pages)
        self.assertEqual(["contact@mazda.co.jp"], emails)

    def test_extract_rule_emails_drops_placeholder_offsite_noise_but_keeps_possible_real_mail(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        html = """
        <html>
          info@alpha.co.jp
          found@fastcompany.com
          owner@template.com
          contact@vendor-support.com
          ceo.personal@gmail.com
          donna.boynton@partner.org
        </html>
        """
        pages = [HtmlPageResult(url="https://example.co.jp/contact", html=html)]
        emails = service._extract_rule_emails("https://alpha.co.jp", pages)
        self.assertEqual(
            [
                "info@alpha.co.jp",
                "contact@vendor-support.com",
                "ceo.personal@gmail.com",
                "donna.boynton@partner.org",
            ],
            emails,
        )

    def test_extract_rule_representative_reads_next_line_value(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        html = """
        <html>
          <ul>
            <li><p>会社名</p><p>川上建材株式会社</p></li>
            <li><p>代表者</p><p>川上　雅央</p></li>
          </ul>
        </html>
        """
        representative, quote = service._extract_rule_representative_from_html(html)
        self.assertEqual("川上 雅央", representative)
        self.assertIn("代表者", quote)
        self.assertIn("川上", quote)

    def test_extract_rule_representative_skips_greeting_headings(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        representative, quote = service._extract_rule_representative_from_html(
            "<html><h2>代表あいさつ</h2></html>"
        )
        self.assertEqual("", representative)
        self.assertEqual("", quote)

    def test_discover_emails_uses_rule_representative_before_llm(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        service._map_site = lambda start_url: [start_url]  # type: ignore[method-assign]
        service._select_urls_for_scrape = lambda **_: ["https://example.co.jp/company"]  # type: ignore[method-assign]
        service._scrape_html_pages = lambda urls: [  # type: ignore[method-assign]
            HtmlPageResult(
                url=str(urls[0]),
                html="<html><p>代表者</p><p>川上　雅央</p></html>",
            )
        ]
        service._extract_rule_emails = lambda start_url, pages: []  # type: ignore[method-assign]
        service._llm = _FailIfCalledLlm()  # type: ignore[assignment]

        result = service.discover_emails(
            company_name="川上建材株式会社",
            homepage="https://example.co.jp",
        )

        self.assertEqual("川上 雅央", result.representative)
        self.assertEqual("https://example.co.jp/company", result.evidence_url)
        self.assertIn("代表者", result.evidence_quote)

    def test_discover_emails_keeps_personal_mail_from_llm(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        service._map_site = lambda start_url: [start_url]  # type: ignore[method-assign]
        service._select_urls_for_scrape = lambda **_: ["https://example.co.jp/contact"]  # type: ignore[method-assign]
        service._scrape_html_pages = lambda urls: [  # type: ignore[method-assign]
            HtmlPageResult(url=str(urls[0]), html="<html>contact page</html>")
        ]
        service._extract_rule_emails = lambda start_url, pages: []  # type: ignore[method-assign]
        service._llm = _DummyLlm(
            HtmlContactExtraction(
                company_name="Example",
                representative="山田 太郎",
                emails=["info@alpha.co.jp", "ceo.personal@gmail.com"],
                evidence_url="https://example.co.jp/contact",
                evidence_quote="山田 太郎",
            )
        )
        result = service.discover_emails(
            company_name="Example",
            homepage="https://example.co.jp",
            allow_llm_email_extraction=True,
        )
        self.assertEqual([], result.emails)
        self.assertEqual("山田 太郎", result.representative)

    def test_discover_emails_keeps_full_html_for_rule_path_and_only_truncates_llm_path(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        long_html = "<html>" + ("A" * 300000) + " contact@alpha.co.jp </html>"
        seen_rule_html_lengths: list[int] = []

        service._map_site = lambda start_url: [start_url]  # type: ignore[method-assign]
        service._select_urls_for_scrape = lambda **_: ["https://example.co.jp/contact"]  # type: ignore[method-assign]
        service._scrape_html_pages = lambda urls: [  # type: ignore[method-assign]
            HtmlPageResult(url=str(urls[0]), html=long_html)
        ]

        def _capture_rule_emails(start_url: str, pages: list[HtmlPageResult]) -> list[str]:
            seen_rule_html_lengths.append(len(pages[0].html))
            return []

        service._extract_rule_emails = _capture_rule_emails  # type: ignore[method-assign]
        llm = _CaptureLlm()
        service._llm = llm  # type: ignore[assignment]

        service.discover_emails(
            company_name="Example",
            homepage="https://example.co.jp",
            allow_llm_email_extraction=True,
        )

        self.assertEqual([len(long_html)], seen_rule_html_lengths)
        self.assertIsNotNone(llm.last_pages)
        self.assertLess(len(llm.last_pages[0]["html"]), len(long_html))
        self.assertIn("页面内容过长已截断", llm.last_pages[0]["html"])

    def test_scrape_html_pages_requests_full_html_from_supported_crawler(self) -> None:
        long_html = "<html>" + ("A" * 300000) + " contact@alpha.co.jp </html>"
        crawler = _CaptureFullHtmlCrawler(long_html)
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=crawler,
        )

        pages = service._scrape_html_pages(["https://example.co.jp/contact"])

        self.assertEqual(False, crawler.last_truncate_html)
        self.assertEqual(long_html, pages[0].html)

    def test_split_emails_rejects_placeholder_and_url_embedded_noise(self) -> None:
        values = [
            "info@alpha.co.jp",
            "xxxxx@yourdmain.co.jp",
            "name@email.com",
            "contact@sample-corp.co.jp",
            "abcd@examplemail.jp",
            "fsample@alpha.co.jp",
            "exsample@alpha.co.jp",
            "uff09example@alpha.co.jp",
            "info@xxxxxx-bento.com",
            "n@mockconsole.prototype",
            "n@t.prototype.render",
            "http://soumu@icco2012.com",
            "https://kishubaiko.jp/info@kishubaiko.jp",
            "owner@template.com",
            "example.mail@site.com.br",
            "john.smith@xyzmail.com",
            "hello@01gov.com",
            "athello@01gov.com",
            "ceo.personal@gmail.com",
        ]
        self.assertEqual(
            ["info@alpha.co.jp", "hello@01gov.com", "ceo.personal@gmail.com"],
            split_emails(values),
        )


if __name__ == "__main__":
    unittest.main()
