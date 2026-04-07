"""邮箱服务生命周期测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.oldiron_core.fc_email.email_service import FirecrawlEmailService
from shared.oldiron_core.fc_email.email_service import FirecrawlEmailSettings
from shared.oldiron_core.fc_email.email_service import extract_domain
from shared.oldiron_core.fc_email.client import HtmlPageResult
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

    def extract_contacts_from_html(self, **_: object) -> HtmlContactExtraction:
        return self._result


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


class _CaptureFullHtmlCrawler:
    def __init__(self, html: str) -> None:
        self._html = html
        self.last_truncate_html: bool | None = None

    def scrape_html_pages(self, urls: list[str], *, truncate_html: bool = True) -> list[HtmlPageResult]:
        self.last_truncate_html = truncate_html
        return [HtmlPageResult(url=str(urls[0]), html=self._html)]


class FirecrawlEmailServiceLifecycleTests(unittest.TestCase):
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
            "ceo.personal@gmail.com",
        ]
        self.assertEqual(
            ["info@alpha.co.jp", "ceo.personal@gmail.com"],
            split_emails(values),
        )


if __name__ == "__main__":
    unittest.main()
