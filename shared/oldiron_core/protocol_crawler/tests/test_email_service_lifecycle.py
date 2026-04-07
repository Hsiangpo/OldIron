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
                html="<html>info@example.co.jp ceo.personal@gmail.com</html>",
            )
        ]
        emails = service._extract_rule_emails("https://example.co.jp", pages)
        self.assertEqual(["info@example.co.jp", "ceo.personal@gmail.com"], emails)

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
                emails=["info@example.co.jp", "ceo.personal@gmail.com"],
                evidence_url="https://example.co.jp/contact",
                evidence_quote="山田 太郎",
            )
        )
        result = service.discover_emails(
            company_name="Example",
            homepage="https://example.co.jp",
            allow_llm_email_extraction=True,
        )
        self.assertEqual(["info@example.co.jp", "ceo.personal@gmail.com"], result.emails)


if __name__ == "__main__":
    unittest.main()
