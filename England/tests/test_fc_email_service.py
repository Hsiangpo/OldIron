import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class FirecrawlEmailServiceTests(unittest.TestCase):
    def test_discover_emails_falls_back_to_single_page_extract(self) -> None:
        from england_crawler.fc_email.client import EmailExtractResult
        from england_crawler.fc_email.client import FirecrawlError
        from england_crawler.fc_email.email_service import FirecrawlEmailService

        class _FakeClient:
            def map_site(self, url: str, *, limit: int = 200) -> list[str]:
                return ["https://example.com/contact", "https://example.com/about"]

            def extract_emails(self, urls: list[str]) -> EmailExtractResult:
                if len(urls) > 1:
                    raise FirecrawlError("firecrawl_extract_failed")
                if urls[0].endswith("/contact"):
                    return EmailExtractResult(
                        emails=["info@example.com"],
                        evidence_url=urls[0],
                        evidence_quote="info@example.com",
                        contact_form_only=False,
                    )
                return EmailExtractResult(
                    emails=[],
                    evidence_url=urls[0],
                    evidence_quote="",
                    contact_form_only=True,
                )

        class _FakeLlm:
            def pick_candidate_urls(
                self,
                *,
                company_name: str,
                domain: str,
                homepage: str,
                candidate_urls: list[str],
                target_count: int,
            ) -> list[str]:
                return candidate_urls[:target_count]

        service = FirecrawlEmailService.__new__(FirecrawlEmailService)
        service._settings = SimpleNamespace(map_limit=10, prefilter_limit=5, llm_pick_count=3, extract_max_urls=3)
        service._firecrawl = _FakeClient()
        service._llm = _FakeLlm()

        result = service.discover_emails(company_name="Example Ltd", homepage="https://example.com", domain="example.com")

        self.assertEqual(["info@example.com"], result.emails)
        self.assertEqual("https://example.com/contact", result.evidence_url)

    def test_clean_emails_filters_placeholder_local_parts(self) -> None:
        from england_crawler.fc_email.email_service import FirecrawlEmailService

        service = FirecrawlEmailService.__new__(FirecrawlEmailService)
        cleaned = service._clean_emails(
            [
                "xxx@example.com",
                "info@example.com",
                "INFO@example.com",
                "test@example.com",
            ]
        )

        self.assertEqual(["info@example.com"], cleaned)


if __name__ == "__main__":
    unittest.main()
