import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class FirecrawlEmailServiceTests(unittest.TestCase):
    def test_settings_validate_accepts_existing_keys_file(self) -> None:
        from england_crawler.fc_email.email_service import FirecrawlEmailSettings

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            keys_file = root / "firecrawl_keys.txt"
            keys_file.write_text("fc-demo-key\n", encoding="utf-8")
            settings = FirecrawlEmailSettings(
                project_root=root,
                keys_inline=[],
                keys_file=keys_file,
                llm_api_key="llm-demo",
                llm_model="gpt-5.1-codex-mini",
            )

            settings.validate()

    def test_ensure_keys_file_keeps_existing_file_when_inline_empty(self) -> None:
        from england_crawler.fc_email.email_service import FirecrawlEmailService

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            keys_file = root / "firecrawl_keys.txt"
            keys_file.write_text("fc-demo-key\n", encoding="utf-8")

            FirecrawlEmailService.ensure_keys_file(keys_file, [])

            self.assertEqual("fc-demo-key", keys_file.read_text(encoding="utf-8").strip())

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
