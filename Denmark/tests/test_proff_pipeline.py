from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from denmark_crawler.fc_email.email_service import EmailDiscoveryResult  # noqa: E402
from denmark_crawler.google_maps import GoogleMapsPlaceResult  # noqa: E402
from denmark_crawler.sites.proff.config import ProffDenmarkConfig  # noqa: E402
from denmark_crawler.sites.proff.models import ProffCompany  # noqa: E402
from denmark_crawler.sites.proff.pipeline import ProffPipelineRunner  # noqa: E402


class _FakeSearchClient:
    def discover_search_task_keys(self, queries, max_results_per_segment: int):
        _ = max_results_per_segment
        return list(queries)

    def fetch_search_page(self, *, query: str, page: int):
        return (
            [
                ProffCompany(
                    orgnr="1",
                    company_name="Alpha ApS",
                    representative="Alice",
                    representative_role="Direktør",
                    address="Alpha Street 1, 1000 København K",
                    homepage="https://alpha.dk/",
                    email="hello@alpha.dk",
                    phone="1000",
                    source_query=query,
                    source_page=page,
                    source_url="https://example.com/1",
                ),
                ProffCompany(
                    orgnr="2",
                    company_name="Beta ApS",
                    representative="",
                    representative_role="",
                    address="Beta Street 2, 2100 København Ø",
                    homepage="",
                    email="",
                    phone="2000",
                    source_query=query,
                    source_page=page,
                    source_url="https://example.com/2",
                ),
            ],
            2,
            1,
        )


class _FakeGMapClient:
    def search_company_profile(self, query: str, company_name: str = "") -> GoogleMapsPlaceResult:
        _ = query, company_name
        return GoogleMapsPlaceResult(company_name="Beta ApS", phone="2000", website="https://beta.dk/", score=99)


class _FakeFirecrawlService:
    def discover_emails(
        self,
        *,
        company_name: str,
        homepage: str,
        domain: str = "",
        existing_representative: str = "",
    ) -> EmailDiscoveryResult:
        _ = company_name, homepage, domain, existing_representative
        return EmailDiscoveryResult(
            company_name="Beta ApS",
            representative="Bob",
            emails=["team@beta.dk"],
        )


class _BrokenFirecrawlService:
    def discover_emails(
        self,
        *,
        company_name: str,
        homepage: str,
        domain: str = "",
        existing_representative: str = "",
    ) -> EmailDiscoveryResult:
        _ = company_name, homepage, domain, existing_representative
        raise RuntimeError("llm unavailable")


class ProffPipelineTests(unittest.TestCase):
    def test_pipeline_runs_search_gmap_and_firecrawl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            keys_file = root / "firecrawl_keys.txt"
            keys_file.write_text("fc-test-key\n", encoding="utf-8")
            config = ProffDenmarkConfig.from_env(
                project_root=root,
                output_dir=root / "output",
                query_file=None,
                inline_queries=["ApS"],
                max_pages_per_query=1,
                max_companies=0,
                search_workers=1,
                gmap_workers=1,
                firecrawl_workers=1,
            )
            config.firecrawl_keys_file = keys_file
            config.firecrawl_keys_inline = ["fc-test-key"]
            config.llm_api_key = "llm-test"
            config.llm_model = "gpt-test"
            runner = ProffPipelineRunner(
                config=config,
                client=_FakeSearchClient(),
                skip_gmap=False,
                skip_firecrawl=False,
            )
            with patch.object(ProffPipelineRunner, "_get_gmap_client", return_value=_FakeGMapClient()):
                with patch.object(ProffPipelineRunner, "_get_firecrawl_service", return_value=_FakeFirecrawlService()):
                    runner.run()

            from denmark_crawler.sites.proff.store import ProffStore  # noqa: E402

            store = ProffStore(config.store_db_path)
            try:
                progress = store.get_progress()
                self.assertEqual(2, progress.final_total)
            finally:
                store.close()

    def test_pipeline_does_not_silently_swallow_llm_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            keys_file = root / "firecrawl_keys.txt"
            keys_file.write_text("fc-test-key\n", encoding="utf-8")
            config = ProffDenmarkConfig.from_env(
                project_root=root,
                output_dir=root / "output",
                query_file=None,
                inline_queries=["ApS"],
                max_pages_per_query=1,
                max_companies=0,
                search_workers=1,
                gmap_workers=1,
                firecrawl_workers=1,
            )
            config.firecrawl_keys_file = keys_file
            config.firecrawl_keys_inline = ["fc-test-key"]
            config.llm_api_key = "llm-test"
            config.llm_model = "gpt-test"
            runner = ProffPipelineRunner(
                config=config,
                client=_FakeSearchClient(),
                skip_gmap=False,
                skip_firecrawl=False,
            )
            with patch.object(ProffPipelineRunner, "_get_gmap_client", return_value=_FakeGMapClient()):
                with patch.object(ProffPipelineRunner, "_get_firecrawl_service", return_value=_BrokenFirecrawlService()):
                    runner.run()
            from denmark_crawler.sites.proff.store import ProffStore  # noqa: E402

            store = ProffStore(config.store_db_path)
            try:
                progress = store.get_progress()
                self.assertEqual(1, progress.final_total)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
