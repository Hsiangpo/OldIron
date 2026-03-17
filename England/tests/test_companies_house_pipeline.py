import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from openpyxl import Workbook


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class CompaniesHousePipelineTests(unittest.TestCase):
    def test_firecrawl_services_share_key_pool_across_threads(self) -> None:
        from england_crawler.companies_house.pipeline import CompaniesHousePipelineRunner
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "英国.xlsx"
            source_path.write_text("placeholder", encoding="utf-8")
            output_dir = root / "output"
            config = SimpleNamespace(
                project_root=root,
                output_dir=output_dir,
                store_db_path=output_dir / "store.db",
                input_xlsx=source_path,
                max_companies=0,
                ch_workers=1,
                gmap_workers=1,
                snov_workers=2,
                queue_poll_interval=0.05,
                snapshot_flush_interval=30.0,
                stale_running_requeue_seconds=300,
                retry_backoff_cap_seconds=1.0,
                ch_proxy=BlurpathProxyConfig(False, "", 0, "", "", "GB", 10),
                firecrawl_keys_inline=["fc-test"],
                firecrawl_keys_file=output_dir / "firecrawl_keys.txt",
                firecrawl_pool_db=output_dir / "cache" / "firecrawl_keys.db",
                firecrawl_base_url="https://api.firecrawl.dev/v2/",
                firecrawl_timeout_seconds=1.0,
                firecrawl_max_retries=0,
                firecrawl_key_per_limit=1,
                firecrawl_key_wait_seconds=1,
                firecrawl_key_cooldown_seconds=1,
                firecrawl_key_failure_threshold=1,
                llm_api_key="llm-test",
                llm_base_url="https://api.gpteamservices.com/v1",
                llm_model="gpt-5.1-codex-mini",
                llm_reasoning_effort="medium",
                llm_timeout_seconds=1.0,
                firecrawl_prefilter_limit=4,
                firecrawl_llm_pick_count=2,
            )
            runner = CompaniesHousePipelineRunner(
                config,
                skip_ch=True,
                skip_gmap=True,
                skip_firecrawl=False,
            )
            try:
                main_service = runner._get_firecrawl_service()
                worker_services: list[object] = []

                def _worker() -> None:
                    worker_services.append(runner._get_firecrawl_service())

                thread = threading.Thread(target=_worker)
                thread.start()
                thread.join()

                self.assertEqual(1, len(worker_services))
                self.assertIsNot(main_service, worker_services[0])
                self.assertIs(main_service._key_pool, worker_services[0]._key_pool)
            finally:
                runner.firecrawl_domain_cache.close()
                runner.store.close()

    def test_ch_worker_handles_claimed_task(self) -> None:
        from england_crawler.companies_house.pipeline import CompaniesHousePipelineRunner
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        runner = CompaniesHousePipelineRunner.__new__(CompaniesHousePipelineRunner)
        runner.stop_event = threading.Event()
        runner.poll_interval = 0.0
        runner.config = SimpleNamespace(
            ch_timeout_seconds=1.0,
            ch_proxy=BlurpathProxyConfig(False, "", 0, "", "", "GB", 10),
        )
        task = SimpleNamespace(comp_id="c1", company_name="ZZZ DEVELOPMENTS LTD")

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def claim_ch_task(self):
                self.calls += 1
                if self.calls == 1:
                    return task
                runner.stop_event.set()
                return None

        class _FakeClient:
            def probe_proxy(self) -> tuple[bool, str]:
                return True, "direct"

            def current_session_label(self) -> str:
                return "session"

            def describe_proxy(self) -> str:
                return "direct"

            def describe_preproxy(self) -> str:
                return "-"

            def close(self) -> None:
                return None

        runner.store = _Store()

        with (
            patch("england_crawler.companies_house.pipeline.CompaniesHouseClient", return_value=_FakeClient()),
            patch.object(runner, "_handle_ch_task") as mocked,
        ):
            runner._run_ch_worker()

        mocked.assert_called_once()
        called_client, called_task = mocked.call_args.args
        self.assertEqual("session", called_client.current_session_label())
        self.assertEqual(task.comp_id, called_task.comp_id)

    def test_pipeline_exports_final_company_when_three_stages_complete(self) -> None:
        from england_crawler.companies_house.client import CompaniesHouseCandidate
        from england_crawler.companies_house.pipeline import run_companies_house_pipeline
        from england_crawler.companies_house.proxy import BlurpathProxyConfig
        from england_crawler.fc_email.email_service import EmailDiscoveryResult
        from england_crawler.google_maps.client import GoogleMapsPlaceResult

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "英国.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["CompanyName"])
            sheet.append(["ZZZ DEVELOPMENTS LTD"])
            workbook.save(source_path)

            output_dir = root / "output"
            config = SimpleNamespace(
                project_root=root,
                output_dir=output_dir,
                store_db_path=output_dir / "store.db",
                source_xlsx_path=source_path,
                max_companies=0,
                ch_workers=1,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.05,
                snapshot_flush_interval=0.05,
                stale_running_requeue_seconds=300,
                retry_backoff_cap_seconds=1.0,
                snov_timeout_seconds=1.0,
                snov_retry_delay_seconds=0.0,
                snov_max_retries=1,
                ch_proxy=BlurpathProxyConfig(False, "", 0, "", "", "GB", 10),
                firecrawl_keys_inline=["fc-test"],
                firecrawl_keys_file=output_dir / "firecrawl_keys.txt",
                firecrawl_pool_db=output_dir / "cache" / "firecrawl_keys.db",
                firecrawl_base_url="https://api.firecrawl.dev/v2/",
                firecrawl_timeout_seconds=1.0,
                firecrawl_max_retries=0,
                firecrawl_key_per_limit=1,
                firecrawl_key_wait_seconds=1,
                firecrawl_key_cooldown_seconds=1,
                firecrawl_key_failure_threshold=1,
                llm_api_key="llm-test",
                llm_base_url="https://api.gpteamservices.com/v1",
                llm_model="gpt-5.1-codex-mini",
                llm_reasoning_effort="medium",
                llm_timeout_seconds=1.0,
                firecrawl_prefilter_limit=4,
                firecrawl_llm_pick_count=2,
            )

            with (
                patch(
                    "england_crawler.companies_house.pipeline.CompaniesHouseClient.search_companies",
                    return_value=[
                        CompaniesHouseCandidate(
                            company_name="ZZZ DEVELOPMENTS LTD",
                            company_number="00000002",
                            status_text="00000002 - Incorporated on 20 April 2020",
                            address="Manchester, United Kingdom",
                            detail_path="/company/00000002",
                        )
                    ],
                ),
                patch(
                    "england_crawler.companies_house.pipeline.CompaniesHouseClient.fetch_first_active_director",
                    return_value="CHARRO, Jorge Manrique",
                ),
                patch(
                    "england_crawler.companies_house.pipeline.GoogleMapsClient.search_company_profile",
                    return_value=GoogleMapsPlaceResult(
                        company_name="ZZZ DEVELOPMENTS LTD",
                        phone="+44 20 1234 5678",
                        website="https://www.zzzdevelopments.co.uk",
                        score=100,
                    ),
                ),
                patch(
                    "england_crawler.companies_house.pipeline.FirecrawlEmailService.discover_emails",
                    return_value=EmailDiscoveryResult(emails=["jorge@zzzdevelopments.co.uk"]),
                ),
            ):
                run_companies_house_pipeline(config)

            rows = [
                json.loads(line)
                for line in (output_dir / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(1, len(rows))
        self.assertEqual("ZZZ DEVELOPMENTS LTD", rows[0]["company_name"])
        self.assertEqual("CHARRO, Jorge Manrique", rows[0]["ceo"])
        self.assertEqual("zzzdevelopments.co.uk", rows[0]["domain"])
        self.assertEqual(["jorge@zzzdevelopments.co.uk"], rows[0]["emails"])


if __name__ == "__main__":
    unittest.main()




