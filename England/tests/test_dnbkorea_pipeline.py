import sys
import threading
import unittest
from pathlib import Path
import tempfile
from unittest.mock import Mock
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandPipelineTests(unittest.TestCase):
    def test_detail_worker_does_not_resync_queue_every_loop(self) -> None:
        from england_crawler.dnb.client import DnbClient
        from england_crawler.dnb.config import DnbEnglandConfig
        from england_crawler.dnb.pipeline import DnbEnglandPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbEnglandConfig(
                project_root=root,
                output_dir=root / "output",
                store_db_path=root / "output" / "store.db",
                snov_client_id="id",
                snov_client_secret="secret",
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                max_companies=0,
                dnb_pipeline_workers=1,
                dnb_workers=2,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                detail_task_max_retries=8,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbEnglandPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_snov=True,
                )
                runner.detail_queue.close()
                queue = Mock()
                queue.claim.return_value = None
                runner.detail_queue = queue

                with patch("england_crawler.dnb.pipeline.time.sleep", side_effect=lambda _v: runner.stop_event.set()):
                    runner._detail_worker()

                self.assertEqual(0, queue.sync_from_companies.call_count)
                self.assertEqual(1, queue.claim.call_count)
            finally:
                runner._snov_domain_cache.close()
                runner.store.close()
                base.session.close()

    def test_detail_backlog_exceeded_when_pending_or_gap_too_large(self) -> None:
        from england_crawler.dnb.pipeline import _detail_backlog_exceeded

        self.assertIs(False, _detail_backlog_exceeded(companies_total=100, detail_done=90, pending_tasks=5))
        self.assertIs(True, _detail_backlog_exceeded(companies_total=6000, detail_done=500, pending_tasks=10))
        self.assertIs(True, _detail_backlog_exceeded(companies_total=100, detail_done=95, pending_tasks=6000))

    def test_create_dnb_client_factory_returns_thread_local_clients(self) -> None:
        from england_crawler.dnb.client import DnbClient
        from england_crawler.dnb.client import RateLimitConfig
        from england_crawler.dnb.pipeline import _create_dnb_client_factory

        base = DnbClient(
            rate_config=RateLimitConfig(min_delay=0.1, max_delay=0.2),
            cookie_header="foo=bar",
        )
        try:
            get_client = _create_dnb_client_factory(base)
            main_client = get_client()
            worker_ids: list[int] = []

            def _worker() -> None:
                worker_ids.append(id(get_client()))

            thread = threading.Thread(target=_worker)
            thread.start()
            thread.join()

            self.assertNotEqual(id(main_client), worker_ids[0])
            self.assertEqual("foo=bar", main_client.cookie_header)
            self.assertEqual(base.rate_config.min_delay, main_client.rate_config.min_delay)
        finally:
            base.session.close()

    def test_build_seed_rows_returns_full_naics_catalog(self) -> None:
        from england_crawler.dnb.pipeline import _build_seed_rows

        rows = _build_seed_rows("gb")

        self.assertEqual(327, len(rows))
        segment_ids = {str(row["segment_id"]) for row in rows}
        self.assertIn("construction|gb||", segment_ids)
        self.assertIn("general_medical_and_surgical_hospitals|gb||", segment_ids)
        self.assertIn("mining_quarrying_and_oil_and_gas_extraction|gb||", segment_ids)

    def test_runner_uses_external_seed_file_when_configured(self) -> None:
        from england_crawler.dnb.client import DnbClient
        from england_crawler.dnb.config import DnbEnglandConfig
        from england_crawler.dnb.pipeline import DnbEnglandPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seed_path = root / "segments.jsonl"
            seed_path.write_text(
                '{"industry_path":"custom_industry","country_iso_two_code":"gb","region_name":"","city_name":"","expected_count":0}\n',
                encoding="utf-8",
            )
            config = DnbEnglandConfig(
                project_root=root,
                output_dir=root / "output",
                store_db_path=root / "output" / "store.db",
                snov_client_id="id",
                snov_client_secret="secret",
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                max_companies=0,
                dnb_pipeline_workers=1,
                dnb_workers=2,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                detail_task_max_retries=8,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
                seed_file_path=seed_path,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbEnglandPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_snov=True,
                )

                rows = runner._seed_rows()
            finally:
                runner._snov_domain_cache.close()
                runner.detail_queue.close()
                runner.store.close()
                base.session.close()

        self.assertEqual(1, len(rows))
        self.assertEqual("custom_industry", rows[0]["industry_path"])
        self.assertEqual("custom_industry|gb||", rows[0]["segment_id"])

    def test_build_page_signature_uses_duns_and_url(self) -> None:
        from england_crawler.dnb.models import CompanyRecord
        from england_crawler.dnb.pipeline import _build_page_signature

        signature = _build_page_signature(
            [
                CompanyRecord(duns="D1", company_name_url="foo.1"),
                CompanyRecord(duns="", company_name_url="foo.2", company_name_en_dnb="Foo Ltd"),
            ]
        )

        self.assertEqual(("D1", "foo.2"), signature)

    def test_discover_stable_segments_upserts_current_segment_and_geo_children(self) -> None:
        from england_crawler.dnb.client import DnbClient
        from england_crawler.dnb.config import DnbEnglandConfig
        from england_crawler.dnb.pipeline import DnbEnglandPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbEnglandConfig(
                project_root=root,
                output_dir=root / "output",
                store_db_path=root / "output" / "store.db",
                snov_client_id="id",
                snov_client_secret="secret",
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                max_companies=0,
                dnb_pipeline_workers=1,
                dnb_workers=2,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                detail_task_max_retries=8,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbEnglandPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_snov=True,
                )
                runner.store.ensure_discovery_seed("construction|gb||", 461092)

                class _DiscoveryClient:
                    def fetch_company_listing_page(self, segment, page_number: int = 1):
                        return {
                            "candidatesMatchedQuantityInt": 2100,
                            "companyInformationGeos": [
                                {"href": "gb.na", "quantity": "2,100"},
                                {"href": "gb.na.nottinghamshire", "quantity": "350"},
                            ],
                            "relatedIndustries": {},
                        }

                runner._discover_stable_segments(_DiscoveryClient())
                stats = runner.store.get_stats()
                self.assertEqual(3, stats["segments_total"])
                self.assertEqual(
                    0,
                    runner.store._scalar("SELECT COUNT(*) FROM dnb_discovery_queue WHERE status = 'pending'"),
                )
            finally:
                runner._snov_domain_cache.close()
                runner.detail_queue.close()
                runner.store.close()
                base.session.close()

    def test_fetch_detail_rows_does_not_require_page_retry_when_all_details_fail(self) -> None:
        from england_crawler.dnb.client import DnbClient
        from england_crawler.dnb.config import DnbEnglandConfig
        from england_crawler.dnb.pipeline import DnbEnglandPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbEnglandConfig(
                project_root=root,
                output_dir=root / "output",
                store_db_path=root / "output" / "store.db",
                snov_client_id="id",
                snov_client_secret="secret",
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                max_companies=0,
                dnb_pipeline_workers=1,
                dnb_workers=2,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                detail_task_max_retries=8,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbEnglandPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_snov=True,
                )

                class _FailingClient:
                    def fetch_company_profile(self, _company_name_url: str):
                        raise RuntimeError(
                            "Failed to perform, curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR"
                        )

                runner._dnb_client_factory = lambda: _FailingClient()  # type: ignore[method-assign]
                success, needs_retry = runner._fetch_detail_rows(
                    [
                        {
                            "duns": "D1",
                            "company_name_en_dnb": "Foo Co., Ltd.",
                            "company_name_url": "foo.123",
                            "address": "",
                            "city": "",
                            "region": "",
                            "country": "United Kingdom",
                            "postal_code": "",
                            "sales_revenue": "",
                        }
                    ]
                )

                self.assertEqual(0, success)
                self.assertIs(False, needs_retry)
            finally:
                runner._snov_domain_cache.close()
                runner.detail_queue.close()
                runner.store.close()
                base.session.close()

    def test_detail_task_stops_retrying_after_max_attempts(self) -> None:
        from england_crawler.dnb.config import DnbEnglandConfig
        from england_crawler.dnb.pipeline import DnbEnglandPipelineRunner
        from england_crawler.dnb.runtime.detail_queue import DetailTask
        from england_crawler.dnb.client import DnbClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbEnglandConfig(
                project_root=root,
                output_dir=root / "output",
                store_db_path=root / "output" / "store.db",
                snov_client_id="id",
                snov_client_secret="secret",
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                max_companies=0,
                dnb_pipeline_workers=1,
                dnb_workers=2,
                gmap_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                detail_task_max_retries=3,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbEnglandPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_snov=True,
                )
                runner.detail_queue.close()
                runner.detail_queue = Mock()
                runner._dnb_client_factory = lambda: Mock(fetch_company_profile=Mock(side_effect=RuntimeError("403")))  # type: ignore[method-assign]

                task = DetailTask(
                    duns="D1",
                    company_name_en_dnb="Foo Co., Ltd.",
                    company_name_url="foo.1",
                    address="",
                    city="",
                    region="",
                    country="United Kingdom",
                    postal_code="",
                    sales_revenue="",
                    retries=2,
                )

                runner._process_detail_task(task)

                runner.detail_queue.mark_failed.assert_called_once()
                runner.detail_queue.defer.assert_not_called()
            finally:
                runner._snov_domain_cache.close()
                runner.store.close()
                base.session.close()


if __name__ == "__main__":
    unittest.main()
