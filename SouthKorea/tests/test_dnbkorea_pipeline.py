import sys
import threading
import unittest
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaPipelineTests(unittest.TestCase):
    def test_build_discovery_seed_rows_matches_full_catalog(self) -> None:
        from korea_crawler.dnb.pipeline import _build_discovery_seed_rows

        rows = _build_discovery_seed_rows()

        self.assertEqual(327, len(rows))
        self.assertEqual("accommodation_and_food_services|kr||", rows[0]["segment_id"])
        self.assertEqual("wholesale_trade_agents_and_brokers|kr||", rows[-1]["segment_id"])

    def test_page_signature_uses_duns_to_detect_loops(self) -> None:
        from korea_crawler.dnb.models import CompanyRecord
        from korea_crawler.dnb.pipeline import _page_signature

        first = _page_signature(
            [
                CompanyRecord(duns="D1", company_name_url="a"),
                CompanyRecord(duns="D2", company_name_url="b"),
            ]
        )
        second = _page_signature(
            [
                CompanyRecord(duns="D1", company_name_url="x"),
                CompanyRecord(duns="D2", company_name_url="y"),
            ]
        )

        self.assertEqual(("D1", "D2"), first)
        self.assertEqual(first, second)

    def test_detail_backlog_exceeded_when_pending_or_gap_too_large(self) -> None:
        from korea_crawler.dnb.pipeline import _detail_backlog_exceeded

        self.assertIs(False, _detail_backlog_exceeded(companies_total=100, detail_done=90, pending_tasks=5))
        self.assertIs(True, _detail_backlog_exceeded(companies_total=6000, detail_done=500, pending_tasks=10))
        self.assertIs(True, _detail_backlog_exceeded(companies_total=100, detail_done=95, pending_tasks=6000))

    def test_create_dnb_client_factory_returns_thread_local_clients(self) -> None:
        from korea_crawler.dnb.client import DnbClient
        from korea_crawler.dnb.client import RateLimitConfig
        from korea_crawler.dnb.pipeline import _create_dnb_client_factory

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

    def test_fetch_detail_rows_does_not_require_page_retry_when_all_details_fail(self) -> None:
        from korea_crawler.dnb.client import DnbClient
        from korea_crawler.dnb.config import DnbKoreaConfig
        from korea_crawler.dnb.pipeline import DnbKoreaPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbKoreaConfig(
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
                site_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                site_max_retries=5,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbKoreaPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_site_name=True,
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
                            "country": "Republic Of Korea",
                            "postal_code": "",
                            "sales_revenue": "",
                        }
                    ]
                )

                self.assertEqual(0, success)
                self.assertIs(False, needs_retry)
            finally:
                runner.detail_queue.close()
                runner.store.close()
                base.session.close()

    def test_discover_stable_segments_keeps_parent_segment_when_geos_exist(self) -> None:
        from korea_crawler.dnb.client import DnbClient
        from korea_crawler.dnb.config import DnbKoreaConfig
        from korea_crawler.dnb.pipeline import DnbKoreaPipelineRunner

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = DnbKoreaConfig(
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
                site_workers=1,
                snov_workers=1,
                queue_poll_interval=0.1,
                stale_running_requeue_seconds=600,
                gmap_max_retries=3,
                site_max_retries=5,
                snov_task_max_retries=5,
                retry_backoff_cap_seconds=180.0,
            )
            base = DnbClient(cookie_header="foo=bar")
            try:
                runner = DnbKoreaPipelineRunner(
                    config=config,
                    client=base,
                    skip_dnb=False,
                    skip_gmap=True,
                    skip_site_name=True,
                    skip_snov=True,
                )
                runner.store.ensure_discovery_seeds(
                    [
                        {
                            "segment_id": "construction|kr||",
                            "industry_path": "construction",
                            "country_iso_two_code": "kr",
                            "region_name": "",
                            "city_name": "",
                            "expected_count": 0,
                        }
                    ]
                )

                class _DiscoveryClient:
                    def fetch_company_listing_page(self, segment, page_number: int = 1):
                        return {
                            "candidatesMatchedQuantityInt": 224288,
                            "companyInformationGeos": [
                                {"href": "kr.gyeonggi", "quantity": "65568"},
                                {"href": "kr.seoul", "quantity": "43651"},
                            ],
                            "relatedIndustries": {},
                        }

                runner._discover_stable_segments(_DiscoveryClient())
                stats = runner.store.get_stats()
                self.assertGreaterEqual(stats["segments_total"], 1)
                parent = runner.store._conn.execute(
                    "SELECT COUNT(*) FROM dnb_segments WHERE segment_id = ?",
                    ("construction|kr||",),
                ).fetchone()[0]
                self.assertEqual(1, parent)
                geo_segments = runner.store._scalar(
                    "SELECT COUNT(*) FROM dnb_segments WHERE segment_id IN ('construction|kr|gyeonggi|', 'construction|kr|seoul|')"
                )
                self.assertEqual(2, geo_segments)
            finally:
                runner.detail_queue.close()
                runner.store.close()
                base.session.close()


if __name__ == "__main__":
    unittest.main()
