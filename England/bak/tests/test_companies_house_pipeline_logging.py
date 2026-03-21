import logging
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class CompaniesHousePipelineLoggingTests(unittest.TestCase):
    def test_handle_ch_task_logs_retry_detail(self) -> None:
        from england_crawler.companies_house.pipeline import CompaniesHousePipelineRunner
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        with tempfile.TemporaryDirectory() as tmp:
            config = SimpleNamespace(
                project_root=Path(tmp),
                input_xlsx=Path(tmp) / "companies.xlsx",
                output_dir=Path(tmp) / "output",
                store_db_path=Path(tmp) / "output" / "store.db",
                queue_poll_interval=1.0,
                stale_running_requeue_seconds=600,
                retry_backoff_cap_seconds=180.0,
                ch_workers=1,
                gmap_workers=1,
                snov_workers=1,
                max_companies=0,
                ch_max_retries=4,
                gmap_max_retries=3,
                snov_task_max_retries=5,
                snov_timeout_seconds=30.0,
                snov_retry_delay_seconds=10.0,
                snov_max_retries=5,
                ch_proxy=BlurpathProxyConfig(False, "", 0, "", "", "GB", 10),
            )
            config.input_xlsx.write_text("stub", encoding="utf-8")
            runner = CompaniesHousePipelineRunner(
                config,
                skip_ch=False,
                skip_gmap=True,
                skip_snov=True,
            )
            try:
                runner.store.close()
                runner.store = Mock()
                task = SimpleNamespace(
                    comp_id="c1",
                    company_name="ZZZ DEVELOPMENTS LTD",
                    retries=0,
                )
                client = Mock()
                client.search_companies.side_effect = RuntimeError("boom")

                with self.assertLogs("england_crawler.companies_house.pipeline", level=logging.INFO) as logs:
                    runner._handle_ch_task(client, task)

                joined = "\n".join(logs.output)
                self.assertIn("CH 开始：c1 | ZZZ DEVELOPMENTS LTD", joined)
                self.assertIn("CH 重试：c1 | 第1次", joined)
                self.assertIn("等待=2.0s", joined)
            finally:
                runner.snov_domain_cache.close()
                if hasattr(runner.store, "close"):
                    runner.store.close()


if __name__ == "__main__":
    unittest.main()
