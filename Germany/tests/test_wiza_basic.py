import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from germany_crawler.sites.common import cli_common
from germany_crawler.delivery import build_delivery_bundle
from germany_crawler.sites.common.enrich import merge_representatives
from germany_crawler.sites.common.enrich import normalize_person_name
from germany_crawler.sites.common.enrich import normalize_website_url
from germany_crawler.sites.common.store import GermanyCompanyStore
from germany_crawler.sites.wiza.client import COMPANY_FILTER
from germany_crawler.sites.wiza.client import load_runtime_login_state
from germany_crawler.sites.wiza.client import parse_cookie_header
from germany_crawler.sites.wiza.pipeline import run_pipeline_list as run_wiza_pipeline_list


class GermanyWizaTestCase(unittest.TestCase):
    def test_run_p1_with_resume_retries_then_succeeds(self) -> None:
        calls: list[int] = []

        class Args:
            delay = 0
            max_pages = 0
            list_workers = 1

        def flaky_runner(**kwargs) -> dict[str, int]:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("temporary failure")
            self.assertIn("output_dir", kwargs)
            return {"pages": 3, "new_companies": 5, "total_companies": 8}

        with TemporaryDirectory() as tmp_dir:
            old_delay = cli_common.P1_RETRY_DELAY_SECONDS
            cli_common.P1_RETRY_DELAY_SECONDS = 0
            try:
                result, error = cli_common._run_p1_with_resume(
                    "wiza",
                    Path(tmp_dir),
                    "http://127.0.0.1:7897",
                    Args(),
                    flaky_runner,
                )
            finally:
                cli_common.P1_RETRY_DELAY_SECONDS = old_delay

        self.assertEqual(error, "")
        self.assertEqual(result, {"pages": 3, "new_companies": 5, "total_companies": 8})
        self.assertEqual(len(calls), 2)

    def test_run_p1_with_resume_retries_usage_limit_without_consuming_retry_budget(self) -> None:
        calls: list[int] = []

        class Args:
            delay = 0
            max_pages = 0
            list_workers = 1

        def flaky_runner(**kwargs) -> dict[str, int]:
            calls.append(1)
            if len(calls) <= 2:
                raise RuntimeError("Wiza 当前账号搜索额度已用尽，暂时无法继续抓公司列表。")
            self.assertIn("output_dir", kwargs)
            return {"pages": 1, "new_companies": 2, "total_companies": 2}

        with TemporaryDirectory() as tmp_dir:
            old_usage_delay = cli_common.P1_USAGE_LIMIT_RETRY_DELAY_SECONDS
            cli_common.P1_USAGE_LIMIT_RETRY_DELAY_SECONDS = 0
            try:
                with patch("time.sleep", return_value=None):
                    result, error = cli_common._run_p1_with_resume(
                        "wiza",
                        Path(tmp_dir),
                        "http://127.0.0.1:7897",
                        Args(),
                        flaky_runner,
                    )
            finally:
                cli_common.P1_USAGE_LIMIT_RETRY_DELAY_SECONDS = old_usage_delay

        self.assertEqual(error, "")
        self.assertEqual(result, {"pages": 1, "new_companies": 2, "total_companies": 2})
        self.assertEqual(len(calls), 3)

    def test_run_p1_with_resume_retries_transient_error_without_consuming_retry_budget(self) -> None:
        calls: list[int] = []

        class Args:
            delay = 0
            max_pages = 0
            list_workers = 1

        def flaky_runner(**kwargs) -> dict[str, int]:
            calls.append(1)
            if len(calls) <= 2:
                raise RuntimeError("Expecting value: line 1 column 1 (char 0)")
            self.assertIn("output_dir", kwargs)
            return {"pages": 1, "new_companies": 3, "total_companies": 3}

        with TemporaryDirectory() as tmp_dir:
            old_retry_delay = cli_common.P1_TRANSIENT_RETRY_DELAY_SECONDS
            cli_common.P1_TRANSIENT_RETRY_DELAY_SECONDS = 0
            try:
                with patch("time.sleep", return_value=None):
                    result, error = cli_common._run_p1_with_resume(
                        "wiza",
                        Path(tmp_dir),
                        "http://127.0.0.1:7897",
                        Args(),
                        flaky_runner,
                    )
            finally:
                cli_common.P1_TRANSIENT_RETRY_DELAY_SECONDS = old_retry_delay

        self.assertEqual(error, "")
        self.assertEqual(result, {"pages": 1, "new_companies": 3, "total_companies": 3})
        self.assertEqual(len(calls), 3)

    def test_merge_representatives_keeps_only_p3_when_p1_empty(self) -> None:
        value = merge_representatives("", "Jane Doe", "Example Company")
        self.assertEqual(value, "Jane Doe")

    def test_normalize_person_name_rejects_company_like_value(self) -> None:
        self.assertEqual(normalize_person_name("Example Trading GmbH", "Example Trading GmbH"), "")

    def test_normalize_person_name_normalizes_latin_casing(self) -> None:
        self.assertEqual(normalize_person_name("max mustermann", "Example Company"), "Max Mustermann")

    def test_normalize_website_url_rejects_fake_or_noisy_value(self) -> None:
        self.assertEqual(normalize_website_url("http://+49 30 123456"), "")
        self.assertEqual(normalize_website_url("https://share.google/abc"), "")
        self.assertEqual(normalize_website_url("https://example.de/,"), "https://example.de/")

    def test_wiza_cookie_header_extracts_auth_cookies(self) -> None:
        cookies = parse_cookie_header("foo=1; user_secret=abc; wz_session=def")
        self.assertEqual(cookies["user_secret"], "abc")
        self.assertEqual(cookies["wz_session"], "def")

    def test_wiza_login_state_reads_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "login_state.json"
            state_path.write_text(
                '{"cookies":{"user_secret":"abc","wz_session":"def"},"csrf_token":"csrf","user_id":"u1","account_id":"a1"}',
                encoding="utf-8",
            )
            state = load_runtime_login_state(state_path)
        self.assertEqual(state.cookies["user_secret"], "abc")
        self.assertEqual(state.user_id, "u1")

    def test_company_filter_targets_germany(self) -> None:
        self.assertEqual(COMPANY_FILTER["v"], "germany")

    def test_wiza_list_done_checkpoint_finalizes_legacy_pending_p1(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = GermanyCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example GmbH",
                        "source_pdl_id": "pdl-1",
                        "p1_status": "pending",
                        "website": "https://example.de",
                    }
                ]
            )
            (output_dir / "list_checkpoint.json").write_text(
                '{"page": 1, "search_after": [], "status": "done"}',
                encoding="utf-8",
            )
            result = run_wiza_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0, concurrency=1)
            rows = store.get_p1_pending()
        self.assertEqual(result["pages"], 0)
        self.assertEqual(rows, [])

    def test_wiza_list_done_checkpoint_exports_unique_websites_txt(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = GermanyCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {"company_name": "Example GmbH", "website": "https://example.de"},
                    {"company_name": "Example Holdings", "website": "https://example.de"},
                    {"company_name": "Another GmbH", "website": "https://another.de"},
                    {"company_name": "Blank GmbH", "website": ""},
                ]
            )
            (output_dir / "list_checkpoint.json").write_text(
                '{"page": 1, "search_after": [], "status": "done"}',
                encoding="utf-8",
            )

            run_wiza_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0, concurrency=1)

            websites_path = output_dir / "websites.txt"
            lines = websites_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(lines, ["https://another.de", "https://example.de"])

    def test_germany_websites_delivery_uses_independent_day_sequence(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            data_root = root / "output"
            delivery_root = root / "delivery"
            wiza_dir = data_root / "wiza"
            wiza_dir.mkdir(parents=True, exist_ok=True)
            (wiza_dir / "websites.txt").write_text(
                "https://example.de\nhttps://example.de\nhttps://another.de\n",
                encoding="utf-8",
            )
            kompass_dir = data_root / "kompass"
            kompass_dir.mkdir(parents=True, exist_ok=True)
            (kompass_dir / "websites.txt").write_text(
                "https://kompass-only.de\n",
                encoding="utf-8",
            )
            (delivery_root / "Germany_day001").mkdir(parents=True, exist_ok=True)

            summary = build_delivery_bundle(data_root, delivery_root, "day1", delivery_kind="websites")

            package_dir = delivery_root / "Germany_websites_day001"
            with (package_dir / "wiza.csv").open("r", encoding="utf-8-sig", newline="") as fp:
                wiza_rows = list(csv.DictReader(fp))
            with (package_dir / "kompass.csv").open("r", encoding="utf-8-sig", newline="") as fp:
                kompass_rows = list(csv.DictReader(fp))

        self.assertEqual(summary["day"], 1)
        self.assertEqual(summary["baseline_day"], 0)
        self.assertEqual(summary["delta_websites"], 3)
        self.assertEqual(summary["total_current_websites"], 3)
        self.assertEqual(summary["sites"], {
            "kompass": {"qualified_current": 1, "delta": 1},
            "wiza": {"qualified_current": 2, "delta": 2},
        })
        self.assertEqual(summary["skipped_sites_no_delta"], [])
        self.assertFalse((package_dir / "websites.csv").exists())
        self.assertEqual(wiza_rows, [
            {"website": "https://another.de"},
            {"website": "https://example.de"},
        ])
        self.assertEqual(kompass_rows, [
            {"website": "https://kompass-only.de"},
        ])


if __name__ == "__main__":
    unittest.main()
