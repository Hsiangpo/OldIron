import json
import sqlite3
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

from unitedarabemirates_crawler.sites.common import cli_common
from unitedarabemirates_crawler.sites.common.enrich import merge_representatives
from unitedarabemirates_crawler.sites.common.enrich import normalize_person_name
from unitedarabemirates_crawler.sites.common.enrich import normalize_website_url
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import _looks_like_challenge
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import _filter_cookies
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import DubaiBizDirectoryClient
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import RuntimeCookieState
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import load_runtime_cookie_state
from unitedarabemirates_crawler.sites.dubaibizdirectory.client import parse_cookie_header
from unitedarabemirates_crawler.sites.dubaibizdirectory.browser import BrowserCookieState
from unitedarabemirates_crawler.sites.dayofdubai.pipeline import _decode_cf_email
from unitedarabemirates_crawler.sites.dayofdubai.pipeline import _looks_like_uae_record
from unitedarabemirates_crawler.sites.dubaibusinessdirectory.pipeline import _extract_contact_from_summary
from unitedarabemirates_crawler.sites.dubaibusinessdirectory.pipeline import _parse_companies
from unitedarabemirates_crawler.sites.hidubai.pipeline import _build_fallback_record
from unitedarabemirates_crawler.sites.hidubai.pipeline import _find_website_href
from unitedarabemirates_crawler.sites.hidubai.pipeline import _hydrate_page_items
from unitedarabemirates_crawler.sites.common.store import UaeCompanyStore
from unitedarabemirates_crawler.sites.wiza.client import load_runtime_login_state as load_wiza_login_state
from unitedarabemirates_crawler.sites.wiza.client import parse_cookie_header as parse_wiza_cookie_header
from unitedarabemirates_crawler.sites.wiza.pipeline import run_pipeline_list as run_wiza_pipeline_list
from oldiron_core.snov import SnovAuthError
from oldiron_core.snov import SnovPermissionError

from bs4 import BeautifulSoup


def _raise_snov_auth_runner(**kwargs) -> dict[str, int]:
    raise SnovAuthError("bad snov auth")


def _raise_snov_permission_runner(**kwargs) -> dict[str, int]:
    raise SnovPermissionError("permission denied")


class UaeBasicTestCase(unittest.TestCase):
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
                    "dayofdubai",
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
                        "hidubai",
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

    def test_merge_representatives_keeps_p1_then_p3(self) -> None:
        value = merge_representatives("John Smith", "Jane Doe", "Example Company")
        self.assertEqual(value, "John Smith;Jane Doe")

    def test_normalize_person_name_rejects_company_like_value(self) -> None:
        self.assertEqual(normalize_person_name("Example Trading LLC", "Example Trading LLC"), "")

    def test_normalize_person_name_rejects_gibberish_initial_pattern(self) -> None:
        self.assertEqual(normalize_person_name("P Hjkjb", "Example Company"), "")

    def test_normalize_person_name_rejects_non_person_keyword(self) -> None:
        self.assertEqual(normalize_person_name("Deep Space", "Example Company"), "")

    def test_normalize_person_name_rejects_location_or_companyish_suffix(self) -> None:
        self.assertEqual(normalize_person_name("Rajiv Dubai", "Example Company"), "")
        self.assertEqual(normalize_person_name("Adapt Publicidade", "Example Company"), "")
        self.assertEqual(normalize_person_name("Farnek Pk", "Farnek Services LLC"), "")

    def test_normalize_person_name_normalizes_latin_casing(self) -> None:
        self.assertEqual(normalize_person_name("usama mabrouk", "Example Company"), "Usama Mabrouk")

    def test_normalize_person_name_strips_inline_role_tail(self) -> None:
        self.assertEqual(
            normalize_person_name("J. M. (Daz) Wilson Executive Director Ian Jennings Chairman-Monition Intl-UK", "Example Company"),
            "J. M. (Daz) Wilson",
        )
        self.assertEqual(
            normalize_person_name("Mohd. Ashraf Mohd. Nazeer Supervisor", "Example Company"),
            "Mohd. Ashraf Mohd. Nazeer",
        )

    def test_decode_cf_email_returns_real_email(self) -> None:
        self.assertEqual(_decode_cf_email("0766586f666c586a6247626e6a296662"), "a_hak_me@eim.ae")

    def test_parse_dubai_business_directory_page_extracts_company(self) -> None:
        html = """
        <tr><td>
        <table width="100%">
        <tr><td class="regularblack"><b style='font-size: 120%'>Company Name: Example LLC</b></td></tr>
        <tr><td class="regularblack">Contact name: <b>John Smith</b></td></tr>
        <tr><td class="regularblack">Address: Dubai</td></tr>
        <tr><td class="regularblack">Contact Tel: 0500000000</td></tr>
        <tr><td class="regularblack">Email Address: <a href="mailto:hello@example.com">hello@example.com</a></td></tr>
        <tr><td class="regularblack">Web Address: https://example.com</td></tr>
        <tr><td class="regularblack">Description of services: Test company</td></tr>
        </table></td></tr>
        """
        rows = _parse_companies(html, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company_name"], "Example LLC")
        self.assertEqual(rows[0]["emails"], "hello@example.com")

    def test_normalize_website_url_rejects_fake_or_noisy_value(self) -> None:
        self.assertEqual(normalize_website_url("http://+971 553185400"), "")
        self.assertEqual(normalize_website_url("https://share.google/abc"), "")
        self.assertEqual(normalize_website_url("https://maps.app.goo.gl/abc"), "")
        self.assertEqual(normalize_website_url("https://example.com/,"), "https://example.com/")

    def test_extract_contact_from_summary_reads_real_person(self) -> None:
        summary = (
            "Business Name /Contact Person:- Valkyrie DMCC/ Maher Ghandour "
            "Country/Region:- United Arab Emirates"
        )
        self.assertEqual(
            _extract_contact_from_summary(summary, "Accounting and Bookkeeping Firm in Dubai"),
            "Maher Ghandour",
        )

    def test_hidubai_ignores_platform_social_link_as_website(self) -> None:
        html = """
        <html><body>
        <a href="https://facebook.com/officialhidubai">Facebook</a>
        <a href="https://example.com">Call now</a>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(_find_website_href(soup), "")

    def test_hidubai_fallback_record_keeps_list_fields_when_detail_fails(self) -> None:
        item = {
            "businessName": {"en": "Example LLC"},
            "website": "example.com",
            "contactPhone": "0500000000",
            "friendlyUrlName": "example-llc",
            "businessKeywords": {"en": ["alpha", "beta"]},
            "address": {"en": "Dubai"},
            "neighborhood": {"name": {"en": "Marina"}, "districtName": {"en": "Dubai"}},
        }
        record = _build_fallback_record(item)
        self.assertEqual(record["company_name"], "Example LLC")
        self.assertEqual(record["website"], "https://example.com")
        self.assertEqual(record["phone"], "0500000000")
        self.assertEqual(record["emails"], "")

    def test_hidubai_hydrate_page_items_skips_detail_error_with_fallback(self) -> None:
        item = {
            "businessName": {"en": "Example LLC"},
            "website": "example.com",
            "contactPhone": "0500000000",
            "friendlyUrlName": "example-llc",
            "businessKeywords": {"en": ["alpha", "beta"]},
            "address": {"en": "Dubai"},
            "neighborhood": {"name": {"en": "Marina"}, "districtName": {"en": "Dubai"}},
        }
        with patch("unitedarabemirates_crawler.sites.hidubai.pipeline._fetch_detail", side_effect=RuntimeError("boom")):
            rows = _hydrate_page_items([item], proxy="", concurrency=1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company_name"], "Example LLC")
        self.assertEqual(rows[0]["emails"], "")

    def test_dayofdubai_rejects_non_uae_record_with_foreign_phone(self) -> None:
        self.assertFalse(
            _looks_like_uae_record(
                "+91-9370586696",
                "",
                "Best IVF specialist for patients travelling from India to Dubai",
                "https://www.indianmedguru.com/example.html",
            )
        )

    def test_parse_cookie_header_extracts_core_cookies(self) -> None:
        cookies = parse_cookie_header("cf_clearance=abc; cf_chl_rc_ni=12; CAKEPHP=xyz; ignored")
        self.assertEqual(cookies["cf_clearance"], "abc")
        self.assertEqual(cookies["cf_chl_rc_ni"], "12")
        self.assertEqual(cookies["CAKEPHP"], "xyz")

    def test_dubaibizdirectory_filter_cookies_keeps_extended_cf_keys(self) -> None:
        cookies = _filter_cookies(
            {
                "cf_clearance": "abc",
                "cf_chl_rc_ni": "12",
                "CAKEPHP": "xyz",
                "FCCDCF": "consent",
                "FCNEC": "notice",
                "ignored": "noop",
            }
        )
        self.assertEqual(
            cookies,
            {
                "cf_clearance": "abc",
                "cf_chl_rc_ni": "12",
                "CAKEPHP": "xyz",
                "FCCDCF": "consent",
                "FCNEC": "notice",
            },
        )

    def test_load_runtime_cookie_state_reads_legacy_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cookie_path = Path(tmp_dir) / "cookie_state.json"
            cookie_path.write_text(
                '{"cf_clearance":"abc","CAKEPHP":"xyz","user_agent":"UA"}',
                encoding="utf-8",
            )
            state = load_runtime_cookie_state(cookie_path)
        self.assertEqual(state.cookies["cf_clearance"], "abc")
        self.assertEqual(state.cookies["CAKEPHP"], "xyz")
        self.assertEqual(state.user_agent, "UA")

    def test_dubaibizdirectory_challenge_detector_hits_cf_page(self) -> None:
        self.assertTrue(_looks_like_challenge(403, "window._cf_chl_opt = {}"))
        self.assertFalse(_looks_like_challenge(200, "<html>Companies in Dubai</html>"))

    def test_dubaibizdirectory_refresh_cookie_state_via_browser_updates_state(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            client = DubaiBizDirectoryClient.__new__(DubaiBizDirectoryClient)
            client._output_dir = Path(tmp_dir)
            client._state_path = Path(tmp_dir) / "cookie_state.json"
            client._proxy = ""
            client._using_proxy = False
            client._state = RuntimeCookieState(cookies={"cf_clearance": "old", "CAKEPHP": "old"}, user_agent="UA-OLD")

            class _DummySession:
                def __init__(self) -> None:
                    self.closed = 0

                def close(self) -> None:
                    self.closed += 1

            old_session = _DummySession()
            new_session = _DummySession()
            client._session = old_session
            client._new_session = lambda use_proxy: new_session  # type: ignore[method-assign]

            with patch(
                "unitedarabemirates_crawler.sites.dubaibizdirectory.client.fetch_browser_cookie_state",
                return_value=BrowserCookieState(
                    cookies={
                        "cf_clearance": "new",
                        "CAKEPHP": "cake",
                        "FCCDCF": "consent",
                        "FCNEC": "notice",
                    },
                    user_agent="UA-NEW",
                ),
            ):
                ok = client._refresh_cookie_state_via_browser("https://dubaibizdirectory.com/organisations/search/page:1")

            self.assertTrue(ok)
            self.assertEqual(client._state.cookies["cf_clearance"], "new")
            self.assertEqual(client._state.cookies["FCCDCF"], "consent")
            self.assertEqual(client._state.user_agent, "UA-NEW")
            self.assertEqual(old_session.closed, 1)

    def test_wiza_cookie_header_extracts_auth_cookies(self) -> None:
        cookies = parse_wiza_cookie_header("user_secret=abc; wz_session=xyz; ignored")
        self.assertEqual(cookies["user_secret"], "abc")
        self.assertEqual(cookies["wz_session"], "xyz")

    def test_wiza_login_state_reads_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "login_state.json"
            state_path.write_text(
                '{"cookies":{"user_secret":"abc","wz_session":"xyz"},"user_agent":"UA","csrf_token":"csrf"}',
                encoding="utf-8",
            )
            state = load_wiza_login_state(state_path)
        self.assertEqual(state.cookies["user_secret"], "abc")
        self.assertEqual(state.cookies["wz_session"], "xyz")
        self.assertEqual(state.user_agent, "UA")
        self.assertEqual(state.csrf_token, "csrf")

    def test_wiza_list_done_checkpoint_finalizes_legacy_pending_p1(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = UaeCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example LLC",
                        "source_pdl_id": "pdl_123",
                        "p1_status": "pending",
                        "representative_p1": "",
                        "representative_final": "",
                        "website": "https://example.com",
                        "address": "Dubai",
                        "phone": "",
                        "emails": "",
                        "detail_url": "https://wiza.co/app/prospect",
                        "summary": "",
                        "evidence_url": "https://wiza.co/app/prospect",
                    }
                ]
            )
            (output_dir / "list_checkpoint.json").write_text(
                json.dumps({"page": 1981, "search_after": [], "status": "done"}),
                encoding="utf-8",
            )

            result = run_wiza_pipeline_list(
                output_dir=output_dir,
                request_delay=0,
                proxy="",
                max_pages=0,
                concurrency=1,
            )

            conn = sqlite3.connect(str(output_dir / "companies.db"))
            try:
                row = conn.execute(
                    "SELECT p1_status, representative_p1 FROM companies WHERE record_id = 'examplellc'"
                ).fetchone()
            finally:
                conn.close()

        self.assertEqual(result["pages"], 0)
        self.assertEqual(result["new_companies"], 0)
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "done")
        self.assertEqual(row[1], "")

    def test_run_batch_with_timeout_propagates_snov_auth_error(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            with self.assertRaises(cli_common.BatchFatalError):
                cli_common._run_batch_with_timeout(
                    kind="pipeline3_email",
                    runner=_raise_snov_auth_runner,
                    output_dir=Path(tmp_dir),
                    max_items=1,
                    concurrency=1,
                    fatal_error_types=("SnovAuthError",),
                )

    def test_wiza_pending_checker_does_not_exit_early_when_blank_website_rows_remain(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = UaeCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example LLC",
                        "source_pdl_id": "pdl_123",
                        "website": "",
                        "emails": "",
                        "p1_status": "done",
                    }
                ]
            )
            self.assertTrue(cli_common._has_pending_work(output_dir, "pipeline3_email", site_name="wiza"))
            self.assertFalse(cli_common._has_pending_work(output_dir, "pipeline3_email", site_name="hidubai"))

    def test_run_batch_with_timeout_propagates_snov_permission_error(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            with self.assertRaises(cli_common.BatchFatalError):
                cli_common._run_batch_with_timeout(
                    kind="pipeline3_email",
                    runner=_raise_snov_permission_runner,
                    output_dir=Path(tmp_dir),
                    max_items=1,
                    concurrency=1,
                    fatal_error_types=("SnovPermissionError",),
                )


if __name__ == "__main__":
    unittest.main()
