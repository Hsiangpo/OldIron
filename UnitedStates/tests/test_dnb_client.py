"""DNB 列表接口解析测试。"""

from __future__ import annotations

import sys
import threading
import time
import unittest
from unittest import mock
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from unitedstates_crawler.sites.dnb.client import DnbBrowserHeaders
from unitedstates_crawler.sites.dnb.client import DnbCompanyInformationClient
from unitedstates_crawler.sites.dnb.client import parse_companyinformation_payload
from unitedstates_crawler.sites.dnb.client import parse_companyprofile_payload


SAMPLE_PAYLOAD = {
    "countryMapValue": "United States of America",
    "industryName": "Beverage Manufacturing",
    "currentPageNumber": 1,
    "pageSize": 50,
    "totalPages": 20,
    "companyInformationCompany": [
        {
            "duns": "b1ccf3d86ee5f975f2a30cc754a4b2b0",
            "primaryName": "PepsiCo, Inc.",
            "primaryNameForUrl": "pepsico_inc",
            "primaryAddress": {
                "addressCountry": {"isoAlpha2Code": "US"},
                "addressLocality": {"name": "Purchase"},
                "addressRegion": {"name": "New York", "abbreviatedName": "NY"},
                "postalCode": "10577-1444",
                "streetAddress": {"line1": "700 Anderson Hill Rd"},
            },
            "addressCountryIsoAlphaTwoCode": "US",
            "addressCountryName": "United States",
            "addressLocalityNameFormatted": "Purchase",
            "addressRegionNameFormatted": "New&nbsp;York",
            "salesRevenue": "93,925",
            "companyNameUrl": "pepsico_inc.b1ccf3d86ee5f975f2a30cc754a4b2b0",
        }
    ],
}

SAMPLE_PAYLOAD_WITH_NULL_ADDRESS = {
    "countryMapValue": "United States of America",
    "industryName": "Accounting",
    "currentPageNumber": 17,
    "pageSize": 50,
    "totalPages": 20,
    "companyInformationCompany": [
        {
            "duns": "x1",
            "primaryName": "Null Address Corp",
            "addressCountryIsoAlphaTwoCode": "US",
            "addressCountryName": "United States",
            "addressLocalityNameFormatted": "New York",
            "addressRegionNameFormatted": "New York",
            "primaryAddress": None,
            "salesRevenue": "",
            "companyNameUrl": "null_address_corp.x1",
        },
        None,
    ],
}

SAMPLE_DETAIL_PAYLOAD = {
    "overview": {
        "primaryName": "PepsiCo, Inc.",
        "keyPrincipal": "Ramon L Laguarta",
        "website": "www.pepsico.com",
        "phone": "?\t\t \t \t  \t\t",
    },
    "header": {
        "companyName": "PepsiCo, Inc.",
        "companyWebsiteUrl": "www.pepsico.com",
        "companyNewHeaderParameter": {
            "companyInformationForCookie": {
                "companyAddress": "700 Anderson Hill Rd",
                "companyCity": "Purchase",
                "companyState": "New York",
                "companyZip": "10577-1444",
                "companyName": "PepsiCo, Inc.",
            }
        },
    },
    "contacts": {
        "contacts": [
            {
                "name": "Ramon L Laguarta",
                "position": "Chairman of the Board and Chief Executive Officer",
            }
        ]
    },
}


class _FakeCookieProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.headers = DnbBrowserHeaders(
            user_agent="ua",
            sec_ch_ua='"Chromium";v="146"',
            sec_ch_ua_platform='"macOS"',
            accept_language="en-US,en;q=0.9",
        )

    def fetch_snapshot(self, domain_keyword: str = "dnb.com", *, force: bool = False):
        self.calls += 1
        return (
            [{"name": "sid", "value": str(self.calls), "domain": "www.dnb.com", "path": "/"}],
            self.headers,
        )

    def fetch_cookies(self, domain_keyword: str = "dnb.com"):
        cookies, _headers = self.fetch_snapshot(domain_keyword=domain_keyword, force=False)
        return cookies

    def fetch_browser_headers(self):
        return self.headers


class _BlockingCookieProvider(_FakeCookieProvider):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_started = threading.Event()
        self.release_refresh = threading.Event()

    def fetch_snapshot(self, domain_keyword: str = "dnb.com", *, force: bool = False):
        if self.calls >= 1 and force:
            self.refresh_started.set()
            self.release_refresh.wait(timeout=2.0)
        return super().fetch_snapshot(domain_keyword=domain_keyword, force=force)


class DnbClientTests(unittest.TestCase):
    def test_refresh_cookies_debounces_forced_browser_snapshot(self) -> None:
        provider = _FakeCookieProvider()
        client = DnbCompanyInformationClient(cookie_provider=provider)
        client._forced_refresh_cooldown_seconds = 60.0
        with mock.patch("unitedstates_crawler.sites.dnb.client.time.monotonic", return_value=100.0):
            self.assertTrue(client.refresh_cookies(force=True))
        with mock.patch("unitedstates_crawler.sites.dnb.client.time.monotonic", return_value=110.0):
            self.assertFalse(client.refresh_cookies(force=True))
        with mock.patch("unitedstates_crawler.sites.dnb.client.time.monotonic", return_value=170.0):
            self.assertTrue(client.refresh_cookies(force=True))
        self.assertEqual(2, provider.calls)

    def test_forced_refresh_does_not_block_existing_cookie_reads(self) -> None:
        provider = _BlockingCookieProvider()
        client = DnbCompanyInformationClient(cookie_provider=provider)
        self.assertTrue(client.refresh_cookies(force=False))

        thread = threading.Thread(target=client.refresh_cookies, kwargs={"force": True}, daemon=True)
        thread.start()
        self.assertTrue(provider.refresh_started.wait(timeout=1.0))

        start = time.monotonic()
        cookies = client._get_cookies()
        headers = client._get_browser_headers()
        elapsed = time.monotonic() - start

        provider.release_refresh.set()
        thread.join(timeout=1.0)

        self.assertLess(elapsed, 0.2)
        self.assertEqual("1", cookies[0]["value"])
        self.assertEqual("ua", headers.user_agent)

    def test_browser_cookie_provider_reuses_disk_snapshot(self) -> None:
        from unitedstates_crawler.sites.dnb.client import DnbBrowserCookieProvider

        with TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "dnb_cookie.json"
            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "DNB_COOKIE_CACHE_FILE": str(cache_file),
                        "DNB_COOKIE_CACHE_SECONDS": "2592000",
                    },
                    clear=False,
                ),
                mock.patch.object(
                    DnbBrowserCookieProvider,
                    "_fetch_snapshot_via_launch",
                    return_value=(
                        [{"name": "akaas_us", "value": "cookie1", "domain": ".dnb.com", "path": "/"}],
                        DnbBrowserHeaders(
                            user_agent="ua",
                            sec_ch_ua='"Chromium";v="146"',
                            sec_ch_ua_platform='"macOS"',
                            accept_language="en-US,en;q=0.9",
                        ),
                    ),
                ) as launch_mock,
            ):
                first = DnbBrowserCookieProvider()
                cookies_a, headers_a = first.fetch_snapshot(force=False)
                second = DnbBrowserCookieProvider()
                cookies_b, headers_b = second.fetch_snapshot(force=False)

            self.assertEqual(1, launch_mock.call_count)
            self.assertEqual(cookies_a, cookies_b)
            self.assertEqual(headers_a.user_agent, headers_b.user_agent)

    def test_parse_companyinformation_payload(self) -> None:
        parsed = parse_companyinformation_payload(SAMPLE_PAYLOAD, "beverage_manufacturing")
        self.assertEqual(1, parsed.current_page)
        self.assertEqual(20, parsed.total_pages)
        self.assertEqual("United States of America", parsed.country_name)
        self.assertEqual(1, len(parsed.records))
        record = parsed.records[0]
        self.assertEqual("PepsiCo, Inc.", record["company_name"])
        self.assertEqual("Purchase", record["city"])
        self.assertEqual("New York", record["region"])
        self.assertEqual(
            "https://www.dnb.com/business-directory/company-profiles.pepsico_inc.b1ccf3d86ee5f975f2a30cc754a4b2b0.html",
            record["detail_url"],
        )

    def test_parse_companyprofile_payload(self) -> None:
        parsed = parse_companyprofile_payload(SAMPLE_DETAIL_PAYLOAD)
        self.assertEqual("PepsiCo, Inc.", parsed.company_name)
        self.assertEqual("Ramon L Laguarta", parsed.representative)
        self.assertEqual("https://www.pepsico.com", parsed.website)
        self.assertEqual("", parsed.phone)
        self.assertEqual("700 Anderson Hill Rd", parsed.address)
        self.assertEqual("Purchase", parsed.city)
        self.assertEqual("New York", parsed.region)
        self.assertEqual("10577-1444", parsed.postal_code)

    def test_parse_companyinformation_payload_handles_null_primary_address(self) -> None:
        parsed = parse_companyinformation_payload(
            SAMPLE_PAYLOAD_WITH_NULL_ADDRESS,
            "accounting_tax_preparation_bookkeeping_and_payroll_services",
        )
        self.assertEqual(1, len(parsed.records))
        record = parsed.records[0]
        self.assertEqual("Null Address Corp", record["company_name"])
        self.assertEqual("", record["address"])
        self.assertEqual("", record["postal_code"])


if __name__ == "__main__":
    unittest.main()
