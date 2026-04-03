"""DNB 列表接口解析测试。"""

from __future__ import annotations

import sys
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from brazil_crawler.sites.dnb.client import DnbBrowserHeaders
from brazil_crawler.sites.dnb.client import DnbCompanyInformationClient
from brazil_crawler.sites.dnb.client import parse_companyinformation_payload
from brazil_crawler.sites.dnb.client import parse_companyprofile_payload


SAMPLE_PAYLOAD = {
    "countryMapValue": "Brazil",
    "industryName": "Software Publishers",
    "currentPageNumber": 1,
    "pageSize": 50,
    "totalPages": 20,
    "companyInformationGeos": [
        {"name": "Sao&nbsp;Paulo", "href": "br.sao_paulo", "quantity": "20,116"},
        {"name": "Campinas", "href": "br.sao_paulo.campinas", "quantity": "702"},
    ],
    "companyInformationCompany": [
        {
            "duns": "c8fdd5aeb86441c36516748562a16dd5",
            "primaryName": "MLABS SOFTWARE SA",
            "primaryNameForUrl": "mlabs_software_sa",
            "primaryAddress": {
                "addressCountry": {"isoAlpha2Code": "BR"},
                "addressLocality": {"name": "SAO JOSE DOS CAMPOS"},
                "addressRegion": {"name": "SAO PAULO", "abbreviatedName": "SP"},
                "postalCode": "12246-870",
                "streetAddress": {"line1": "Av. CASSIANO RICARDO 601"},
            },
            "addressCountryIsoAlphaTwoCode": "BR",
            "addressCountryName": "Brazil",
            "addressLocalityNameFormatted": "Sao&nbsp;Jose&nbsp;Dos&nbsp;Campos",
            "addressRegionNameFormatted": "Sao&nbsp;Paulo",
            "salesRevenue": "2,856.9",
            "companyNameUrl": "mlabs_software_sa.c8fdd5aeb86441c36516748562a16dd5",
        }
    ],
}

SAMPLE_PAYLOAD_WITH_NULL_ADDRESS = {
    "countryMapValue": "Brazil",
    "industryName": "Accounting",
    "currentPageNumber": 17,
    "pageSize": 50,
    "totalPages": 20,
    "companyInformationCompany": [
        {
            "duns": "x1",
            "primaryName": "Null Address Corp",
            "addressCountryIsoAlphaTwoCode": "BR",
            "addressCountryName": "Brazil",
            "addressLocalityNameFormatted": "Sao Paulo",
            "addressRegionNameFormatted": "Sao Paulo",
            "primaryAddress": None,
            "salesRevenue": "",
            "companyNameUrl": "null_address_corp.x1",
        },
        None,
    ],
}

SAMPLE_DETAIL_PAYLOAD = {
    "overview": {
        "primaryName": "MLABS SOFTWARE SA",
        "keyPrincipal": "RAFAEL KISO",
        "website": "www.mlabs.com.br",
        "phone": "?\t\t \t \t  \t\t",
    },
    "header": {
        "companyName": "MLABS SOFTWARE SA",
        "companyWebsiteUrl": "www.mlabs.com.br",
        "companyNewHeaderParameter": {
            "companyInformationForCookie": {
                "companyAddress": "Av. CASSIANO RICARDO 601",
                "companyCity": "SAO JOSE DOS CAMPOS",
                "companyState": "SAO PAULO",
                "companyZip": "12246-870",
                "companyName": "MLABS SOFTWARE SA",
            }
        },
    },
    "contacts": {
        "contacts": [
            {
                "name": "RAFAEL KISO",
                "position": "Chief Executive Officer",
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


class DnbClientTests(unittest.TestCase):
    def test_refresh_cookies_debounces_forced_browser_snapshot(self) -> None:
        provider = _FakeCookieProvider()
        client = DnbCompanyInformationClient(cookie_provider=provider)
        client._forced_refresh_cooldown_seconds = 60.0
        with mock.patch("brazil_crawler.sites.dnb.client.time.monotonic", side_effect=[100.0, 110.0, 170.0]):
            self.assertTrue(client.refresh_cookies(force=True))
            self.assertFalse(client.refresh_cookies(force=True))
            self.assertTrue(client.refresh_cookies(force=True))
        self.assertEqual(2, provider.calls)

    def test_parse_companyinformation_payload(self) -> None:
        parsed = parse_companyinformation_payload(SAMPLE_PAYLOAD, "software_publishers")
        self.assertEqual(1, parsed.current_page)
        self.assertEqual(20, parsed.total_pages)
        self.assertEqual("Brazil", parsed.country_name)
        self.assertEqual(2, len(parsed.geos))
        self.assertEqual(1, len(parsed.records))
        record = parsed.records[0]
        self.assertEqual("MLABS SOFTWARE SA", record["company_name"])
        self.assertEqual("Sao Jose Dos Campos", record["city"])
        self.assertEqual("Sao Paulo", record["region"])
        self.assertEqual(
            "https://www.dnb.com/business-directory/company-profiles.mlabs_software_sa.c8fdd5aeb86441c36516748562a16dd5.html",
            record["detail_url"],
        )

    def test_parse_companyprofile_payload(self) -> None:
        parsed = parse_companyprofile_payload(SAMPLE_DETAIL_PAYLOAD)
        self.assertEqual("MLABS SOFTWARE SA", parsed.company_name)
        self.assertEqual("RAFAEL KISO", parsed.representative)
        self.assertEqual("https://www.mlabs.com.br", parsed.website)
        self.assertEqual("", parsed.phone)
        self.assertEqual("Av. CASSIANO RICARDO 601", parsed.address)
        self.assertEqual("SAO JOSE DOS CAMPOS", parsed.city)
        self.assertEqual("SAO PAULO", parsed.region)
        self.assertEqual("12246-870", parsed.postal_code)

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
