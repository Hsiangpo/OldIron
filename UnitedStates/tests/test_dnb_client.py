"""DNB 列表接口解析测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


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


class DnbClientTests(unittest.TestCase):
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
