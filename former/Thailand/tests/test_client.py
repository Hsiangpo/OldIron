from __future__ import annotations

from thailand_crawler.client import build_company_profile_api_params
from thailand_crawler.client import build_listing_page_path
from thailand_crawler.client import build_listing_payload
from thailand_crawler.client import DnbClient
from thailand_crawler.client import parse_company_listing
from thailand_crawler.client import parse_company_profile
from thailand_crawler.models import CompanyRecord
from thailand_crawler.models import Segment


def test_build_listing_payload_includes_region_and_city() -> None:
    segment = Segment(
        industry_path="construction",
        country_iso_two_code="th",
        region_name="bangkok",
        city_name="huai_khwang",
        expected_count=744,
        segment_type="city",
    )

    assert build_listing_payload(segment, page_number=3) == {
        "pageNumber": 3,
        "industryPath": "construction",
        "countryIsoTwoCode": "th",
        "regionName": "bangkok",
        "cityName": "huai_khwang",
    }


def test_build_listing_page_path_for_city_segment() -> None:
    segment = Segment(
        industry_path="construction",
        country_iso_two_code="th",
        region_name="bangkok",
        city_name="huai_khwang",
        expected_count=744,
        segment_type="city",
    )

    assert build_listing_page_path(segment) == "/business-directory/company-information.construction.th.bangkok.huai_khwang.html"


def test_build_company_profile_api_params() -> None:
    params = build_company_profile_api_params("italian-thai_development_public_company_limited.98c5")

    assert params["language"] == "en"
    assert params["country"] == "us"
    assert params["path"].endswith("company-profiles.italian-thai_development_public_company_limited.98c5")


def test_parse_company_listing_maps_core_fields() -> None:
    payload = {
        "companyInformationCompany": [
            {
                "duns": "98c5",
                "primaryName": "ITALIAN-THAI DEVELOPMENT PUBLIC COMPANY LIMITED",
                "primaryAddress": {
                    "addressCountry": {"countryName": "Thailand"},
                    "addressLocality": {"name": "HUAI KHWANG"},
                    "addressRegion": {"name": "BANGKOK"},
                    "postalCode": "10310",
                    "streetAddress": {"line1": "2034/132-161 New Phetchaburi Road"},
                },
                "addressLocalityNameFormatted": "Huai&nbsp;Khwang",
                "addressRegionNameFormatted": "Bangkok",
                "salesRevenue": "2,047.79",
                "companyNameUrl": "italian-thai_development_public_company_limited.98c5",
            }
        ]
    }

    rows = parse_company_listing(payload)

    assert len(rows) == 1
    row = rows[0]
    assert row.duns == "98c5"
    assert row.company_name == "ITALIAN-THAI DEVELOPMENT PUBLIC COMPANY LIMITED"
    assert row.address == "2034/132-161 New Phetchaburi Road"
    assert row.city == "Huai Khwang"
    assert row.region == "Bangkok"
    assert row.country == "Thailand"
    assert row.postal_code == "10310"
    assert row.sales_revenue == "2,047.79"
    assert row.company_name_url == "italian-thai_development_public_company_limited.98c5"


def test_parse_company_profile_updates_website_domain_and_principal() -> None:
    record = CompanyRecord(duns="98c5", company_name="ITALIAN-THAI")
    payload = {
        "overview": {
            "website": "www.itd.co.th",
            "keyPrincipal": "Premchai Karnasuta",
            "phone": "",
            "tradeStyleName": "ITD",
            "formattedRevenue": "$2.05 billion",
        }
    }

    result = parse_company_profile(record, payload)

    assert result.website == "https://www.itd.co.th"
    assert result.domain == "itd.co.th"
    assert result.key_principal == "Premchai Karnasuta"
    assert result.trade_style_name == "ITD"
    assert result.formatted_revenue == "$2.05 billion"


def test_parse_company_profile_handles_null_overview() -> None:
    record = CompanyRecord(duns="null-overview", company_name="NULL")
    payload = {"overview": None}

    result = parse_company_profile(record, payload)

    assert result.duns == "null-overview"
    assert result.website == ""
    assert result.domain == ""
    assert result.key_principal == ""
    assert result.phone == ""


def test_dnb_client_accepts_optional_cookie_header() -> None:
    client = DnbClient(cookie_header="a=b; c=d")

    assert client.session.headers["Cookie"] == "a=b; c=d"
