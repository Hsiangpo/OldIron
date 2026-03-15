import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandClientTests(unittest.TestCase):
    def test_build_session_uses_thailand_aligned_chrome110_fingerprint(self) -> None:
        from england_crawler.dnb.client import DnbClient

        created: dict[str, object] = {}

        class _FakeSession:
            def __init__(self, *, impersonate: str) -> None:
                created["impersonate"] = impersonate
                self.trust_env = True
                self.headers = {}
                self.cookies = {}
                self.proxies = {}

        with patch("england_crawler.dnb.client.cffi_requests.Session", _FakeSession):
            client = DnbClient()

        self.assertEqual("chrome110", created["impersonate"])
        self.assertIs(False, client.session.trust_env)
        self.assertEqual(
            {"http": "socks5h://127.0.0.1:7897", "https": "socks5h://127.0.0.1:7897"},
            client.session.proxies,
        )

    def test_seed_cookie_header_uses_cookie_jar_instead_of_static_header(self) -> None:
        from england_crawler.dnb.client import DnbClient

        client = DnbClient(cookie_header="foo=bar; token=a=b=c")

        self.assertIsNone(client.session.headers.get("Cookie"))
        self.assertEqual("bar", client.session.cookies.get("foo"))
        self.assertEqual("a=b=c", client.session.cookies.get("token"))

    def test_reset_session_refreshes_cookie_from_provider(self) -> None:
        from england_crawler.dnb.client import DnbClient

        class _Provider:
            def __init__(self) -> None:
                self.calls: list[bool] = []

            def get(self, *, force_refresh: bool = False) -> str:
                self.calls.append(force_refresh)
                return "foo=new"

        provider = _Provider()
        client = DnbClient(cookie_header="foo=old", cookie_provider=provider)  # type: ignore[arg-type]

        client._reset_session(refresh_cookie=True)

        self.assertEqual([True], provider.calls)
        self.assertEqual("new", client.session.cookies.get("foo"))

    def test_warm_page_skips_html_request_when_seed_cookie_exists(self) -> None:
        from england_crawler.dnb.client import DnbClient

        class _FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

        class _FakeSession:
            def __init__(self) -> None:
                self.headers = {}
                self.cookies = {}
                self.calls: list[str] = []

            def get(self, url: str, **kwargs):
                self.calls.append(url)
                return _FakeResponse()

        client = DnbClient(cookie_header="foo=bar")
        fake_session = _FakeSession()
        client.session = fake_session  # type: ignore[assignment]
        client._sleep = lambda: None  # type: ignore[method-assign]

        client._warm_page("/business-directory/company-information.construction.gb.html")

        self.assertEqual([], fake_session.calls)
        self.assertIn(
            "/business-directory/company-information.construction.gb.html",
            client._warmed_paths,
        )

    def test_request_json_retries_transport_errors_long_enough(self) -> None:
        from england_crawler.dnb.client import DnbClient

        class _FakeResponse:
            status_code = 200
            text = "{}"

            def raise_for_status(self) -> None:
                return None

        class _FakeSession:
            def __init__(self) -> None:
                self.headers = {}
                self.cookies = {}
                self.attempts = 0
                self.get_calls = 0

            def get(self, *args, **kwargs):
                self.get_calls += 1
                return _FakeResponse()

            def post(self, *args, **kwargs):
                self.attempts += 1
                if self.attempts < 5:
                    raise RuntimeError(
                        "Failed to perform, curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR"
                    )
                return _FakeResponse()

        client = DnbClient(cookie_header="foo=bar")
        client.session = _FakeSession()
        client._reset_session = lambda **_kwargs: None  # type: ignore[method-assign]
        client._sleep = lambda: None  # type: ignore[method-assign]
        with patch("england_crawler.dnb.client.time.sleep", lambda *_args, **_kwargs: None):
            payload = client._request_json(
                method="POST",
                path="/business-directory/api/companyinformation",
                headers={"accept": "application/json"},
                referer_path="/business-directory/company-information.construction.gb.html",
                json_body={"pageNumber": 1},
            )

        self.assertEqual({}, payload)

    def test_request_json_reports_seed_cookie_refresh_hint_after_transport_exhaustion(self) -> None:
        from england_crawler.dnb.client import DnbClient

        class _FakeSession:
            def __init__(self) -> None:
                self.headers = {}
                self.cookies = {}

            def post(self, *args, **kwargs):
                raise RuntimeError(
                    "Failed to perform, curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR"
                )

        client = DnbClient(cookie_header="foo=bar")
        client.session = _FakeSession()  # type: ignore[assignment]
        client._warm_page = lambda _path: None  # type: ignore[method-assign]
        client._reset_session = lambda **_kwargs: None  # type: ignore[method-assign]
        client._sleep = lambda: None  # type: ignore[method-assign]

        with patch("england_crawler.dnb.client.time.sleep", lambda *_args, **_kwargs: None):
            with self.assertRaisesRegex(RuntimeError, "会话可能已过期或被上游重置"):
                client._request_json(
                    method="POST",
                    path="/business-directory/api/companyinformation",
                    headers={"accept": "application/json"},
                    referer_path="/business-directory/company-information.construction.gb.html",
                    json_body={"pageNumber": 1},
                )

    def test_extract_child_segments_accepts_generic_geo_segments(self) -> None:
        from england_crawler.dnb.client import extract_child_segments

        payload = {
            "companyInformationGeos": [
                {"href": "gb.na", "quantity": "2,100"},
                {"href": "gb.na.nottinghamshire", "quantity": "350"},
            ]
        }

        segments = extract_child_segments(
            industry_path="construction",
            payload=payload,
            country_iso_two_code="gb",
        )

        self.assertEqual(2, len(segments))
        self.assertEqual("construction|gb|na|", segments[0].segment_id)
        self.assertEqual("region", segments[0].segment_type)
        self.assertEqual("construction|gb|na|nottinghamshire", segments[1].segment_id)
        self.assertEqual("city", segments[1].segment_type)

    def test_extract_related_industry_segments_only_expands_uk_root(self) -> None:
        from england_crawler.dnb.client import extract_related_industry_segments
        from england_crawler.dnb.models import Segment

        root_segment = Segment(industry_path="construction", country_iso_two_code="gb")
        child_segment = Segment(
            industry_path="foundation_structure_and_building_exterior_contractors",
            country_iso_two_code="gb",
        )
        payload = {
            "relatedIndustries": {
                "Foundation": "foundation_structure_and_building_exterior_contractors",
                "Residential": "residential_building_construction",
            }
        }

        root_children = extract_related_industry_segments(
            parent_segment=root_segment,
            payload=payload,
        )
        child_children = extract_related_industry_segments(
            parent_segment=child_segment,
            payload=payload,
        )

        self.assertEqual(
            [
                "foundation_structure_and_building_exterior_contractors|gb||",
                "residential_building_construction|gb||",
            ],
            [item.segment_id for item in root_children],
        )
        self.assertEqual([], child_children)

    def test_parse_company_listing_tolerates_null_nested_address_fields(self) -> None:
        from england_crawler.dnb.client import parse_company_listing

        rows = parse_company_listing(
            {
                "companyInformationCompany": [
                    {
                        "duns": "123",
                        "primaryName": "Example Ltd",
                        "companyNameUrl": "example-ltd.123",
                        "addressLocalityNameFormatted": None,
                        "addressRegionNameFormatted": None,
                        "primaryAddress": {
                            "streetAddress": None,
                            "addressCountry": None,
                            "addressLocality": None,
                            "addressRegion": None,
                            "postalCode": "W1A 1AA",
                        },
                    }
                ]
            }
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("Example Ltd", rows[0].company_name_en_dnb)
        self.assertEqual("", rows[0].address)
        self.assertEqual("", rows[0].city)
        self.assertEqual("", rows[0].region)
        self.assertEqual("", rows[0].country)
        self.assertEqual("W1A 1AA", rows[0].postal_code)

    def test_build_listing_payload_keeps_region_and_city(self) -> None:
        from england_crawler.dnb.client import build_listing_payload
        from england_crawler.dnb.models import Segment

        segment = Segment(
            industry_path="construction",
            country_iso_two_code="gb",
            region_name="na",
            city_name="nottinghamshire",
            segment_type="city",
        )

        payload = build_listing_payload(segment, page_number=3)

        self.assertEqual(
            {
                "pageNumber": 3,
                "industryPath": "construction",
                "countryIsoTwoCode": "gb",
                "regionName": "na",
                "cityName": "nottinghamshire",
            },
            payload,
        )

    def test_build_company_profile_api_params_matches_live_browser_request(self) -> None:
        from england_crawler.dnb.client import build_company_profile_api_params

        params = build_company_profile_api_params("foo-ltd.12345678")

        self.assertEqual("en", params["language"])
        self.assertEqual("us", params["country"])
        self.assertTrue(params["path"].endswith("//business-directory/company-profiles.foo-ltd.12345678"))

    def test_build_company_profile_api_params_url_encodes_non_ascii_slug(self) -> None:
        from england_crawler.dnb.client import build_company_profile_api_params

        params = build_company_profile_api_params("fête_ltd.12345678")

        self.assertIn("f%C3%AAte_ltd.12345678", params["path"])


if __name__ == "__main__":
    unittest.main()
