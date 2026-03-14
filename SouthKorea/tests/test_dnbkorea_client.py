import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaClientTests(unittest.TestCase):
    def test_industry_catalog_counts_match_directory_snapshot(self) -> None:
        from korea_crawler.dnb.catalog import INDUSTRY_CATEGORY_COUNT
        from korea_crawler.dnb.catalog import INDUSTRY_PAGE_COUNT
        from korea_crawler.dnb.catalog import INDUSTRY_SUBCATEGORY_COUNT
        from korea_crawler.dnb.catalog import build_country_industry_segments

        segments = build_country_industry_segments("kr")

        self.assertEqual(20, INDUSTRY_CATEGORY_COUNT)
        self.assertEqual(307, INDUSTRY_SUBCATEGORY_COUNT)
        self.assertEqual(327, INDUSTRY_PAGE_COUNT)
        self.assertEqual(327, len(segments))
        self.assertEqual("accommodation_and_food_services|kr||", segments[0].segment_id)
        self.assertEqual("wholesale_trade_agents_and_brokers|kr||", segments[-1].segment_id)

    def test_build_session_uses_thailand_aligned_chrome110_fingerprint(self) -> None:
        from korea_crawler.dnb.client import DnbClient

        created: dict[str, object] = {}

        class _FakeSession:
            def __init__(self, *, impersonate: str) -> None:
                created["impersonate"] = impersonate
                self.trust_env = True
                self.headers = {}
                self.cookies = {}

        with patch("korea_crawler.dnb.client.cffi_requests.Session", _FakeSession):
            client = DnbClient()

        self.assertEqual("chrome110", created["impersonate"])
        self.assertIs(False, client.session.trust_env)

    def test_seed_cookie_header_uses_cookie_jar_instead_of_static_header(self) -> None:
        from korea_crawler.dnb.client import DnbClient

        client = DnbClient(cookie_header="foo=bar; token=a=b=c")

        self.assertIsNone(client.session.headers.get("Cookie"))
        self.assertEqual("bar", client.session.cookies.get("foo"))
        self.assertEqual("a=b=c", client.session.cookies.get("token"))

    def test_reset_session_refreshes_cookie_from_provider(self) -> None:
        from korea_crawler.dnb.client import DnbClient

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
        from korea_crawler.dnb.client import DnbClient

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

        client._warm_page("/business-directory/company-information.construction.kr.html")

        self.assertEqual([], fake_session.calls)
        self.assertIn(
            "/business-directory/company-information.construction.kr.html",
            client._warmed_paths,
        )

    def test_warm_page_retries_transport_errors(self) -> None:
        from korea_crawler.dnb.client import DnbClient

        class _FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

        class _FakeSession:
            def __init__(self) -> None:
                self.headers = {}
                self.cookies = {}
                self.calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                if self.calls < 3:
                    raise RuntimeError(
                        "Failed to perform, curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR"
                    )
                return _FakeResponse()

        client = DnbClient(cookie_header="")
        client.session = _FakeSession()  # type: ignore[assignment]
        client._reset_session = lambda **_kwargs: None  # type: ignore[method-assign]
        client._sleep = lambda: None  # type: ignore[method-assign]

        with patch("korea_crawler.dnb.client.time.sleep", lambda *_args, **_kwargs: None):
            client._warm_page("/business-directory/company-information.construction.kr.html")

        self.assertEqual(3, client.session.calls)

    def test_request_json_retries_transport_errors_long_enough(self) -> None:
        from korea_crawler.dnb.client import DnbClient

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
        with patch("korea_crawler.dnb.client.time.sleep", lambda *_args, **_kwargs: None):
            payload = client._request_json(
                method="POST",
                path="/business-directory/api/companyinformation",
                headers={"accept": "application/json"},
                referer_path="/business-directory/company-information.construction.kr.html",
                json_body={"pageNumber": 1},
            )

        self.assertEqual({}, payload)

    def test_request_json_reports_seed_cookie_refresh_hint_after_transport_exhaustion(self) -> None:
        from korea_crawler.dnb.client import DnbClient

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

        with patch("korea_crawler.dnb.client.time.sleep", lambda *_args, **_kwargs: None):
            with self.assertRaisesRegex(RuntimeError, "会话可能已过期或被上游重置"):
                client._request_json(
                    method="POST",
                    path="/business-directory/api/companyinformation",
                    headers={"accept": "application/json"},
                    referer_path="/business-directory/company-information.construction.kr.html",
                    json_body={"pageNumber": 1},
                )

    def test_extract_child_segments_uses_company_information_geos(self) -> None:
        from korea_crawler.dnb.client import extract_child_segments

        payload = {
            "companyInformationGeos": [
                {"name": "Gyeonggi", "href": "kr.gyeonggi", "quantity": "65,568"},
                {"name": "Seoul", "href": "kr.seoul", "quantity": "43,651"},
            ]
        }

        segments = extract_child_segments(
            industry_path="construction",
            payload=payload,
            country_iso_two_code="kr",
        )

        self.assertEqual(2, len(segments))
        self.assertEqual("construction|kr|gyeonggi|", segments[0].segment_id)
        self.assertEqual(65568, segments[0].expected_count)
        self.assertEqual("region", segments[0].segment_type)
        self.assertEqual("construction|kr|seoul|", segments[1].segment_id)

    def test_extract_child_segments_filters_invalid_korean_region(self) -> None:
        from korea_crawler.dnb.client import extract_child_segments

        payload = {
            "companyInformationGeos": [
                {"name": "Nairobi", "href": "kr.nairobi", "quantity": "10"},
                {"name": "Gyeonggi", "href": "kr.gyeonggi", "quantity": "65,568"},
            ]
        }

        segments = extract_child_segments(
            industry_path="construction",
            payload=payload,
            country_iso_two_code="kr",
        )

        self.assertEqual(1, len(segments))
        self.assertEqual("construction|kr|gyeonggi|", segments[0].segment_id)

    def test_build_listing_payload_keeps_region_and_city(self) -> None:
        from korea_crawler.dnb.client import build_listing_payload
        from korea_crawler.dnb.models import Segment

        segment = Segment(
            industry_path="construction",
            country_iso_two_code="kr",
            region_name="seoul",
            city_name="seoul",
            segment_type="city",
        )

        payload = build_listing_payload(segment, page_number=3)

        self.assertEqual(
            {
                "pageNumber": 3,
                "industryPath": "construction",
                "countryIsoTwoCode": "kr",
                "regionName": "seoul",
                "cityName": "seoul",
            },
            payload,
        )


if __name__ == "__main__":
    unittest.main()
