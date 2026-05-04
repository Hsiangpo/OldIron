from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


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

from italy_crawler.sites.dnb.client import parse_companyinformation_payload
from italy_crawler.sites.dnb.email_rules import build_email_candidates
from italy_crawler.sites.dnb.email_rules import build_common_probe_urls
from italy_crawler.sites.dnb.email_rules import build_email_fetch_plan
from italy_crawler.sites.dnb.email_rules import looks_related_subdomain_seed
from italy_crawler.sites.dnb.email_rules import select_email_urls
from italy_crawler.sites.dnb.email_service import ItalyDnbEmailService
from italy_crawler.sites.dnb.email_service import ItalyDnbEmailSettings
from italy_crawler.sites.dnb.pipeline import _build_child_segments
from italy_crawler.sites.dnb.pipeline import _needs_geo_split
from italy_crawler.sites.dnb.verif_client import extract_company_fields_from_text
from italy_crawler.sites.dnb.verif_client import pick_best_company_link


SAMPLE_DNB_PAYLOAD = {
    "countryMapValue": "Italy",
    "industryName": "Manufacturing",
    "currentPageNumber": 1,
    "pageSize": 50,
    "totalPages": 20,
    "candidatesMatchedQuantityInt": 302139,
    "companyInformationGeos": [
        {"name": "Milano", "href": "it.milano", "quantity": 19624},
        {"name": "Torino", "href": "it.torino", "quantity": 14215},
    ],
    "companyInformationCompany": [
        {
            "duns": "f330b0d35cfdeeb6ebc8f2200e2c652f",
            "primaryName": "STELLANTIS EUROPE SPA",
            "addressCountryIsoAlphaTwoCode": "IT",
            "addressCountryName": "Italy",
            "addressLocalityNameFormatted": "Torino",
            "addressRegionNameFormatted": "Torino",
            "primaryAddress": {
                "postalCode": "10100",
                "streetAddress": {"line1": "Corso G. Agnelli 200"},
            },
        }
    ],
}


class ItalyDnbBasicTests(unittest.TestCase):
    def test_parse_companyinformation_payload_keeps_geos_and_company_names(self) -> None:
        page = parse_companyinformation_payload(SAMPLE_DNB_PAYLOAD, "manufacturing")

        self.assertEqual(page.country_name, "Italy")
        self.assertEqual(page.matched_count, 302139)
        self.assertEqual(len(page.geos), 2)
        self.assertEqual(page.geos[0]["href"], "it.milano")
        self.assertEqual(page.records[0]["company_name"], "STELLANTIS EUROPE SPA")
        self.assertEqual(page.records[0]["city"], "Torino")

    def test_geo_split_builds_region_and_city_children(self) -> None:
        page = parse_companyinformation_payload(SAMPLE_DNB_PAYLOAD, "manufacturing")
        task = type(
            "Task",
            (),
            {
                "industry_path": "manufacturing",
                "country_iso_two_code": "it",
                "region_name": "",
                "city_name": "",
                "segment_id": "manufacturing|it||",
            },
        )()

        self.assertTrue(_needs_geo_split(task, page))
        children = _build_child_segments(task, page.geos)

        self.assertEqual(children[0]["segment_id"], "manufacturing|it|milano|")
        self.assertEqual(children[1]["segment_id"], "manufacturing|it|torino|")

    def test_pick_best_company_link_prefers_exact_name(self) -> None:
        picked = pick_best_company_link(
            [
                ("STELLANTIS N.V.", "https://www.verif.com/en/company/stellantis-nv"),
                ("STELLANTIS EUROPE SPA", "https://www.verif.com/en/company/stellantis-europe-spa"),
            ],
            "STELLANTIS EUROPE SPA",
        )
        self.assertEqual(
            picked,
            ("STELLANTIS EUROPE SPA", "https://www.verif.com/en/company/stellantis-europe-spa"),
        )

    def test_extract_company_fields_from_text_reads_verif_labels(self) -> None:
        website, representative = extract_company_fields_from_text(
            """
            Company
            STELLANTIS EUROPE SPA
            Website
            www.stellantis.com
            Most senior leader
            John Elkann
            """
        )
        self.assertEqual(website, "https://www.stellantis.com")
        self.assertEqual(representative, "John Elkann")

    def test_email_rule_selection_keeps_homepage_and_contact_pages_first(self) -> None:
        start_url = "https://example.it"
        urls = [
            "https://example.it/contact",
            "https://example.it/privacy-policy",
            "https://example.it/about-us/team",
            "https://example.it/blog/post-1",
            "https://example.it/legal",
            "https://example.it/careers",
        ]

        candidates = build_email_candidates(start_url, urls)
        selected = select_email_urls(candidates)
        plan = build_email_fetch_plan(
            start_url,
            selected,
            email_soft_limit=2,
            email_hard_limit=4,
            total_hard_limit=5,
        )

        self.assertEqual(plan["homepage_primary_urls"], [start_url])
        self.assertEqual(plan["email_primary_urls"][0], "https://example.it/contact")
        self.assertIn("https://example.it/privacy-policy", plan["email_primary_urls"])
        self.assertIn("https://example.it/careers", plan["email_overflow_urls"])

    def test_common_probe_urls_include_contact_and_privacy(self) -> None:
        urls = build_common_probe_urls("https://example.it")
        self.assertIn("https://example.it/contact", urls)
        self.assertIn("https://example.it/privacy-policy", urls)

    def test_related_subdomain_seed_detects_contact_subdomain(self) -> None:
        self.assertTrue(looks_related_subdomain_seed("https://careers.example.it/contact", "https://www.example.it"))
        self.assertFalse(looks_related_subdomain_seed("https://www.example.it/blog/post-1", "https://www.example.it"))

    def test_email_service_can_use_common_probe_and_related_subdomain_urls(self) -> None:
        homepage = "https://acmeholdings.it"

        class FakeCrawler:
            def __init__(self) -> None:
                self.scrape_calls: list[str] = []
                self.map_calls: list[str] = []

            def close(self) -> None:
                return None

            def scrape_html(self, url: str, *, truncate_html: bool = False):
                del truncate_html
                self.scrape_calls.append(url)
                if url == homepage:
                    return SimpleNamespace(
                        url=url,
                        html='<html><a href="https://careers.acmeholdings.it/contact">Jobs</a></html>',
                    )
                return SimpleNamespace(url=url, html="")

            def map_site(self, url: str, *, limit: int = 200):
                del limit
                self.map_calls.append(url)
                if url == homepage:
                    return []
                if url == "https://careers.acmeholdings.it/contact":
                    return ["https://careers.acmeholdings.it/contact"]
                return []

            def scrape_html_pages(self, urls: list[str], *, truncate_html: bool = False):
                del truncate_html
                pages = []
                for url in urls:
                    if "careers.acmeholdings.it/contact" in url:
                        pages.append(SimpleNamespace(url=url, html="<html>jobs@acmeholdings.it</html>"))
                return pages

        service = ItalyDnbEmailService(ItalyDnbEmailSettings(proxy_url="http://127.0.0.1:7897"))
        service._crawler = FakeCrawler()
        try:
            result = service.discover_emails(homepage)
        finally:
            service.close()

        self.assertIn("jobs@acmeholdings.it", result.emails)
