from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from england_crawler.sites.companyname.companies_house import CompaniesHouseOfficer
from england_crawler.sites.companyname.companies_house import choose_best_search_result
from england_crawler.sites.companyname.companies_house import parse_officers_page
from england_crawler.sites.companyname.companies_house import parse_search_results
from england_crawler.sites.companyname.companies_house import select_representative_names


class CompaniesHouseParsingTests(unittest.TestCase):
    def test_search_exact_normalized_match(self) -> None:
        html = """
        <ul id="results">
          <li><a href="/company/02468686">AVIVA PLC</a> 02468686 - Incorporated on 9 February 1990</li>
          <li><a href="/company/04101873">AVIVA DOMAINS LIMITED</a> 04101873 - Dissolved on 3 February 2015</li>
        </ul>
        """
        results = parse_search_results(html, "https://find-and-update.company-information.service.gov.uk")
        matched = choose_best_search_result("AVIVA PLC", results)
        self.assertIsNotNone(matched)
        self.assertEqual("02468686", matched.company_number)

    def test_search_suffix_stripped_containment_match(self) -> None:
        html = """
        <ul id="results">
          <li><a href="/company/02468686">AVIVA PLC</a> 02468686 - Incorporated on 9 February 1990</li>
          <li><a href="/company/15103210">AVIVA ADVISERS LIMITED</a> 15103210 - Incorporated on 29 August 2023</li>
        </ul>
        """
        results = parse_search_results(html, "https://find-and-update.company-information.service.gov.uk")
        matched = choose_best_search_result("AVIVA", results)
        self.assertIsNotNone(matched)
        self.assertEqual("02468686", matched.company_number)

    def test_search_ambiguous_non_match_returns_none(self) -> None:
        html = """
        <ul id="results">
          <li><a href="/company/10000001">ALPHA TRADING LIMITED</a> 10000001 - Incorporated on 1 January 2020</li>
          <li><a href="/company/10000002">ALPHA PROPERTY LIMITED</a> 10000002 - Incorporated on 1 January 2020</li>
        </ul>
        """
        results = parse_search_results(html, "https://find-and-update.company-information.service.gov.uk")
        matched = choose_best_search_result("ALPHA", results)
        self.assertIsNone(matched)

    def test_parse_officers_current_humans_in_order(self) -> None:
        html = """
        <div class="appointments-list">
          <div class="appointment-1">
            <h2 class="heading-medium">SMITH, John David</h2>
            <span id="officer-status-tag-1" class="status-tag font-xsmall">Active</span>
            <dd id="officer-role-1" class="data">Director</dd>
          </div>
          <div class="appointment-2">
            <h2 class="heading-medium">DOE, Jane Mary</h2>
            <span id="officer-status-tag-2" class="status-tag font-xsmall">Active</span>
            <dd id="officer-role-2" class="data">Secretary</dd>
          </div>
          <div class="appointment-3">
            <h2 class="heading-medium">BROWN, Alex</h2>
            <span id="officer-status-tag-3" class="status-tag font-xsmall">Resigned</span>
            <dd id="officer-role-3" class="data">Director</dd>
          </div>
        </div>
        """
        officers = parse_officers_page(html)
        names = select_representative_names(officers)
        self.assertEqual(["SMITH, John David", "DOE, Jane Mary"], names)

    def test_parse_officers_prefers_humans_over_company_entities(self) -> None:
        officers = [
            CompaniesHouseOfficer(name="ACME HOLDINGS LIMITED", role="Director", is_active=True),
            CompaniesHouseOfficer(name="DOE, Jane Mary", role="Director", is_active=True),
        ]
        self.assertEqual(["DOE, Jane Mary"], select_representative_names(officers))

    def test_parse_officers_uses_company_entities_when_no_humans(self) -> None:
        officers = [
            CompaniesHouseOfficer(name="ACME HOLDINGS LIMITED", role="Director", is_active=True),
            CompaniesHouseOfficer(name="BRAVO INVESTMENTS LLP", role="Director", is_active=True),
        ]
        self.assertEqual(
            ["ACME HOLDINGS LIMITED", "BRAVO INVESTMENTS LLP"],
            select_representative_names(officers),
        )


if __name__ == "__main__":
    unittest.main()
