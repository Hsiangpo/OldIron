from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_DIR = ROOT.parent / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from england_crawler.sites.companyname.email_rules import EnglandRuleEmailExtractor
from england_crawler.sites.companyname.store import CompanyNameStore
from oldiron_core.fc_email.client import HtmlPageResult


class _FakeRuleEmailService:
    def __init__(self) -> None:
        self._settings = type("S", (), {"prefilter_limit": 12, "extract_max_urls": 5})()
        self.llm_touched = False

    @property
    def _llm(self):  # noqa: N802
        self.llm_touched = True
        raise AssertionError("England rule email extractor must not touch LLM")

    def _normalize_start_url(self, homepage: str, domain: str) -> str:
        return homepage or (f"https://{domain}" if domain else "")

    def _map_site(self, start_url: str) -> list[str]:
        return [f"{start_url}/contact", f"{start_url}/about"]

    def _rank_all_urls(self, start_url: str, mapped_urls: list[str]) -> list[str]:
        return [start_url, *mapped_urls]

    def _build_rule_shortlist(self, *, start_url: str, all_urls: list[str], limit: int) -> list[str]:
        return all_urls[:limit]

    def _build_final_urls(self, start_url: str, ranked_urls: list[str], candidate_urls: list[str], *, limit: int) -> list[str]:
        return [start_url, *candidate_urls][:limit]

    def _scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        return [HtmlPageResult(url=urls[0], html="<html>info@example.co.uk sales@example.co.uk</html>")]

    def _extract_rule_emails(self, start_url: str, pages: list[HtmlPageResult]) -> list[str]:
        return ["info@example.co.uk", "sales@example.co.uk"]


class EnglandRuleRefreshTests(unittest.TestCase):
    def test_rule_email_extractor_uses_rule_path_only(self) -> None:
        service = _FakeRuleEmailService()
        extractor = EnglandRuleEmailExtractor(service)
        result = extractor.discover(company_name="Acme", homepage="https://example.co.uk", domain="example.co.uk")
        self.assertEqual(["info@example.co.uk", "sales@example.co.uk"], result.emails)
        self.assertFalse(service.llm_touched)
        self.assertEqual("https://example.co.uk", result.evidence_url)

    def test_store_final_row_created_with_ch_representative_and_rule_email(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Acme Limited"])
                task = store.claim_gmap_task()
                assert task is not None
                store.complete_gmap_task(task.orgnr, "https://acme.example", "", "")
                store.update_companies_house_result(
                    task.orgnr,
                    company_number="01234567",
                    officers_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    officer_names=["SMITH, John David", "DOE, Jane Mary"],
                    representative="SMITH, John David; DOE, Jane Mary",
                )
                store.complete_firecrawl_task(
                    task.orgnr,
                    ["info@acme.example"],
                    evidence_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    representative="SMITH, John David; DOE, Jane Mary",
                    company_number="01234567",
                    officers_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    officer_names=["SMITH, John David", "DOE, Jane Mary"],
                )
                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT representative, company_number, officers_url, officers_names_json FROM companies"
                ).fetchone()
                final_count = conn.execute("SELECT COUNT(*) FROM final_companies").fetchone()[0]
                conn.close()
                self.assertEqual("SMITH, John David; DOE, Jane Mary", row[0])
                self.assertEqual("01234567", row[1])
                self.assertIn("/company/01234567/officers", row[2])
                self.assertIn("SMITH, John David", row[3])
                self.assertEqual(1, final_count)
            finally:
                store.close()

    def test_no_email_no_final_row_even_with_representative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Acme Limited"])
                task = store.claim_gmap_task()
                assert task is not None
                store.complete_gmap_task(task.orgnr, "https://acme.example", "", "")
                store.update_companies_house_result(
                    task.orgnr,
                    company_number="01234567",
                    officers_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    officer_names=["SMITH, John David"],
                    representative="SMITH, John David",
                )
                store.complete_firecrawl_task(
                    task.orgnr,
                    [],
                    evidence_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    representative="SMITH, John David",
                    company_number="01234567",
                    officers_url="https://find-and-update.company-information.service.gov.uk/company/01234567/officers",
                    officer_names=["SMITH, John David"],
                )
                conn = sqlite3.connect(db_path)
                final_count = conn.execute("SELECT COUNT(*) FROM final_companies").fetchone()[0]
                conn.close()
                self.assertEqual(0, final_count)
            finally:
                store.close()

    def test_existing_historical_representative_is_not_cleared_when_ch_lookup_is_blank(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Legacy Limited"])
                conn = sqlite3.connect(db_path)
                conn.execute(
                    "UPDATE companies SET representative = 'Legacy Rep' WHERE company_name = 'Legacy Limited'"
                )
                conn.commit()
                conn.close()
                conn = sqlite3.connect(db_path)
                orgnr = conn.execute("SELECT orgnr FROM companies").fetchone()[0]
                conn.close()
                store.update_companies_house_result(
                    orgnr,
                    company_number="",
                    officers_url="",
                    officer_names=[],
                    representative="",
                )
                conn = sqlite3.connect(db_path)
                rep = conn.execute("SELECT representative FROM companies").fetchone()[0]
                conn.close()
                self.assertEqual("Legacy Rep", rep)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
