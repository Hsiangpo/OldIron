from __future__ import annotations

import sqlite3
import sys
import tempfile
import threading
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
from england_crawler.sites.companyname.pipeline import CompanyNamePipelineRunner
from england_crawler.sites.companyname.store import CompanyNameStore
from england_crawler.sites.companyname.store import FirecrawlTask
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


class _EmailWorkerExceptionStore:
    def __init__(self, task: FirecrawlTask, stop_event: threading.Event) -> None:
        self._task = task
        self._stop_event = stop_event
        self._claimed = False
        self.deferred: list[tuple[str, float, str]] = []

    def claim_firecrawl_task(self) -> FirecrawlTask | None:
        if self._claimed:
            self._stop_event.set()
            return None
        self._claimed = True
        return self._task

    def defer_firecrawl_task(self, orgnr: str, delay_seconds: float, error: str = "") -> None:
        self.deferred.append((orgnr, delay_seconds, error))
        self._stop_event.set()


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

    def test_email_worker_exception_does_not_kill_worker_loop(self) -> None:
        stop_event = threading.Event()
        task = FirecrawlTask(
            orgnr="org-1",
            company_name="Acme Limited",
            representative="",
            website="https://acme.example",
            domain="acme.example",
            override_mode="",
            retries=0,
        )
        store = _EmailWorkerExceptionStore(task, stop_event)
        runner = CompanyNamePipelineRunner.__new__(CompanyNamePipelineRunner)
        runner.stop_event = stop_event
        runner.store = store
        runner._firecrawl_local = threading.local()
        runner._companies_house_local = threading.local()

        def _boom(_: FirecrawlTask) -> None:
            raise RuntimeError("companies house timeout")

        runner._process_firecrawl_task = _boom  # type: ignore[method-assign]
        runner._email_worker()

        self.assertEqual(1, len(store.deferred))
        self.assertEqual("org-1", store.deferred[0][0])
        self.assertIn("companies house timeout", store.deferred[0][2])


if __name__ == "__main__":
    unittest.main()
