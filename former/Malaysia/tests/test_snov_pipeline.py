import json
from pathlib import Path

from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.ctos_directory.models import CTOSCompanyItem
from malaysia_crawler.ctos_directory.models import CTOSDirectoryPage
from malaysia_crawler.manager_agent.service import ManagerAgentResult
from malaysia_crawler.snov.pipeline import CtosBusinessListSnovPipeline


class _FakeCTOSCrawler:
    def fetch_list_page(self, prefix: str, page: int) -> CTOSDirectoryPage:
        if prefix == "s" and page == 1:
            return CTOSDirectoryPage(
                prefix="s",
                current_page=1,
                next_page=2,
                companies=[
                    CTOSCompanyItem(
                        company_name="Securepay Sdn Bhd",
                        registration_no="0000000X",
                        detail_path="/x",
                        detail_url="https://example.com/x",
                    )
                ],
            )
        return CTOSDirectoryPage(prefix=prefix, current_page=page, next_page=None, companies=[])


class _FakeBusinessListCrawler:
    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None:
        if company_id != 381082:
            return None
        return BusinessListCompany(
            company_id=381082,
            company_url="https://www.businesslist.my/company/381082/securepay-sdn-bhd",
            company_name="Securepay Sdn Bhd",
            registration_code="1358366-A",
            address="Shah Alam",
            contact_numbers=["+60123121979"],
            website_href="/redir/381082?u=www.securepay.my",
            website_url="https://www.securepay.my",
            contact_email="hello@securepay.my",
            company_manager="AMIR HARIS AHMAD",
            employees=[{"name": "AMIR DIRECTOR", "role": "DIRECTOR", "phone": "+60136527979"}],
        )


class _FakeSnovClient:
    def get_domain_emails_count(self, domain: str) -> int:
        assert domain == "securepay.my"
        return 2

    def get_domain_emails(self, domain: str) -> list[str]:
        assert domain == "securepay.my"
        return ["hello@securepay.my", "support@securepay.my"]


class _FakeManagerAgent:
    def __init__(self) -> None:
        self.calls = 0

    def enrich_manager(
        self,
        *,
        company_name: str,
        domain: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> ManagerAgentResult:
        self.calls += 1
        assert company_name
        assert domain == "securepay.my"
        return ManagerAgentResult(
            success=True,
            manager_name="LLM Manager",
            manager_role="Managing Director",
            evidence_url="https://www.securepay.my/about",
            evidence_quote="LLM Manager / Managing Director",
            candidate_pool=candidate_pool,
            tried_urls=tried_urls,
            error_code="",
            error_text="",
            retry_after=0.0,
        )


class _FakeFlakyManagerAgent:
    def __init__(self) -> None:
        self.calls = 0

    def enrich_manager(
        self,
        *,
        company_name: str,
        domain: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> ManagerAgentResult:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("firecrawl_429")
        return ManagerAgentResult(
            success=True,
            manager_name="Recovered Manager",
            manager_role="Manager",
            evidence_url="https://www.securepay.my/team",
            evidence_quote="Recovered Manager / Manager",
            candidate_pool=candidate_pool,
            tried_urls=tried_urls,
            error_code="",
            error_text="",
            retry_after=0.0,
        )


def test_pipeline_joins_ctos_businesslist_and_snov(tmp_path: Path) -> None:
    pipeline = CtosBusinessListSnovPipeline(
        ctos_crawler=_FakeCTOSCrawler(),
        businesslist_crawler=_FakeBusinessListCrawler(),
        snov_client=_FakeSnovClient(),
    )
    stats = pipeline.run(
        output_dir=tmp_path,
        target_companies=1,
        ctos_prefixes="s",
        ctos_max_pages_per_prefix=1,
        businesslist_start_id=381082,
        businesslist_end_id=381082,
    )
    assert stats["matched_companies"] == 1
    assert stats["snov_enriched"] == 1
    row = json.loads((tmp_path / "ctos_businesslist_snov.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["company_name"] == "Securepay Sdn Bhd"
    assert row["domain"] == "securepay.my"
    assert row["company_manager"] == "AMIR HARIS AHMAD"
    assert row["contact_eamils"] == "[\"hello@securepay.my\", \"support@securepay.my\"]"


class _NoManagerBusinessListCrawler(_FakeBusinessListCrawler):
    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None:
        profile = super().fetch_company_profile(company_id)
        if profile is None:
            return None
        profile.company_manager = ""
        return profile


def test_pipeline_fallback_to_manager_agent_when_manager_missing(tmp_path: Path) -> None:
    manager_agent = _FakeManagerAgent()
    pipeline = CtosBusinessListSnovPipeline(
        ctos_crawler=_FakeCTOSCrawler(),
        businesslist_crawler=_NoManagerBusinessListCrawler(),
        snov_client=_FakeSnovClient(),
        manager_agent=manager_agent,
        manager_enrich_max_rounds=2,
    )
    stats = pipeline.run(
        output_dir=tmp_path,
        target_companies=1,
        ctos_prefixes="s",
        ctos_max_pages_per_prefix=1,
        businesslist_start_id=381082,
        businesslist_end_id=381082,
    )
    row = json.loads((tmp_path / "ctos_businesslist_snov.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["company_manager"] == "LLM Manager"
    assert manager_agent.calls == 1
    assert stats["manager_from_firecrawl_llm"] == 1


def test_pipeline_manager_agent_429_will_retry_without_crash(tmp_path: Path) -> None:
    manager_agent = _FakeFlakyManagerAgent()
    pipeline = CtosBusinessListSnovPipeline(
        ctos_crawler=_FakeCTOSCrawler(),
        businesslist_crawler=_NoManagerBusinessListCrawler(),
        snov_client=_FakeSnovClient(),
        manager_agent=manager_agent,
        manager_enrich_max_rounds=2,
    )
    stats = pipeline.run(
        output_dir=tmp_path,
        target_companies=1,
        ctos_prefixes="s",
        ctos_max_pages_per_prefix=1,
        businesslist_start_id=381082,
        businesslist_end_id=381082,
    )
    row = json.loads((tmp_path / "ctos_businesslist_snov.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["company_manager"] == "Recovered Manager"
    assert manager_agent.calls == 2
    assert stats["manager_from_firecrawl_llm"] == 1
