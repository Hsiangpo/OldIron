from __future__ import annotations

import sys
from pathlib import Path
from concurrent.futures import Future

from openpyxl import Workbook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.importer import load_websites
from oldironcrawler.config import AppConfig
from oldironcrawler.importer import ImportedWebsite
from oldironcrawler.extractor.representative_search import (
    ActiveRepresentativeSearchResult,
    ActiveRepresentativeSearcher,
)
from oldironcrawler.extractor.llm_client import LlmExtractionResult
from oldironcrawler.extractor.service import DiscoverySnapshot, SiteProfileService
from oldironcrawler.reporter import print_site_result, write_delivery_csv
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore, SiteResult


def test_xlsx_loader_keeps_company_name_with_website(tmp_path: Path) -> None:
    path = tmp_path / "companies.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(["Company Name", "Company Website"])
    ws.append(["Acme Holdings Ltd", "https://acme.example"])
    ws.append(["Beta Group", "beta.example"])
    wb.save(path)

    rows = load_websites(path)

    assert [(row.company_name, row.website) for row in rows] == [
        ("Acme Holdings Ltd", "https://acme.example"),
        ("Beta Group", "https://beta.example"),
    ]


def test_xlsx_loader_keeps_chinese_company_name_with_website(tmp_path: Path) -> None:
    path = tmp_path / "companies_cn.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(["邮箱地址", "全名", "公司名称", "公司网址", "法人"])
    ws.append(["owner@example.com", "Owner", "Hidrodomi", "http://hidrodomi.com", "Guilherme"])
    ws.append(["lead@example.com", "Lead", "TAMEK", "http://tamek.com.tr", "Fevzi"])
    wb.save(path)

    rows = load_websites(path)

    assert [(row.company_name, row.website) for row in rows] == [
        ("Hidrodomi", "http://hidrodomi.com"),
        ("TAMEK", "http://tamek.com.tr"),
    ]


def test_txt_loader_has_empty_company_name(tmp_path: Path) -> None:
    path = tmp_path / "sites.txt"
    path.write_text("example.com\n", encoding="utf-8")

    rows = load_websites(path)

    assert rows[0].company_name == ""
    assert rows[0].website == "https://example.com"


def test_config_loads_tavily_search_settings(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://llm.example/v1",
                "LLM_KEY=test-key",
                "LLM_MODEL=test-model",
                "TAVILY_API_KEY=tvly-test",
                "SEARCH_REPRESENTATIVE_ENABLED=true",
                "SEARCH_REPRESENTATIVE_CONCURRENCY=3",
                "TAVILY_MAX_RESULTS=7",
            ]
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(tmp_path)

    assert config.tavily_api_key == "tvly-test"
    assert config.search_representative_enabled is True
    assert config.search_representative_concurrency == 3
    assert config.tavily_max_results == 7


def test_store_round_trips_searched_representative(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    store.prepare_job(
        input_name="companies.xlsx",
        fingerprint="abc",
        rows=[
            ImportedWebsite(
                input_index=1,
                raw_website="https://acme.example",
                website="https://acme.example",
                dedupe_key="acme.example",
                company_name="Acme Holdings Ltd",
            )
        ],
    )
    task = store.claim_next_site()
    assert task is not None
    assert task.company_name == "Acme Holdings Ltd"

    store.mark_done(
        task.id,
        SiteResult(
            company_name="Acme Holdings Ltd",
            representative="Alice Website",
            emails="info@acme.example",
            searched_representative="Alice Search",
            website="https://acme.example",
            phones="",
        ),
    )

    assert store.delivery_rows() == [
        {
            "company_name": "Acme Holdings Ltd",
            "representative": "Alice Website",
            "emails": "info@acme.example",
            "searched_representative": "Alice Search",
            "phones": "",
            "website": "https://acme.example",
        }
    ]


class _FakeSearchLlm:
    def build_active_representative_queries(self, *, company_name, website, deadline_monotonic=None):
        assert company_name == "Acme Holdings Ltd"
        assert website == "https://acme.example"
        return ["Acme Holdings Ltd current CEO official"]

    def extract_active_representative_from_search_results(
        self,
        *,
        company_name,
        website,
        results,
        deadline_monotonic=None,
    ):
        assert company_name == "Acme Holdings Ltd"
        assert website == "https://acme.example"
        assert results[0]["url"] == "https://acme.example/about"
        return {
            "representative": "Alice Search",
            "confidence": "high",
            "evidence_url": "https://acme.example/about",
        }


class _FakeTavilyClient:
    def __init__(self):
        self.queries = []

    def search(self, query: str) -> list[dict[str, str]]:
        self.queries.append(query)
        return [
            {
                "title": "Leadership",
                "url": "https://acme.example/about",
                "content": "Alice Search is the current Chief Executive Officer of Acme Holdings Ltd.",
            }
        ]


def test_active_representative_searcher_returns_name() -> None:
    tavily = _FakeTavilyClient()
    searcher = ActiveRepresentativeSearcher(
        llm_client=_FakeSearchLlm(),
        tavily_client=tavily,
        enabled=True,
        max_queries=2,
    )

    result = searcher.search(
        company_name="Acme Holdings Ltd",
        website="https://acme.example",
    )

    assert result == ActiveRepresentativeSearchResult(
        representative="Alice Search",
        confidence="high",
        evidence_url="https://acme.example/about",
    )
    assert tavily.queries == ["Acme Holdings Ltd current CEO official"]


def test_active_representative_searcher_skips_missing_company_name() -> None:
    searcher = ActiveRepresentativeSearcher(
        llm_client=_FakeSearchLlm(),
        tavily_client=_FakeTavilyClient(),
        enabled=True,
        max_queries=2,
    )

    result = searcher.search(company_name="", website="https://acme.example")

    assert result.representative == ""


class _FakeActiveSearcher:
    def __init__(self):
        self.calls = []

    def submit(self, *, company_name: str, website: str, deadline_monotonic=None):
        self.calls.append((company_name, website))
        future = Future()
        future.set_result(
            ActiveRepresentativeSearchResult(
                representative="Alice Search",
                confidence="high",
                evidence_url="https://acme.example/about",
            )
        )
        return future


class _FailingActiveSearcher:
    def __init__(self):
        self.calls = []

    def submit(self, *, company_name: str, website: str, deadline_monotonic=None):
        self.calls.append((company_name, website))
        future = Future()
        future.set_exception(RuntimeError("tavily_unavailable"))
        return future


def _patch_service_for_fast_profile(monkeypatch, service: SiteProfileService) -> None:
    monkeypatch.setattr(
        "oldironcrawler.extractor.service._discover_value_snapshot",
        lambda *args, **kwargs: DiscoverySnapshot(
            urls=[],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[],
        ),
    )
    monkeypatch.setattr(
        "oldironcrawler.extractor.service._plan_fetch_targets",
        lambda *args, **kwargs: {
            "rep_urls": [],
            "email_primary_urls": [],
            "email_overflow_urls": [],
            "all_primary_urls": [],
        },
    )
    monkeypatch.setattr(
        service,
        "_collect_budgeted_pages",
        lambda *args, **kwargs: (
            {},
            [],
            LlmExtractionResult(
                company_name="Acme Holdings Ltd",
                representative="Alice Website",
                evidence_url="https://acme.example/team",
                evidence_quote="Alice Website CEO",
            ),
        ),
    )
    monkeypatch.setattr("oldironcrawler.extractor.service._collect_email_rule_pages", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "oldironcrawler.extractor.service._collect_contact_details",
        lambda *args, **kwargs: ([], {}, [], {}),
    )


def _build_profile_service(tmp_path: Path, searcher) -> SiteProfileService:
    return SiteProfileService(
        AppConfig.load(tmp_path, llm_key_override="key"),
        RuntimeStore(tmp_path / "runtime.sqlite3"),
        GlobalLearningStore(tmp_path / "learning.sqlite3"),
        llm_client=object(),
        page_pool=object(),
        representative_searcher=searcher,
    )


def test_site_profile_service_adds_searched_representative(monkeypatch, tmp_path: Path) -> None:
    searcher = _FakeActiveSearcher()
    service = _build_profile_service(tmp_path, searcher)
    _patch_service_for_fast_profile(monkeypatch, service)

    result = service.process(
        1,
        "https://acme.example",
        input_company_name="Acme Holdings Ltd",
    )

    assert result.result.representative == "Alice Website"
    assert result.result.searched_representative == "Alice Search"
    assert result.result.searched_representative_evidence_url == "https://acme.example/about"
    assert searcher.calls == [("Acme Holdings Ltd", "https://acme.example")]


def test_site_profile_service_ignores_search_failure(monkeypatch, tmp_path: Path) -> None:
    searcher = _FailingActiveSearcher()
    service = _build_profile_service(tmp_path, searcher)
    _patch_service_for_fast_profile(monkeypatch, service)

    result = service.process(
        1,
        "https://acme.example",
        input_company_name="Acme Holdings Ltd",
    )

    assert result.result.company_name == "Acme Holdings Ltd"
    assert result.result.representative == "Alice Website"
    assert result.result.searched_representative == ""
    assert searcher.calls == [("Acme Holdings Ltd", "https://acme.example")]


def test_site_profile_service_searches_with_extracted_company_name(monkeypatch, tmp_path: Path) -> None:
    searcher = _FakeActiveSearcher()
    service = _build_profile_service(tmp_path, searcher)
    _patch_service_for_fast_profile(monkeypatch, service)

    result = service.process(1, "https://acme.example", input_company_name="")

    assert result.result.searched_representative == "Alice Search"
    assert searcher.calls == [("Acme Holdings Ltd", "https://acme.example")]


def test_reporter_prints_searched_representative(capsys) -> None:
    print_site_result(
        completed_index=1,
        total=1,
        website="https://acme.example",
        company_name="Acme Holdings Ltd",
        representative="Alice Website",
        emails="info@acme.example",
        searched_representative="Alice Search",
        phones="",
    )

    out = capsys.readouterr().out
    assert "公司名: Acme Holdings Ltd" in out
    assert "姓名: Alice Website" in out
    assert "邮箱: info@acme.example" in out
    assert "搜索现役最大代表人: Alice Search" in out


def test_delivery_csv_writes_searched_representative(tmp_path: Path) -> None:
    path = tmp_path / "delivery.csv"

    write_delivery_csv(
        path,
        [
            {
                "company_name": "Acme Holdings Ltd",
                "representative": "Alice Website",
                "emails": "info@acme.example",
                "searched_representative": "Alice Search",
                "phones": "",
                "website": "https://acme.example",
            }
        ],
    )

    text = path.read_text(encoding="utf-8-sig")
    assert text.splitlines()[0] == "company_name,representative,emails,searched_representative,phones,website"
    assert "Alice Search" in text
