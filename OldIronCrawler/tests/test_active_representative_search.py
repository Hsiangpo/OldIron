from __future__ import annotations

import sys
from pathlib import Path

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
