# Active Representative Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a lightweight Tavily-backed AI search path to OldIronCrawler that finds the current highest active representative for each input company and writes it to terminal output and CSV.

**Architecture:** Keep the existing official-website extraction flow intact. Add a bounded two-pass tool loop: LLM proposes search queries from `company_name + website`, code executes Tavily search, then LLM extracts one active top representative name from the search results or returns empty. Run this search in parallel with the existing website extraction when the input row already contains a company name.

**Tech Stack:** Python 3.10+, existing `httpx` and `openai` clients, Tavily Search API via direct HTTP, existing SQLite runtime store, existing pytest suite.

---

## Confirmed Requirements

- Scope is only `OldIronCrawler`.
- Input records should support `company_name + website`.
- If `company_name` is missing from the input record, do not run Tavily search for that row in the first version.
- This is not a chat agent and not an interactive agent.
- The implementation is a batch run loop across many companies.
- For each company, run the normal website extraction and the new active-representative search in parallel when possible.
- The search output accepts one person name only.
- If there is no clear current highest representative, store an empty string.
- Terminal output must include:
  - `公司名`
  - `姓名`
  - `邮箱`
  - `搜索现役最大代表人`
- CSV output must include a new column for the searched representative.

## Reference Patterns

- Tavily Search API: `POST /search`, direct API or SDK, `max_results`, `include_raw_content`, and async examples are documented at `https://docs.tavily.com/documentation/api-reference/endpoint/search`.
- Tavily best practices recommend short search-agent style queries and concurrent async searches for batches: `https://docs.tavily.com/documentation/best-practices/best-practices-search`.
- LangGraph `tools_condition` is the mature ReAct-style reference pattern: LLM step, check whether a tool call is needed, tool execution, return to LLM, or end. Use the pattern concept only, not the dependency: `https://reference.langchain.com/python/langgraph.prebuilt/tool_node/tools_condition`.
- gptme is a mature terminal-agent reference with local tools and web tools, but OldIronCrawler should not import it or copy its interactive UX: `https://github.com/gptme/gptme`.

## File Map

- Modify `OldIronCrawler/src/oldironcrawler/importer.py`
  - Preserve an optional input company name alongside the normalized website.
  - Detect company-name columns in CSV/XLSX.
- Modify `OldIronCrawler/src/oldironcrawler/config.py`
  - Add Tavily and search concurrency settings.
- Modify `OldIronCrawler/.env.example`
  - Document new Tavily/search settings without secrets.
- Modify `OldIronCrawler/src/oldironcrawler/runtime/store.py`
  - Add input company-name and searched-representative fields.
  - Add SQLite migrations for old runtime DBs.
- Modify `OldIronCrawler/src/oldironcrawler/extractor/llm_client.py`
  - Add two small public LLM helpers for search-query generation and final representative extraction.
  - Keep the file under 1000 lines.
- Create `OldIronCrawler/src/oldironcrawler/extractor/representative_search.py`
  - Own Tavily HTTP calls and the bounded two-pass agent loop.
  - Keep every function under 200 lines.
- Modify `OldIronCrawler/src/oldironcrawler/extractor/service.py`
  - Start active-representative search early and join the result before returning `SiteResult`.
- Modify `OldIronCrawler/src/oldironcrawler/runner.py`
  - Instantiate and close the shared searcher.
  - Pass input company name to `SiteProfileService`.
- Modify `OldIronCrawler/src/oldironcrawler/reporter.py`
  - Print and write the new column.
- Add tests in `OldIronCrawler/tests/test_active_representative_search.py`
  - Keep new tests isolated instead of growing `test_core.py`, which is already large.

## CSV Column Decision

Use this output order:

```csv
company_name,representative,emails,searched_representative,phones,website
```

Reason: it preserves the existing leading fields, places the new result beside the core extracted values, and mirrors the terminal display.

## Task 1: Preserve Company Name From Input Files

**Files:**
- Modify: `OldIronCrawler/src/oldironcrawler/importer.py`
- Test: `OldIronCrawler/tests/test_active_representative_search.py`

- [ ] **Step 1: Write failing importer tests**

Create `OldIronCrawler/tests/test_active_representative_search.py` with:

```python
from __future__ import annotations

import sys
from pathlib import Path

from openpyxl import Workbook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.importer import load_websites


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
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py -q
```

Expected: fail because `ImportedWebsite.company_name` does not exist.

- [ ] **Step 3: Extend `ImportedWebsite`**

In `OldIronCrawler/src/oldironcrawler/importer.py`, change the dataclass to:

```python
@dataclass
class ImportedWebsite:
    input_index: int
    raw_website: str
    website: str
    dedupe_key: str
    company_name: str = ""
```

- [ ] **Step 4: Add structured-row support**

Keep the existing website-detection behavior. Add a small internal row model and company-column detection:

```python
@dataclass
class _ImportedRawRow:
    raw_website: str
    company_name: str = ""


_COMPANY_HEADER_HINTS = {
    "company",
    "company name",
    "company_name",
    "name",
    "business name",
    "organization",
    "organisation",
    "会社名",
    "企業名",
    "公司名",
}


def _looks_like_company_header(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in _COMPANY_HEADER_HINTS
```

Then adjust `_load_from_txt`, `_load_from_csv`, and `_load_from_xlsx` so they return `list[_ImportedRawRow]` instead of `list[str]`. For TXT, use `_ImportedRawRow(raw_website=line, company_name="")`.

For CSV/XLSX:

- Keep the selected website column logic exactly as it is.
- Find the first header whose normalized text matches `_looks_like_company_header`.
- If no company column exists, use empty company names.
- Do not treat email/phone/address columns as company-name columns.

Finally update `_dedupe_websites(rows)` to read both fields:

```python
def _dedupe_websites(rows: list[_ImportedRawRow]) -> list[ImportedWebsite]:
    results: list[ImportedWebsite] = []
    seen: set[str] = set()
    for index, row in enumerate(rows, start=1):
        website = _normalize_website(row.raw_website)
        if not website:
            continue
        dedupe_key = _build_dedupe_key(website)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        results.append(
            ImportedWebsite(
                input_index=index,
                raw_website=str(row.raw_website).strip(),
                website=website,
                dedupe_key=dedupe_key,
                company_name=str(row.company_name or "").strip(),
            )
        )
    return results
```

- [ ] **Step 5: Run importer tests**

Run:

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py -q
python3 -m pytest tests/test_core.py::test_xlsx_loader_prefers_llm_selected_company_website_column -q
python3 -m pytest tests/test_core.py::test_txt_loader_dedupes_by_full_website_then_domain -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add OldIronCrawler/src/oldironcrawler/importer.py OldIronCrawler/tests/test_active_representative_search.py
git commit -m "feat: preserve input company names in OldIronCrawler"
```

## Task 2: Add Config And Runtime Store Fields

**Files:**
- Modify: `OldIronCrawler/src/oldironcrawler/config.py`
- Modify: `OldIronCrawler/.env.example`
- Modify: `OldIronCrawler/src/oldironcrawler/runtime/store.py`
- Test: `OldIronCrawler/tests/test_active_representative_search.py`

- [ ] **Step 1: Write failing store/config tests**

Append to `OldIronCrawler/tests/test_active_representative_search.py`:

```python
from oldironcrawler.config import AppConfig
from oldironcrawler.importer import ImportedWebsite
from oldironcrawler.runtime.store import RuntimeStore, SiteResult


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

    rows = store.delivery_rows()
    assert rows == [
        {
            "company_name": "Acme Holdings Ltd",
            "representative": "Alice Website",
            "emails": "info@acme.example",
            "searched_representative": "Alice Search",
            "phones": "",
            "website": "https://acme.example",
        }
    ]
```

- [ ] **Step 2: Run tests and verify failure**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_config_loads_tavily_search_settings -q
python3 -m pytest tests/test_active_representative_search.py::test_store_round_trips_searched_representative -q
```

Expected: fail because config/store fields do not exist.

- [ ] **Step 3: Add config fields**

In `AppConfig`, add:

```python
    tavily_api_key: str
    search_representative_enabled: bool
    search_representative_concurrency: int
    tavily_max_results: int
    tavily_search_depth: str
    tavily_timeout_seconds: float
```

Add a boolean parser:

```python
def _config_bool(values: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _config_str(values, name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}
```

In `AppConfig.load`, set:

```python
            tavily_api_key=_config_str(values, "TAVILY_API_KEY"),
            search_representative_enabled=_config_bool(values, "SEARCH_REPRESENTATIVE_ENABLED", True),
            search_representative_concurrency=max(_config_int(values, "SEARCH_REPRESENTATIVE_CONCURRENCY", 8), 1),
            tavily_max_results=min(max(_config_int(values, "TAVILY_MAX_RESULTS", 5), 1), 10),
            tavily_search_depth=_config_str(values, "TAVILY_SEARCH_DEPTH", "basic"),
            tavily_timeout_seconds=max(_config_float(values, "TAVILY_TIMEOUT_SECONDS", 20.0), 3.0),
```

Do not require `TAVILY_API_KEY` in `validate()`. Missing Tavily key should disable search and leave the searched field empty, not stop the website crawler.

- [ ] **Step 4: Update `.env.example`**

Append:

```env
TAVILY_API_KEY=
SEARCH_REPRESENTATIVE_ENABLED=true
SEARCH_REPRESENTATIVE_CONCURRENCY=8
TAVILY_MAX_RESULTS=5
TAVILY_SEARCH_DEPTH=basic
TAVILY_TIMEOUT_SECONDS=20
```

- [ ] **Step 5: Add store fields and migrations**

In `SiteTask`, add:

```python
    company_name: str = ""
```

In `SiteResult`, add:

```python
    searched_representative: str = ""
    searched_representative_evidence_url: str = ""
    searched_representative_confidence: str = ""
```

In `SiteStageMetrics`, add:

```python
    search_rep_ms: int = 0
```

In the `sites` table schema, add:

```sql
input_company_name TEXT NOT NULL DEFAULT '',
searched_representative TEXT NOT NULL DEFAULT '',
searched_representative_evidence_url TEXT NOT NULL DEFAULT '',
searched_representative_confidence TEXT NOT NULL DEFAULT '',
search_rep_ms INTEGER NOT NULL DEFAULT 0,
```

Add a migration method and call it from `_init_db()`:

```python
def _ensure_site_search_columns(self, conn: sqlite3.Connection) -> None:
    existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(sites)").fetchall()}
    additions = {
        "input_company_name": "ALTER TABLE sites ADD COLUMN input_company_name TEXT NOT NULL DEFAULT ''",
        "searched_representative": "ALTER TABLE sites ADD COLUMN searched_representative TEXT NOT NULL DEFAULT ''",
        "searched_representative_evidence_url": (
            "ALTER TABLE sites ADD COLUMN searched_representative_evidence_url TEXT NOT NULL DEFAULT ''"
        ),
        "searched_representative_confidence": (
            "ALTER TABLE sites ADD COLUMN searched_representative_confidence TEXT NOT NULL DEFAULT ''"
        ),
        "search_rep_ms": "ALTER TABLE sites ADD COLUMN search_rep_ms INTEGER NOT NULL DEFAULT 0",
    }
    for name, sql in additions.items():
        if name not in existing:
            conn.execute(sql)
```

Update:

- `prepare_job()` to insert `input_company_name`.
- `reset_running_tasks()` and `reset_completed_job_for_rerun()` to clear searched-representative fields and `search_rep_ms`.
- `claim_next_site()` to select `input_company_name` and return it as `SiteTask.company_name`.
- `mark_done()` to store searched-representative fields.
- `_METRIC_COLUMNS` to include `search_rep_ms`.
- `delivery_rows()` to select and return `searched_representative`.

- [ ] **Step 6: Run tests**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py -q
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add OldIronCrawler/src/oldironcrawler/config.py OldIronCrawler/.env.example OldIronCrawler/src/oldironcrawler/runtime/store.py OldIronCrawler/tests/test_active_representative_search.py
git commit -m "feat: add active representative runtime fields"
```

## Task 3: Add Tavily Search Agent Module

**Files:**
- Create: `OldIronCrawler/src/oldironcrawler/extractor/representative_search.py`
- Modify: `OldIronCrawler/src/oldironcrawler/extractor/llm_client.py`
- Test: `OldIronCrawler/tests/test_active_representative_search.py`

- [ ] **Step 1: Write failing unit tests for the bounded loop**

Append:

```python
from oldironcrawler.extractor.representative_search import (
    ActiveRepresentativeSearchResult,
    ActiveRepresentativeSearcher,
)


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
```

- [ ] **Step 2: Run tests and verify failure**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_active_representative_searcher_returns_name -q
```

Expected: fail because `representative_search.py` does not exist.

- [ ] **Step 3: Create the new search module**

Create `OldIronCrawler/src/oldironcrawler/extractor/representative_search.py`:

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

import httpx


@dataclass(frozen=True)
class ActiveRepresentativeSearchResult:
    representative: str = ""
    confidence: str = ""
    evidence_url: str = ""


class ActiveRepresentativeLlm(Protocol):
    def build_active_representative_queries(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> list[str]:
        ...

    def extract_active_representative_from_search_results(
        self,
        *,
        company_name: str,
        website: str,
        results: list[dict[str, str]],
        deadline_monotonic: float | None = None,
    ) -> dict[str, str]:
        ...


class TavilySearchClient:
    def __init__(
        self,
        *,
        api_key: str,
        max_results: int,
        search_depth: str,
        timeout_seconds: float,
        proxy_url: str = "",
    ) -> None:
        self._api_key = str(api_key or "").strip()
        self._max_results = max(int(max_results or 5), 1)
        self._search_depth = str(search_depth or "basic").strip() or "basic"
        self._client = httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            proxy=proxy_url or None,
            trust_env=False,
        )

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def close(self) -> None:
        self._client.close()

    def search(self, query: str) -> list[dict[str, str]]:
        if not self._api_key:
            return []
        response = self._client.post(
            "https://api.tavily.com/search",
            json={
                "api_key": self._api_key,
                "query": query,
                "search_depth": self._search_depth,
                "max_results": self._max_results,
                "include_answer": False,
                "include_raw_content": False,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return [_normalize_tavily_result(item) for item in payload.get("results", [])]


class ActiveRepresentativeSearcher:
    def __init__(
        self,
        *,
        llm_client: ActiveRepresentativeLlm,
        tavily_client,
        enabled: bool,
        max_queries: int = 2,
    ) -> None:
        self._llm = llm_client
        self._tavily = tavily_client
        self._enabled = bool(enabled)
        self._max_queries = max(int(max_queries or 1), 1)

    def close(self) -> None:
        close = getattr(self._tavily, "close", None)
        if callable(close):
            close()

    def search(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> ActiveRepresentativeSearchResult:
        company = str(company_name or "").strip()
        if not self._enabled or not company:
            return ActiveRepresentativeSearchResult()
        if _deadline_expired(deadline_monotonic):
            return ActiveRepresentativeSearchResult()
        queries = self._llm.build_active_representative_queries(
            company_name=company,
            website=website,
            deadline_monotonic=deadline_monotonic,
        )
        results = self._run_queries(queries[: self._max_queries])
        if not results or _deadline_expired(deadline_monotonic):
            return ActiveRepresentativeSearchResult()
        data = self._llm.extract_active_representative_from_search_results(
            company_name=company,
            website=website,
            results=results,
            deadline_monotonic=deadline_monotonic,
        )
        return ActiveRepresentativeSearchResult(
            representative=str(data.get("representative", "") or "").strip(),
            confidence=str(data.get("confidence", "") or "").strip(),
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
        )

    def _run_queries(self, queries: list[str]) -> list[dict[str, str]]:
        merged: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for query in queries:
            text = str(query or "").strip()
            if not text:
                continue
            for result in self._tavily.search(text):
                url = str(result.get("url", "") or "").strip()
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                merged.append(result)
        return merged


def _normalize_tavily_result(item: object) -> dict[str, str]:
    if not isinstance(item, dict):
        return {"title": "", "url": "", "content": ""}
    return {
        "title": str(item.get("title", "") or "").strip(),
        "url": str(item.get("url", "") or "").strip(),
        "content": str(item.get("content", "") or "").strip(),
    }


def _deadline_expired(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic
```

- [ ] **Step 4: Add LLM helper methods**

In `OldIronCrawler/src/oldironcrawler/extractor/llm_client.py`, add methods on `WebsiteLlmClient`:

```python
    def build_active_representative_queries(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> list[str]:
        prompt = (
            "你是企业负责人搜索查询生成器。\n"
            "目标：根据公司名和官网，为搜索引擎生成最多2条短查询，用来查找该公司的现役最高负责人。\n"
            "规则：\n"
            "1. 查询必须包含公司名。\n"
            "2. 可以包含官网域名，帮助排除同名公司。\n"
            "3. 优先查询 current CEO, president, managing director, representative director, owner。\n"
            "4. 不要生成解释文字。\n"
            '返回 JSON：{"queries":[""]}\n\n'
            f"公司名: {company_name}\n"
            f"官网: {website}"
        )
        data = self._call_json(prompt, deadline_monotonic=deadline_monotonic)
        queries = data.get("queries", [])
        if not isinstance(queries, list):
            return []
        return [str(item or "").strip() for item in queries if str(item or "").strip()][:2]

    def extract_active_representative_from_search_results(
        self,
        *,
        company_name: str,
        website: str,
        results: list[dict[str, str]],
        deadline_monotonic: float | None = None,
    ) -> dict[str, str]:
        prompt = (
            "你是企业现役最高负责人判断器。\n"
            "目标：只根据搜索结果，找出目标公司的现役最高负责人单人。\n"
            "强规则：\n"
            "1. 只返回真人姓名，不能返回职位、部门、公司名或团队名。\n"
            "2. 必须是现役或当前负责人；历史负责人、创始但已卸任、新闻采访对象不能用。\n"
            "3. 优先级：CEO > President > Managing Director > Representative Director > Chief Executive > Owner > Founder-CEO。\n"
            "4. 必须确认搜索结果指向同一家公司；官网域名或公司名明显不匹配时返回空。\n"
            "5. 没有明确证据时 representative 返回空字符串，不要猜。\n"
            '返回 JSON：{"representative":"","confidence":"high|medium|low|","evidence_url":""}\n\n'
            f"目标公司名: {company_name}\n"
            f"目标官网: {website}\n"
            f"搜索结果(JSON): {json.dumps(results[:10], ensure_ascii=False)}"
        )
        data = self._call_json(prompt, deadline_monotonic=deadline_monotonic)
        return {
            "representative": _normalize_representative_name(str(data.get("representative", "") or "").strip()),
            "confidence": str(data.get("confidence", "") or "").strip(),
            "evidence_url": str(data.get("evidence_url", "") or "").strip(),
        }
```

Keep these methods short. Do not add a generic chat-agent framework.

- [ ] **Step 5: Run tests**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_active_representative_searcher_returns_name -q
python3 -m pytest tests/test_active_representative_search.py::test_active_representative_searcher_skips_missing_company_name -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add OldIronCrawler/src/oldironcrawler/extractor/representative_search.py OldIronCrawler/src/oldironcrawler/extractor/llm_client.py OldIronCrawler/tests/test_active_representative_search.py
git commit -m "feat: add Tavily active representative searcher"
```

## Task 4: Run Search In Parallel With Website Extraction

**Files:**
- Modify: `OldIronCrawler/src/oldironcrawler/extractor/service.py`
- Modify: `OldIronCrawler/src/oldironcrawler/runner.py`
- Test: `OldIronCrawler/tests/test_active_representative_search.py`

- [ ] **Step 1: Write failing service test**

Append:

```python
from dataclasses import dataclass
from oldironcrawler.extractor.llm_client import LlmExtractionResult
from oldironcrawler.extractor.service import SiteProcessingResult


@dataclass
class _FakeSearchResult:
    representative: str = "Alice Search"
    confidence: str = "high"
    evidence_url: str = "https://acme.example/about"


class _FakeActiveSearcher:
    def __init__(self):
        self.calls = []

    def search(self, *, company_name: str, website: str, deadline_monotonic=None):
        self.calls.append((company_name, website))
        return _FakeSearchResult()


def test_site_profile_service_adds_searched_representative(monkeypatch, tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    learning = GlobalLearningStore(tmp_path / "learning.sqlite3")
    config = AppConfig.load(tmp_path, llm_key_override="key")
    config.llm_base_url = "https://llm.example/v1"
    config.llm_model = "test-model"
    searcher = _FakeActiveSearcher()

    service = SiteProfileService(
        config,
        store,
        learning,
        llm_client=object(),
        page_pool=object(),
        representative_searcher=searcher,
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
    monkeypatch.setattr("oldironcrawler.extractor.service._discover_value_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(service, "_resolve_representative_urls", lambda *args, **kwargs: [])
    monkeypatch.setattr("oldironcrawler.extractor.service._plan_fetch_targets", lambda *args, **kwargs: {
        "rep_urls": [],
        "email_primary_urls": [],
        "email_overflow_urls": [],
        "all_primary_urls": [],
    })
    monkeypatch.setattr("oldironcrawler.extractor.service._collect_email_rule_pages", lambda *args, **kwargs: [])
    monkeypatch.setattr("oldironcrawler.extractor.service._collect_contact_details", lambda *args, **kwargs: ([], {}, [], {}))

    result = service.process(
        1,
        "https://acme.example",
        input_company_name="Acme Holdings Ltd",
    )

    assert result.result.representative == "Alice Website"
    assert result.result.searched_representative == "Alice Search"
    assert searcher.calls == [("Acme Holdings Ltd", "https://acme.example")]
```

If this monkeypatch setup is too brittle after inspecting `service.py`, keep the same assertion but use a smaller helper test around the new private method that starts and joins the search future.

- [ ] **Step 2: Run test and verify failure**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_site_profile_service_adds_searched_representative -q
```

Expected: fail because `SiteProfileService` does not accept `representative_searcher`.

- [ ] **Step 3: Modify `SiteProfileService` constructor and process signature**

In `service.py`, accept:

```python
        representative_searcher=None,
```

Store it as `self._representative_searcher`.

Change `process` signature to:

```python
    def process(
        self,
        site_id: int,
        website: str,
        *,
        input_company_name: str = "",
        deadline_monotonic: float | None = None,
    ) -> SiteProcessingResult:
```

- [ ] **Step 4: Add bounded search helper methods**

Add private helpers in `service.py`:

```python
def _run_active_representative_search(searcher, company_name: str, website: str, deadline_monotonic: float | None):
    if searcher is None:
        return None
    try:
        return searcher.search(
            company_name=company_name,
            website=website,
            deadline_monotonic=deadline_monotonic,
        )
    except Exception:  # noqa: BLE001
        return None
```

Use Chinese comments only if a comment is needed. This helper intentionally swallows search failures because Tavily failure must not drop the website crawl.

- [ ] **Step 5: Start search before website crawling work**

Inside `process`, before `_discover_value_snapshot`, submit the search if `self._representative_searcher` exists and `input_company_name` is not empty. Prefer the searcher's own executor if implemented in Task 4 runner step; otherwise call directly in a `Future`.

The expected behavior:

- Search begins from `input_company_name + website`.
- Normal website extraction continues immediately.
- Before returning `SiteResult`, wait for the search result up to the remaining site deadline.
- If the search result is missing, errored, or timed out, set `searched_representative=""`.
- Record elapsed time in `metrics.search_rep_ms`.

- [ ] **Step 6: Wire runner**

In `runner.py`:

- Import `ActiveRepresentativeSearcher` and `TavilySearchClient`.
- Instantiate once in `run_crawl_session()` after `WebsiteLlmClient`.
- Pass it to `_run_single_site()`.
- Pass `task.company_name` to `service.process()`.
- Close it in `finally`.

Use config:

```python
tavily_client = TavilySearchClient(
    api_key=config.tavily_api_key,
    max_results=config.tavily_max_results,
    search_depth=config.tavily_search_depth,
    timeout_seconds=config.tavily_timeout_seconds,
    proxy_url=config.proxy_url,
)
representative_searcher = ActiveRepresentativeSearcher(
    llm_client=llm_client,
    tavily_client=tavily_client,
    enabled=config.search_representative_enabled and tavily_client.is_configured,
    max_queries=2,
)
```

Do not fail the crawler when `TAVILY_API_KEY` is empty.

- [ ] **Step 7: Run tests**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py -q
python3 -m pytest tests/test_value_budgeting.py -q
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add OldIronCrawler/src/oldironcrawler/extractor/service.py OldIronCrawler/src/oldironcrawler/runner.py OldIronCrawler/tests/test_active_representative_search.py
git commit -m "feat: run active representative search with site crawl"
```

## Task 5: Add Terminal And CSV Output

**Files:**
- Modify: `OldIronCrawler/src/oldironcrawler/reporter.py`
- Modify: `OldIronCrawler/src/oldironcrawler/runner.py`
- Modify: `OldIronCrawler/src/oldironcrawler/runtime/store.py`
- Test: `OldIronCrawler/tests/test_active_representative_search.py`

- [ ] **Step 1: Write failing reporter test**

Append:

```python
from oldironcrawler.reporter import print_site_result, write_delivery_csv


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
```

- [ ] **Step 2: Run test and verify failure**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_reporter_prints_searched_representative -q
python3 -m pytest tests/test_active_representative_search.py::test_delivery_csv_writes_searched_representative -q
```

Expected: fail because reporter functions do not accept/write the field.

- [ ] **Step 3: Update terminal printing**

In `print_site_result`, add parameter:

```python
    searched_representative: str = "",
```

Print it after email:

```python
    print(f"  搜索现役最大代表人: {_display(searched_representative)}", flush=True)
```

In `runner.py`, update the call inside `_handle_future()` so it passes `result.result.searched_representative` for completed sites and empty string for dropped sites.

- [ ] **Step 4: Update CSV writer**

In `write_delivery_csv`, change the fieldnames to:

```python
fieldnames=[
    "company_name",
    "representative",
    "emails",
    "searched_representative",
    "phones",
    "website",
]
```

Ensure `RuntimeStore.delivery_rows()` returns that key for every row.

- [ ] **Step 5: Run reporter tests**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py::test_reporter_prints_searched_representative -q
python3 -m pytest tests/test_active_representative_search.py::test_delivery_csv_writes_searched_representative -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add OldIronCrawler/src/oldironcrawler/reporter.py OldIronCrawler/src/oldironcrawler/runner.py OldIronCrawler/src/oldironcrawler/runtime/store.py OldIronCrawler/tests/test_active_representative_search.py
git commit -m "feat: output searched representative"
```

## Task 6: Documentation And Validation

**Files:**
- Modify: `OldIronCrawler/DOCS/PRD.MD`
- Modify: `OldIronCrawler/DOCS/IMPLEMENTATION_PLAN.MD`
- Test: existing suite

- [ ] **Step 1: Update PRD**

In `OldIronCrawler/DOCS/PRD.MD`, update the product scope and delivery policy:

- Add that structured inputs may include company names.
- Add `searched_representative` as an output field.
- State Tavily search is only used for active representative lookup and not for emails or phones.
- State missing searched representative is stored as an empty string and displayed as `未找到`.

- [ ] **Step 2: Update implementation plan doc**

In `OldIronCrawler/DOCS/IMPLEMENTATION_PLAN.MD`, add a short section:

```markdown
### Active Representative Search

- Preserve optional `company_name` from CSV/XLSX inputs.
- When company name is present, run a Tavily-backed AI search loop in parallel with website extraction.
- Store and deliver `searched_representative`.
- Tavily failure must not fail the website crawl.
```

- [ ] **Step 3: Run targeted unit tests**

```bash
cd OldIronCrawler
python3 -m pytest tests/test_active_representative_search.py -q
python3 -m pytest tests/test_value_budgeting.py -q
python3 -m pytest tests/test_llm_error_handling.py -q
```

Expected: all pass.

- [ ] **Step 4: Run full OldIronCrawler suite**

```bash
cd OldIronCrawler
python3 -m pytest tests -q
```

Expected: pass.

- [ ] **Step 5: Run one real validation**

This requires local `.env` with a real `LLM_KEY` and `TAVILY_API_KEY`.

Create a local-only smoke input under `OldIronCrawler/tmp/active_rep_smoke/companies.csv`:

```csv
Company Name,Company Website
Microsoft Corporation,https://www.microsoft.com
Toyota Motor Corporation,https://global.toyota
```

Copy it into `OldIronCrawler/websites/` only if the interactive runner requires selecting from the websites directory. After the run, remove the smoke file from tracked directories and keep no temp artifacts.

Run:

```bash
cd OldIronCrawler
python3 run.py
```

Expected manual checks:

- Terminal shows `搜索现役最大代表人`.
- CSV contains `searched_representative`.
- At least one of the smoke rows has a non-empty searched representative.
- If Tavily or LLM returns no confident result, the field is empty and the crawler still completes.

- [ ] **Step 6: Clean temp artifacts**

Remove only local smoke artifacts:

```bash
rm -rf OldIronCrawler/tmp/active_rep_smoke
```

Do not delete runtime DBs or delivery outputs unless the user explicitly approves.

- [ ] **Step 7: Commit docs and final changes**

```bash
git add OldIronCrawler/DOCS/PRD.MD OldIronCrawler/DOCS/IMPLEMENTATION_PLAN.MD
git commit -m "docs: document active representative search"
```

## Final Verification Gate

Before reporting completion, run:

```bash
git status --short
cd OldIronCrawler && python3 -m pytest tests -q
```

If a real `.env` with Tavily and LLM keys is available, also run the smoke validation in Task 6. If keys are unavailable, report that real validation is blocked by missing local secrets and do not claim live validation passed.

## Implementation Notes

- Keep `llm_client.py` and `service.py` below 1000 lines. If either approaches the limit, move helper logic into the new `representative_search.py`.
- Keep every backend function below 200 lines.
- Do not add `_v2`, `_old`, or commented-out alternate implementations.
- Do not use Tavily for emails or phones.
- Do not make the Tavily search a delivery gate.
- Do not fail a company just because Tavily fails.
- Do not hardcode API keys in code, docs, or tests.
- Use `TAVILY_API_KEY` only from local `.env` or process environment.
- Code comments, if needed, must be Chinese.
- After verified code changes, commit and push to `main` per repository rules.
