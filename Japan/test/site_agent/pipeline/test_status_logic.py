from __future__ import annotations

import asyncio
from pathlib import Path

from site_agent.config import PipelineSettings
from site_agent.models import SiteInput
from site_agent.pipeline import _status_from_fields
from site_agent.pipeline.heuristics import _sanitize_info
from site_agent.pipeline.process.site import _process_site


def test_status_required_fields_email_rep() -> None:
    required = ["company_name", "email", "representative"]
    assert (
        _status_from_fields("Company", "Rep", "a@b.com", None, required_fields=required)
        == "ok"
    )
    assert (
        _status_from_fields("Company", "", "a@b.com", None, required_fields=required)
        == "partial"
    )
    assert (
        _status_from_fields("Company", "Rep", "", None, required_fields=required)
        == "partial"
    )
    assert (
        _status_from_fields("", "", "", None, required_fields=required) == "failed"
    )


def test_status_required_fields_with_phone() -> None:
    required = ["company_name", "email", "representative", "phone"]
    assert (
        _status_from_fields("Company", "Rep", "a@b.com", "010-1234", required_fields=required)
        == "ok"
    )
    assert (
        _status_from_fields("Company", "Rep", "a@b.com", None, required_fields=required)
        == "partial"
    )


def test_status_simple_mode_company_and_phone() -> None:
    required = ["company_name", "phone"]
    assert _status_from_fields("Company", None, None, "03-1234-5678", required_fields=required) == "ok"
    assert _status_from_fields("Company", None, None, None, required_fields=required) == "partial"
    assert _status_from_fields(None, None, None, "03-1234-5678", required_fields=required) == "partial"


def test_sanitize_info_keeps_plus81_phone() -> None:
    info = {
        "phone": "+81 6-6341-5340",
        "evidence": {"phone": {"url": "https://example.com", "quote": "+81 6-6341-5340"}},
    }
    cleaned = _sanitize_info(info)
    assert cleaned.get("phone") == "+81 6-6341-5340"


def test_process_site_simple_mode_without_crawl(tmp_path: Path) -> None:
    class _NoCrawlClient:
        async def fetch_page(self, _url: str) -> None:
            raise AssertionError("simple 模式不应触发官网抓取")

    settings = PipelineSettings(
        input_path=tmp_path / "input.jsonl",
        output_base_dir=tmp_path,
        run_dir=tmp_path / "run",
        concurrency=1,
        llm_concurrency=1,
        max_pages=1,
        max_rounds=1,
        max_sites=None,
        page_timeout=1000,
        max_content_chars=1000,
        save_pages=False,
        resume=False,
        llm_api_key="",
        llm_base_url="https://example.com/v1",
        llm_model="gpt-5.1-codex-mini",
        llm_temperature=0.0,
        llm_max_output_tokens=128,
        llm_reasoning_effort=None,
        use_llm=False,
        required_fields=["company_name", "phone"],
        simple_mode=True,
    )
    site = SiteInput(
        website="https://example.co.jp",
        input_name="株式会社テスト",
        raw={"phone": "+81 6-6341-5340"},
    )
    result = asyncio.run(
        _process_site(
            site,
            _NoCrawlClient(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            settings,
            tmp_path / "pages",
        )
    )
    assert result.status == "ok"
    assert result.company_name == "株式会社テスト"
    assert result.phone == "+81 6-6341-5340"


def test_process_site_simple_mode_phone_enrich_from_cid(tmp_path: Path) -> None:
    class _NoCrawlClient:
        async def fetch_page(self, _url: str) -> None:
            raise AssertionError("simple 模式不应触发官网抓取")

    class _PhoneResolver:
        def resolve_from_raw(self, raw: dict[str, str] | None) -> str | None:
            return "03-1234-5678" if isinstance(raw, dict) and raw.get("cid") else None

    settings = PipelineSettings(
        input_path=tmp_path / "input.jsonl",
        output_base_dir=tmp_path,
        run_dir=tmp_path / "run",
        concurrency=1,
        llm_concurrency=1,
        max_pages=1,
        max_rounds=1,
        max_sites=None,
        page_timeout=1000,
        max_content_chars=1000,
        save_pages=False,
        resume=False,
        llm_api_key="",
        llm_base_url="https://example.com/v1",
        llm_model="gpt-5.1-codex-mini",
        llm_temperature=0.0,
        llm_max_output_tokens=128,
        llm_reasoning_effort=None,
        use_llm=False,
        required_fields=["company_name", "phone"],
        simple_mode=True,
    )
    site = SiteInput(
        website="https://example.co.jp",
        input_name="株式会社テスト",
        raw={"cid": "0x1:0x2"},
    )
    result = asyncio.run(
        _process_site(
            site,
            _NoCrawlClient(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            settings,
            tmp_path / "pages",
            simple_phone_resolver=_PhoneResolver(),  # type: ignore[arg-type]
        )
    )
    assert result.status == "ok"
    assert result.phone == "03-1234-5678"
