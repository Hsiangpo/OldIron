from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from site_agent.config import PipelineSettings
from site_agent.input_loader import load_sites
from site_agent.pipeline import run_pipeline


def _find_tokyo_input() -> Path | None:
    base = Path("output") / "web_jobs"
    if not base.exists():
        return None
    candidates: list[Path] = []
    for path in base.rglob("enrich.website_map.jsonl"):
        parts = {p.name for p in path.parents}
        if any("东京都" in name for name in parts):
            candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _find_tokyo_output_jsonl() -> Path | None:
    base = Path("output") / "web_jobs"
    if not base.exists():
        return None
    candidates: list[Path] = []
    for path in base.rglob("site/output.jsonl"):
        parts = {p.name for p in path.parents}
        if any("东京都" in name for name in parts):
            candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _build_settings(tmp_path: Path, input_path: Path, max_sites: int) -> PipelineSettings:
    firecrawl_keys_path = Path(
        os.environ.get("FIRECRAWL_KEYS_PATH") or "output/firecrawl_keys.txt"
    )
    return PipelineSettings(
        input_path=input_path,
        output_base_dir=tmp_path,
        run_dir=tmp_path / "tokyo_sample",
        concurrency=int(os.environ.get("TOKYO_SAMPLE_CONCURRENCY") or 5),
        llm_concurrency=int(os.environ.get("TOKYO_SAMPLE_LLM_CONCURRENCY") or 1),
        max_pages=int(os.environ.get("TOKYO_SAMPLE_MAX_PAGES") or 5),
        max_rounds=int(os.environ.get("TOKYO_SAMPLE_MAX_ROUNDS") or 2),
        max_sites=max_sites,
        page_timeout=int(os.environ.get("TOKYO_SAMPLE_PAGE_TIMEOUT") or 20000),
        max_content_chars=int(os.environ.get("TOKYO_SAMPLE_MAX_CONTENT") or 20000),
        save_pages=False,
        resume=False,
        llm_api_key=os.environ.get("LLM_API_KEY") or "dummy",
        llm_base_url=os.environ.get("LLM_BASE_URL") or "",
        llm_model=os.environ.get("LLM_MODEL") or "",
        llm_temperature=float(os.environ.get("LLM_TEMPERATURE") or 0.0),
        llm_max_output_tokens=int(os.environ.get("LLM_MAX_OUTPUT_TOKENS") or 1200),
        llm_reasoning_effort=os.environ.get("LLM_REASONING_EFFORT"),
        crawler_reset_every=0,
        site_timeout_seconds=None,
        snov_extension_selector=os.environ.get("SNOV_EXTENSION_SELECTOR"),
        snov_extension_token=os.environ.get("SNOV_EXTENSION_TOKEN"),
        snov_extension_fingerprint=os.environ.get("SNOV_EXTENSION_FINGERPRINT"),
        snov_extension_cdp_host=os.environ.get("SNOV_EXTENSION_CDP_HOST"),
        snov_extension_cdp_port=int(os.environ.get("SNOV_EXTENSION_CDP_PORT"))
        if os.environ.get("SNOV_EXTENSION_CDP_PORT")
        else None,
        snov_extension_only=(
            os.environ.get("SNOV_EXTENSION_ONLY", "true").strip().lower()
            in {"1", "true", "yes"}
        ),
        skip_email=False,
        country_code=None,
        required_fields=["company_name", "email", "representative"],
        keyword=None,
        keyword_filter_enabled=False,
        keyword_min_confidence=0.6,
        email_max_per_domain=0,
        email_details_limit=80,
        pdf_max_pages=4,
        resume_mode=None,
        firecrawl_keys_path=firecrawl_keys_path if firecrawl_keys_path.exists() else None,
        firecrawl_base_url=os.environ.get("FIRECRAWL_BASE_URL"),
        firecrawl_extract_enabled=bool(
            os.environ.get("FIRECRAWL_EXTRACT_ENABLED", "").strip().lower()
            in {"1", "true", "yes"}
        ),
        firecrawl_extract_max_urls=int(os.environ.get("FIRECRAWL_EXTRACT_MAX_URLS") or 6),
        firecrawl_key_per_limit=int(os.environ.get("FIRECRAWL_KEY_PER_LIMIT") or 2),
        firecrawl_key_wait_seconds=int(os.environ.get("FIRECRAWL_KEY_WAIT_SECONDS") or 120),
    )


@pytest.mark.integration
def test_tokyo_sample_100(tmp_path: Path) -> None:
    if os.environ.get("RUN_TOKYO_SAMPLE") not in {"1", "true", "yes"}:
        pytest.skip("Set RUN_TOKYO_SAMPLE=1 to run live Tokyo sample.")

    input_path = _find_tokyo_input()
    if not input_path:
        pytest.skip("Tokyo input not found under output/web_jobs.")

    sites = load_sites(input_path)
    if len(sites) < 100:
        pytest.skip(f"Tokyo input only has {len(sites)} sites (<100).")

    mode = (os.environ.get("TOKYO_SAMPLE_MODE") or "cache").strip().lower()
    records: list[dict] = []

    if mode == "live":
        settings = _build_settings(tmp_path, input_path, max_sites=100)
        if settings.firecrawl_keys_path is None:
            pytest.skip("Firecrawl keys file not found; set FIRECRAWL_KEYS_PATH.")
        asyncio.run(run_pipeline(settings))
        output_jsonl = settings.run_dir / "output.jsonl"
        for line in output_jsonl.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            records.append(json.loads(line))
        summary_path = settings.run_dir / "summary.json"
    else:
        output_path = _find_tokyo_output_jsonl()
        if not output_path:
            pytest.skip("Tokyo site output.jsonl not found under output/web_jobs.")
        for line in output_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            records.append(json.loads(line))
        if len(records) < 100:
            pytest.skip(f"Tokyo site output only has {len(records)} records (<100).")
        records = records[:100]
        summary_path = tmp_path / "tokyo_sample_summary.json"

    total = len(records)
    ok = [r for r in records if r.get("status") == "ok"]
    partial = [r for r in records if r.get("status") == "partial"]
    failed = [r for r in records if r.get("status") == "failed"]

    missing_rep_text = "\u672a\u627e\u5230\u4ee3\u8868\u4eba"

    def has_value(value: object) -> bool:
        return isinstance(value, str) and value.strip() != ""

    def has_rep(value: object) -> bool:
        return has_value(value) and value != missing_rep_text

    ok_email_rep = [
        r
        for r in ok
        if has_value(r.get("email")) and has_rep(r.get("representative"))
    ]
    ok_email_only = [
        r
        for r in ok
        if has_value(r.get("email")) and not has_rep(r.get("representative"))
    ]

    summary = {
        "total": total,
        "ok": len(ok),
        "ok_email_rep": len(ok_email_rep),
        "ok_email_only": len(ok_email_only),
        "partial": len(partial),
        "failed": len(failed),
        "input_path": str(input_path),
        "mode": mode,
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    assert total == 100
    assert len(ok) + len(partial) + len(failed) == total
