from __future__ import annotations

import re
import threading
from dataclasses import replace

import pytest

from site_agent.config import PipelineSettings
from site_agent.models import PageContent, SiteInput
from site_agent import pipeline as pipeline_mod
from web_agent import service as web_service
from web_agent import runner as web_runner


class _DummyCrawler:
    def __init__(self, response: dict | None) -> None:
        self._response = response
        self.urls: list[str] | None = None
        self.prompt: str | None = None
        self.schema: dict | None = None

    async def extract_fields(self, urls: list[str], *, prompt: str, schema: dict) -> dict | None:
        self.urls = urls
        self.prompt = prompt
        self.schema = schema
        return self._response


def _settings(tmp_path) -> PipelineSettings:
    return PipelineSettings(
        input_path=tmp_path / "input.csv",
        output_base_dir=tmp_path,
        run_dir=tmp_path / "run",
        concurrency=1,
        llm_concurrency=1,
        max_pages=5,
        max_rounds=1,
        max_sites=None,
        page_timeout=30,
        max_content_chars=2000,
        save_pages=False,
        resume=False,
        llm_api_key="",
        llm_base_url="",
        llm_model="",
        llm_temperature=0.0,
        llm_max_output_tokens=512,
        llm_reasoning_effort=None,
    )


def test_stamp_log_text_preserves_existing_timestamp():
    line = "2026-01-28 17:56:00 [官网] 处理站点：http://example.com\n"
    stamped = web_service._stamp_log_text(line)
    assert stamped == line


def test_stamp_log_text_adds_timestamp():
    line = "[官网] 处理站点：http://example.com\n"
    stamped = web_service._stamp_log_text(line)
    assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ", stamped)
    assert "[官网] 处理站点：http://example.com" in stamped


def test_job_log_concurrent_write_utf8(tmp_path):
    log_path = tmp_path / "job.log"
    payload = "处理站点：東京テスト株式会社"

    def worker() -> None:
        for _ in range(200):
            web_service._append_job_log(log_path, payload)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    data = log_path.read_bytes()
    text = data.decode("utf-8-sig")
    assert "�" not in text
    assert text.count(payload) >= 900


def test_job_log_trim_keeps_last_lines_utf8(tmp_path, monkeypatch):
    log_path = tmp_path / "job.log"
    monkeypatch.setattr(web_runner, "_JOB_LOG_MAX_LINES", 50, raising=False)
    monkeypatch.setattr(web_runner, "_JOB_LOG_TRIM_INTERVAL", 0.0, raising=False)

    for i in range(120):
        web_service._append_job_log(log_path, f"处理站点：東京{i}")

    text = log_path.read_text(encoding="utf-8-sig")
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) <= 50
    assert "�" not in text


@pytest.mark.asyncio
async def test_firecrawl_no_urls_logs(tmp_path):
    logs: list[str] = []
    token = pipeline_mod.set_log_sink(logs.append)
    try:
        site = SiteInput(website="")
        crawler = _DummyCrawler({})
        result = await pipeline_mod._extract_with_firecrawl(
            site, crawler, {}, _settings(tmp_path), {}
        )
    finally:
        pipeline_mod.reset_log_sink(token)
    assert result == {"error": "firecrawl_no_urls"}
    assert any("Firecrawl 未找到可用页面" in line for line in logs)


@pytest.mark.asyncio
async def test_firecrawl_error_logs(tmp_path):
    logs: list[str] = []
    token = pipeline_mod.set_log_sink(logs.append)
    try:
        site = SiteInput(website="https://example.com")
        visited = {
            site.website: PageContent(url=site.website, markdown="", title="home")
        }
        crawler = _DummyCrawler({"error": "firecrawl_429"})
        result = await pipeline_mod._extract_with_firecrawl(
            site, crawler, visited, _settings(tmp_path), {}
        )
    finally:
        pipeline_mod.reset_log_sink(token)
    assert result == {"error": "firecrawl_429"}
    assert any("Firecrawl 提取失败：firecrawl_429" in line for line in logs)


@pytest.mark.asyncio
async def test_firecrawl_parse_error_logs(tmp_path):
    logs: list[str] = []
    token = pipeline_mod.set_log_sink(logs.append)
    try:
        site = SiteInput(website="https://example.com")
        visited = {
            site.website: PageContent(url=site.website, markdown="", title="home")
        }
        crawler = _DummyCrawler({})
        result = await pipeline_mod._extract_with_firecrawl(
            site, crawler, visited, _settings(tmp_path), {}
        )
    finally:
        pipeline_mod.reset_log_sink(token)
    assert result == {"error": "firecrawl_extract_failed"}
    assert any("Firecrawl 返回结构异常" in line for line in logs)


@pytest.mark.asyncio
async def test_firecrawl_success_logs_and_evidence(tmp_path):
    logs: list[str] = []
    token = pipeline_mod.set_log_sink(logs.append)
    try:
        site = SiteInput(website="https://example.com")
        visited = {
            site.website: PageContent(url=site.website, markdown="", title="home")
        }
        crawler = _DummyCrawler(
            {"data": {"representative": "山田太郎", "phone": "03-1234-5678"}}
        )
        info = await pipeline_mod._extract_with_firecrawl(
            site, crawler, visited, _settings(tmp_path), {}
        )
        pipeline_mod._log_extracted_info(site.website, info)
    finally:
        pipeline_mod.reset_log_sink(token)

    assert info is not None
    evidence = info.get("evidence") if isinstance(info, dict) else None
    assert evidence["representative"]["source"] == "firecrawl"
    assert evidence["phone"]["source"] == "firecrawl"
    assert any("Firecrawl 提取代表人/座机" in line for line in logs)
    assert any("Firecrawl 找到代表人" in line for line in logs)
