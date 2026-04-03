"""OpenWork Pipeline 3 — 官网补邮箱与法人。"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oldiron_core.fc_email.email_service import FirecrawlEmailService, FirecrawlEmailSettings
from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig

from .store import OpenworkStore


LOGGER = logging.getLogger("openwork.pipeline3")
_DEFAULT_BATCH_SIZE = 24


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 128,
) -> dict[str, int]:
    """对有官网的公司补邮箱与法人。"""
    store = OpenworkStore(output_dir / "openwork_store.db")
    batch_limit = _email_batch_limit(max_items, concurrency)
    pending = store.get_email_pending(batch_limit)
    if not pending:
        LOGGER.info("没有需要提取邮箱的公司")
        return {"processed": 0, "found": 0}

    settings = _build_settings(output_dir)
    settings.validate()
    LOGGER.info("OpenWork 邮箱提取：待处理 %d 家，并发=%d，批量=%d", len(pending), concurrency, batch_limit)

    thread_local = threading.local()
    cleanup_lock = threading.Lock()
    resources: list[tuple[FirecrawlEmailService, SiteCrawlClient]] = []

    def _get_service() -> FirecrawlEmailService:
        service = getattr(thread_local, "service", None)
        if service is not None:
            return service
        crawler = SiteCrawlClient(
            SiteCrawlConfig(
                proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
                timeout_seconds=20.0,
            )
        )
        service = FirecrawlEmailService(settings, firecrawl_client=crawler)
        thread_local.service = service
        thread_local.crawler = crawler
        with cleanup_lock:
            resources.append((service, crawler))
        return service

    def _worker(company: dict[str, str]) -> tuple[str, list[str], str]:
        service = _get_service()
        result = service.discover_emails(
            company_name=company["company_name"],
            homepage=company["website"],
            existing_representative=company.get("representative", ""),
        )
        return company["company_id"], list(result.emails or []), str(result.representative or "").strip()

    processed = 0
    found = 0
    try:
        with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
            futures = {executor.submit(_worker, item): item for item in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    company_id, emails, representative = future.result()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("邮箱提取失败：%s | %s", company["company_name"], exc)
                    company_id, emails, representative = company["company_id"], [], ""
                store.save_email_result(company_id, emails, representative)
                processed += 1
                if emails:
                    found += 1
    finally:
        for service, crawler in resources:
            try:
                service.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                crawler.close()
            except Exception:  # noqa: BLE001
                pass
    return {"processed": processed, "found": found}


def _email_batch_limit(max_items: int, concurrency: int) -> int:
    if max_items > 0:
        return max_items
    configured = int(os.getenv("OPENWORK_EMAIL_BATCH_SIZE", str(_DEFAULT_BATCH_SIZE)) or _DEFAULT_BATCH_SIZE)
    return min(max(int(concurrency or 1), 1), max(configured, 1))


def _build_settings(output_dir: Path) -> FirecrawlEmailSettings:
    return FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
        llm_model=os.getenv("LLM_MODEL", "gpt-5.4-mini"),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", "medium"),
        llm_api_style=os.getenv("LLM_API_STYLE", "auto"),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "12")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "5")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "5")),
    )
