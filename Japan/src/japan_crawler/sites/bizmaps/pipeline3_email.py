"""bizmaps Pipeline 3 — 官网规则补邮箱 + LLM 补代表人。

对 Pipeline 1/2 中有 website 的公司，用协议爬虫抓取官网 HTML，
先用规则提取公开邮箱，再在缺代表人时用 LLM 补代表人。
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oldiron_core.fc_email.email_service import (
    DEFAULT_LLM_API_STYLE,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_REASONING_EFFORT,
    FirecrawlEmailService,
    FirecrawlEmailSettings,
)
from oldiron_core.protocol_crawler import SiteCrawlClient
from oldiron_core.protocol_crawler import SiteCrawlConfig
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline3")

DEFAULT_CONCURRENCY = 64
DEFAULT_BATCH_SIZE = 64


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 3: 官网规则补邮箱 + LLM 补代表人。

    从 SQLite 中读取有 website 但缺邮箱的公司，逐个抓取官网并提取邮箱。
    """
    store = BizmapsStore(output_dir / "bizmaps_store.db")

    batch_limit = _email_batch_limit(max_items, concurrency)
    pending = store.get_email_pending(batch_limit)
    if not pending:
        logger.info("没有需要邮箱提取的公司")
        return {"processed": 0, "found": 0}

    logger.info("官网规则邮箱提取：待处理 %d 家, 并发=%d, 批量=%d", len(pending), concurrency, batch_limit)

    settings = _build_settings(output_dir)
    settings.validate()

    processed = 0
    found = 0
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
        with cleanup_lock:
            resources.append((service, crawler))
        return service

    def _worker(company: dict) -> tuple[str, str, str, list[str], str]:
        name = company["company_name"]
        addr = company.get("address", "")
        website = company["website"]
        svc = _get_service()
        try:
            result = svc.discover_emails(
                company_name=name,
                homepage=website,
                existing_representative=company.get("representative", ""),
            )
            emails = [e.strip().lower() for e in result.emails if e.strip()]
            rep = result.representative or ""
            return name, addr, website, emails, rep
        except Exception as exc:
            logger.debug("邮箱提取失败: %s — %s", name, exc)
            return name, addr, website, [], ""

    try:
        with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
            futures = {executor.submit(_worker, item): item for item in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    name, addr, website, emails, rep = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("邮箱提取失败：%s | %s", company["company_name"], exc)
                    name = company["company_name"]
                    addr = company.get("address", "")
                    website = company["website"]
                    emails = []
                    rep = ""
                store.save_email_result(name, addr, website, emails, rep)
                processed += 1
                if emails:
                    found += 1
                if processed <= 5 or processed % 20 == 0:
                    logger.info(
                        "[Email %d/%d] %.1f%% %s → %s",
                        processed, len(pending),
                        processed / len(pending) * 100,
                        name[:30],
                        ", ".join(emails[:2]) if emails else "-",
                    )
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

    logger.info("Pipeline 3 完成：处理 %d 家, 找到邮箱 %d 家", processed, found)
    return {"processed": processed, "found": found}


def _email_batch_limit(max_items: int, concurrency: int) -> int:
    if max_items > 0:
        return max_items
    configured = int(os.getenv("BIZMAPS_EMAIL_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)) or DEFAULT_BATCH_SIZE)
    return min(max(int(concurrency or 1), 1), max(configured, 1))


def _build_settings(output_dir: Path) -> FirecrawlEmailSettings:
    """从环境变量构建 LLM 设置。"""
    return FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",  # 用协议爬虫，不用 Firecrawl API
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL),
        llm_model=os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT),
        llm_api_style=os.getenv("LLM_API_STYLE", DEFAULT_LLM_API_STYLE),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "12")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "5")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "5")),
    )
