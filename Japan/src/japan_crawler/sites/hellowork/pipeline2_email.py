"""hellowork Pipeline 2 — 官网规则补邮箱 + LLM 补代表人。

对 Pipeline 1 中有 website 的企业，用协议爬虫抓取官网 HTML，
先用规则提取公开邮箱，再在缺代表人时用 LLM 补代表人。
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oldiron_core.fc_email.email_service import (
    FirecrawlEmailService,
    FirecrawlEmailSettings,
)
from .store import HelloworkStore

logger = logging.getLogger("hellowork.pipeline2")

DEFAULT_CONCURRENCY = 64
DEFAULT_BATCH_SIZE = 512


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 2: 官网规则补邮箱 + LLM 补代表人。"""
    store = HelloworkStore(output_dir / "hellowork_store.db")

    pending = store.get_email_pending(limit=max_items)
    if not pending:
        logger.info("没有需要邮箱提取的企业")
        return {"processed": 0, "found": 0}

    logger.info("官网规则邮箱提取：待处理 %d 家, 并发=%d", len(pending), concurrency)

    settings = _build_settings(output_dir)
    settings.validate()

    from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig
    crawler = SiteCrawlClient(SiteCrawlConfig(
        proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
        timeout_seconds=20.0,
    ))

    processed = 0
    found = 0
    lock = threading.Lock()

    batch_size = _resolve_batch_size(concurrency)
    local = threading.local()
    created_services: list[FirecrawlEmailService] = []
    service_lock = threading.Lock()

    def _get_service() -> FirecrawlEmailService:
        svc = getattr(local, "service", None)
        if svc is None:
            svc = FirecrawlEmailService(settings, firecrawl_client=crawler)
            local.service = svc
            with service_lock:
                created_services.append(svc)
        return svc

    def _worker(company: dict) -> tuple[int, list[str], str]:
        """处理一家企业，返回 (id, emails, representative)。"""
        cid = company["id"]
        name = company["company_name"]
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
            return cid, emails, rep
        except Exception as exc:
            logger.debug("邮箱提取失败: %s — %s", name, exc)
            return cid, [], ""

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for batch_index, batch in enumerate(_iter_batches(pending, batch_size), start=1):
                logger.info("Email 批次 %d：大小 %d", batch_index, len(batch))
                futures = {executor.submit(_worker, c): c for c in batch}
                for future in as_completed(futures):
                    company = futures[future]
                    try:
                        cid, emails, rep = future.result()
                        email_str = ",".join(emails) if emails else ""
                        with lock:
                            processed += 1
                            store.save_email_result(cid, email_str, rep)
                            if emails:
                                found += 1
                            if processed <= 5 or processed % 20 == 0:
                                logger.info(
                                    "[Email %d/%d] %.1f%% %s → %s",
                                    processed, len(pending),
                                    processed / len(pending) * 100,
                                    company["company_name"][:30],
                                    ", ".join(emails[:2]) if emails else "-",
                                )
                    except Exception as exc:
                        with lock:
                            processed += 1
                        logger.warning("邮箱工作线程异常: %s", exc)
    except KeyboardInterrupt:
        logger.info("邮箱提取用户中断")
    finally:
        for svc in created_services:
            svc.close()

    logger.info("Pipeline 2 完成：处理 %d 家, 找到邮箱 %d 家", processed, found)
    return {"processed": processed, "found": found}


def _resolve_batch_size(concurrency: int) -> int:
    return max(int(concurrency or 1) * 4, DEFAULT_BATCH_SIZE)


def _iter_batches(items: list[dict], batch_size: int):
    size = max(int(batch_size or 1), 1)
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _build_settings(output_dir: Path) -> FirecrawlEmailSettings:
    """从环境变量构建 LLM 设置。"""
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
