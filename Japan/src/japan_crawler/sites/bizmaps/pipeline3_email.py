"""bizmaps Pipeline 3 — Protocol+LLM 官网邮箱提取。

对 Pipeline 1/2 中有 website 的公司，用协议爬虫抓取官网 HTML，
然后通过 LLM 提取公开联系人邮箱。
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
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline3")

DEFAULT_CONCURRENCY = 64
DEFAULT_COMMIT_INTERVAL = 20
DEFAULT_BATCH_SIZE = 512


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 3: Protocol+LLM 官网邮箱提取。

    从 SQLite 中读取有 website 但没有 email 的公司，逐个抓取官网并提取邮箱。
    """
    store = BizmapsStore(output_dir / "bizmaps_store.db")

    # 筛选需要处理的公司 — 有 website 但还没处理过邮箱的
    all_companies = _load_email_pending(store)
    if max_items > 0:
        all_companies = all_companies[:max_items]
    if not all_companies:
        logger.info("没有需要邮箱提取的公司")
        return {"processed": 0, "found": 0}

    logger.info("Protocol+LLM 邮箱提取：待处理 %d 家, 并发=%d", len(all_companies), concurrency)

    # 构建 LLM + 协议爬虫客户端
    settings = _build_settings(output_dir)
    settings.validate()

    # 使用协议爬虫客户端而不是 Firecrawl API
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
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for batch_index, batch in enumerate(_iter_batches(all_companies, batch_size), start=1):
                logger.info("Email 批次 %d：大小 %d", batch_index, len(batch))
                futures = {executor.submit(_worker, c): c for c in batch}
                for future in as_completed(futures):
                    company = futures[future]
                    try:
                        name, addr, website, emails, rep = future.result()
                        with lock:
                            processed += 1
                            store.save_email_result(name, addr, website, emails, rep)
                            if emails:
                                found += 1
                            if processed <= 5 or processed % 20 == 0:
                                logger.info(
                                    "[Email %d/%d] %.1f%% %s → %s",
                                    processed, len(all_companies),
                                    processed / len(all_companies) * 100,
                                    name[:30],
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

    logger.info("Pipeline 3 完成：处理 %d 家, 找到邮箱 %d 家", processed, found)
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
        crawl_backend="protocol",  # 用协议爬虫，不用 Firecrawl API
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


def _load_email_pending(store: BizmapsStore) -> list[dict]:
    """加载需要邮箱提取的公司列表。"""
    return store.get_email_pending()
