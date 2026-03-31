"""hellowork Pipeline 2 — Protocol+LLM 官网邮箱提取。

对 Pipeline 1 中有 website 的企业，用协议爬虫抓取官网 HTML，
然后通过 LLM 提取公开联系人邮箱。
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from japan_crawler.fc_email.email_service import (
    FirecrawlEmailService,
    FirecrawlEmailSettings,
)
from .store import HelloworkStore

logger = logging.getLogger("hellowork.pipeline2")

# 个人邮箱域名过滤
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "yahoo.co.jp", "hotmail.com",
    "outlook.com", "outlook.jp", "icloud.com", "live.com",
    "live.jp", "msn.com", "me.com", "aol.com",
    "docomo.ne.jp", "softbank.ne.jp", "ezweb.ne.jp",
    "au.com", "i.softbank.jp", "ymobile.ne.jp",
    "nifty.com", "ocn.ne.jp", "plala.or.jp", "biglobe.ne.jp",
    "so-net.ne.jp", "dion.ne.jp", "infoweb.ne.jp",
    "gol.com", "jcom.home.ne.jp", "ybb.ne.jp",
}

DEFAULT_CONCURRENCY = 64


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 2: Protocol+LLM 官网邮箱提取。"""
    store = HelloworkStore(output_dir / "hellowork_store.db")

    pending = store.get_email_pending(limit=max_items)
    if not pending:
        logger.info("没有需要邮箱提取的企业")
        return {"processed": 0, "found": 0}

    logger.info("Protocol+LLM 邮箱提取：待处理 %d 家, 并发=%d", len(pending), concurrency)

    settings = _build_settings(output_dir)
    settings.validate()

    from shared.oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig
    crawler = SiteCrawlClient(SiteCrawlConfig(
        proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
        timeout_seconds=20.0,
    ))

    processed = 0
    found = 0
    lock = threading.Lock()

    def _worker(company: dict) -> tuple[int, list[str], str]:
        """处理一家企业，返回 (id, emails, representative)。"""
        cid = company["id"]
        name = company["company_name"]
        website = company["website"]

        svc = FirecrawlEmailService(settings, firecrawl_client=crawler)
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
            futures = {executor.submit(_worker, c): c for c in pending}
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

    logger.info("Pipeline 2 完成：处理 %d 家, 找到邮箱 %d 家", processed, found)
    return {"processed": processed, "found": found}


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
