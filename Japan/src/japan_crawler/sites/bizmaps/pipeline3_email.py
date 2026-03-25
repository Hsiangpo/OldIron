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

from japan_crawler.fc_email.email_service import (
    FirecrawlEmailService,
    FirecrawlEmailSettings,
)
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline3")

# 个人邮箱域名过滤（交付时过滤，这里也提前踢掉明显的）
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.co.jp", "yahoo.com", "hotmail.com",
    "outlook.com", "icloud.com", "live.com", "msn.com",
    "me.com", "aol.com", "nifty.com", "docomo.ne.jp",
    "softbank.ne.jp", "ezweb.ne.jp", "au.com",
}

DEFAULT_CONCURRENCY = 32
DEFAULT_COMMIT_INTERVAL = 20


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

    # 确保 emails 列存在
    _ensure_email_columns(store)

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
    from shared.oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig
    crawler = SiteCrawlClient(SiteCrawlConfig(
        proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
        timeout_seconds=20.0,
    ))

    processed = 0
    found = 0
    lock = threading.Lock()

    def _worker(company: dict) -> tuple[str, str, list[str], str]:
        """处理一家公司，返回 (company_name, address, emails, representative)。"""
        name = company["company_name"]
        addr = company.get("address", "")
        website = company["website"]

        # 每个线程独立构建 service（共享 crawler 和 settings）
        svc = FirecrawlEmailService(settings, firecrawl_client=crawler)
        try:
            result = svc.discover_emails(
                company_name=name,
                homepage=website,
            )
            emails = _filter_emails(result.emails)
            rep = result.representative or ""
            return name, addr, emails, rep
        except Exception as exc:
            logger.debug("邮箱提取失败: %s — %s", name, exc)
            return name, addr, [], ""

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_worker, c): c for c in all_companies}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    name, addr, emails, rep = future.result()
                    with lock:
                        processed += 1
                        _save_email_result(store, name, addr, emails, rep)
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

    logger.info("Pipeline 3 完成：处理 %d 家, 找到邮箱 %d 家", processed, found)
    return {"processed": processed, "found": found}


def _build_settings(output_dir: Path) -> FirecrawlEmailSettings:
    """从环境变量构建 LLM 设置。"""
    return FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",  # 用协议爬虫，不用 Firecrawl API
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
        llm_model=os.getenv("LLM_MODEL", "gpt-5.4-mini"),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", "medium"),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "40")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "8")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "8")),
    )


def _ensure_email_columns(store: BizmapsStore) -> None:
    """确保 companies 表有 emails 和 email_status 列。"""
    conn = store._conn()
    try:
        conn.execute("ALTER TABLE companies ADD COLUMN emails TEXT DEFAULT ''")
    except Exception:
        pass  # 列已存在
    try:
        conn.execute("ALTER TABLE companies ADD COLUMN email_status TEXT DEFAULT 'pending'")
    except Exception:
        pass
    conn.commit()


def _load_email_pending(store: BizmapsStore) -> list[dict]:
    """加载需要邮箱提取的公司列表。"""
    conn = store._conn()
    rows = conn.execute("""
        SELECT company_name, address, website, representative
        FROM companies
        WHERE website != '' AND website IS NOT NULL
          AND (email_status = 'pending' OR email_status IS NULL)
        ORDER BY id
    """).fetchall()
    return [dict(r) for r in rows]


def _save_email_result(store: BizmapsStore, company_name: str, address: str, emails: list[str], rep: str) -> None:
    """保存邮箱结果。"""
    conn = store._conn()
    email_str = ",".join(emails) if emails else ""
    status = "done" if True else "pending"
    if rep:
        # 如果 LLM 发现了更好的代表者信息，也更新
        conn.execute(
            "UPDATE companies SET emails = ?, email_status = ?, representative = ? WHERE company_name = ? AND address = ?",
            (email_str, status, rep, company_name, address),
        )
    else:
        conn.execute(
            "UPDATE companies SET emails = ?, email_status = ? WHERE company_name = ? AND address = ?",
            (email_str, status, company_name, address),
        )
    conn.commit()


def _filter_emails(emails: list[str]) -> list[str]:
    """过滤掉个人邮箱域名。"""
    result = []
    for email in emails:
        email = email.strip().lower()
        if not email or "@" not in email:
            continue
        domain = email.split("@", 1)[1]
        if domain in PERSONAL_EMAIL_DOMAINS:
            continue
        if email not in result:
            result.append(email)
    return result
