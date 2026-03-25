"""xlsximport Pipeline — Protocol+LLM 从官网提取公司名和代表人。

流程：
  1. 读取 xlsx 文件，将官网+邮箱导入 SQLite
  2. 对每个待处理公司，用 protocol_crawler 爬取官网
  3. 用 LLM 从 HTML 中提取公司名称和代表人（法人）
  4. 结果写回 SQLite
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import openpyxl

from .store import XlsxImportStore

logger = logging.getLogger("xlsximport.pipeline")

DEFAULT_CONCURRENCY = 32
XLSX_PATH = Path(__file__).resolve().parents[4] / "docs" / "日本.xlsx"


def run_pipeline(
    *,
    output_dir: Path,
    xlsx_path: Path | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_items: int = 0,
) -> dict[str, int]:
    """执行 xlsximport 全流程。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "xlsximport_store.db"
    store = XlsxImportStore(db_path)

    # 第一步：导入 xlsx 数据（幂等）
    xlsx_file = xlsx_path or XLSX_PATH
    total_before = store.get_total_count()
    if total_before == 0:
        imported = _import_xlsx(store, xlsx_file)
        logger.info("从 xlsx 导入 %d 条记录", imported)
    else:
        logger.info("数据库已有 %d 条记录，跳过 xlsx 导入", total_before)

    # 第二步：Protocol+LLM 提取公司名和代表人
    pending = store.get_pending_companies()
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        logger.info("全部记录已处理完毕")
        return {"total": store.get_total_count(), "processed": 0, "found": 0}

    logger.info("Protocol+LLM 提取：待处理 %d 条, 并发=%d", len(pending), concurrency)

    stats = _run_extraction(store, pending, concurrency, output_dir)
    total = store.get_total_count()
    logger.info(
        "Pipeline 完成: 处理 %d, 成功提取 %d, 库内总计 %d",
        stats["processed"], stats["found"], total,
    )
    return {"total": total, **stats}


def _import_xlsx(store: XlsxImportStore, xlsx_path: Path) -> int:
    """读取 xlsx 并导入 DB。"""
    if not xlsx_path.exists():
        logger.error("xlsx 文件不存在: %s", xlsx_path)
        return 0

    wb = openpyxl.load_workbook(str(xlsx_path), read_only=True)
    ws = wb.active
    rows: list[dict[str, str]] = []
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
        # 列顺序：官网, 公司名称, 法人, 邮箱
        website = str(row[0] or "").strip()
        email = str(row[3] or "").strip() if len(row) > 3 else ""
        if not website:
            continue
        # 补全 URL 格式
        if website and not website.startswith("http"):
            website = "https://" + website
        rows.append({"website": website, "email": email})
    wb.close()

    return store.import_from_rows(rows)


def _run_extraction(
    store: XlsxImportStore,
    pending: list[dict],
    concurrency: int,
    output_dir: Path,
) -> dict[str, int]:
    """并发运行 Protocol+LLM 提取。"""
    # 初始化协议爬虫客户端
    from shared.oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig
    crawler = SiteCrawlClient(SiteCrawlConfig(
        proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
        timeout_seconds=20.0,
    ))

    # 初始化 LLM 服务（复用 bizmaps 的 fc_email 模块做 LLM 调用）
    from japan_crawler.fc_email.email_service import (
        FirecrawlEmailService,
        FirecrawlEmailSettings,
    )
    settings = FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
        llm_model=os.getenv("LLM_MODEL", "gpt-5.4-mini"),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", "medium"),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "40")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "8")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "8")),
    )
    settings.validate()

    processed = 0
    found = 0
    lock = threading.Lock()

    def _worker(company: dict) -> tuple[int, str, str]:
        """处理单条记录，返回 (id, company_name, representative)。"""
        cid = company["id"]
        website = company["website"]
        svc = FirecrawlEmailService(settings, firecrawl_client=crawler)
        try:
            result = svc.discover_emails(
                company_name="",  # 公司名未知，由 LLM 从页面提取
                homepage=website,
            )
            company_name = result.company_name or ""
            rep = result.representative or ""
            return cid, company_name, rep
        except Exception as exc:
            logger.debug("提取失败: %s — %s", website, exc)
            return cid, "", ""

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_worker, c): c for c in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    cid, company_name, rep = future.result()
                    with lock:
                        processed += 1
                        status = "done" if (company_name or rep) else "empty"
                        store.update_result(cid, company_name, rep, status)
                        if company_name or rep:
                            found += 1
                        if processed <= 5 or processed % 50 == 0:
                            logger.info(
                                "[提取 %d/%d] %.1f%% %s → 公司=%s 代表=%s",
                                processed, len(pending),
                                processed / len(pending) * 100,
                                company["website"][:40],
                                company_name[:20] if company_name else "-",
                                rep[:20] if rep else "-",
                            )
                except Exception as exc:
                    with lock:
                        processed += 1
                    logger.warning("工作线程异常: %s", exc)
    except KeyboardInterrupt:
        logger.info("用户中断")

    return {"processed": processed, "found": found}
