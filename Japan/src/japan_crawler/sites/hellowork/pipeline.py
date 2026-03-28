"""hellowork Pipeline 1 — 列表页 + 详情页爬取。

流程：
  1. 创建 N 个独立 session（各自 JSESSIONID），分配到 N 个县并行
  2. 每个县：POST 搜索 → 分页（串行）→ 每页提取详情 URL
  3. 每页的详情 URL：ThreadPool 并发 GET → 解析 → 入库
  4. 断点续跑：按 (pref_code, page) 保存 checkpoint
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import HelloworkClient, PREF_NAMES
from .parser import (
    parse_detail_page,
    parse_detail_urls,
    parse_total_count,
    parse_total_pages,
)
from .store import HelloworkStore

logger = logging.getLogger("hellowork.pipeline")

PER_PAGE = 30  # 每页件数（30 比 50 更稳定）
DEFAULT_DETAIL_WORKERS = 16  # 详情页并发数
DEFAULT_PREF_WORKERS = 6  # 跨县并发数


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 2.0,
    proxy: str = "",
    max_prefs: int = 0,
    detail_workers: int = DEFAULT_DETAIL_WORKERS,
    pref_workers: int = DEFAULT_PREF_WORKERS,
) -> dict[str, int]:
    """Pipeline 1: 列表/详情页爬取（支持跨县并发 + 详情页并发）。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = HelloworkStore(output_dir / "hellowork_store.db")

    # 注册 47 都道府県
    prefs = [
        {"pref_code": code, "name": name, "total": 0}
        for code, name in sorted(PREF_NAMES.items())
    ]
    store.upsert_prefs(prefs)

    # 获取待处理的都道府県
    pending = store.get_pending_prefs()
    if max_prefs > 0:
        pending = pending[:max_prefs]

    if not pending:
        logger.info("所有都道府県已完成")
        return {"companies": store.get_company_count(), "prefs_done": 0}

    logger.info(
        "待处理都道府県: %d 个 | 县并发=%d, 详情并发=%d",
        len(pending), pref_workers, detail_workers,
    )

    total_new = 0
    prefs_done = 0
    lock = threading.Lock()

    def _pref_task(pref: dict) -> tuple[str, int]:
        """单个县的爬取任务（独立 session）。"""
        pref_code = pref["pref_code"]
        pref_name = pref["name"]
        client = HelloworkClient(request_delay=request_delay, proxy=proxy)
        if not client.init_session():
            logger.error("%s session 初始化失败", pref_name)
            return pref_name, 0
        new = _crawl_prefecture(client, store, pref_code, pref_name, detail_workers)
        logger.info(
            "[进度] %s 完成，新增 %d 家 | 总库存 %d | %s",
            pref_name, new, store.get_company_count(), client.stats,
        )
        return pref_name, new

    if pref_workers <= 1:
        # 串行
        for pref in pending:
            name, new = _pref_task(pref)
            total_new += new
            prefs_done += 1
    else:
        # 跨县并发
        with ThreadPoolExecutor(max_workers=pref_workers, thread_name_prefix="Pref") as executor:
            futures = {executor.submit(_pref_task, p): p for p in pending}
            for future in as_completed(futures):
                name, new = future.result()
                with lock:
                    total_new += new
                    prefs_done += 1

    stats = {
        "companies": store.get_company_count(),
        "new_companies": total_new,
        "prefs_done": prefs_done,
    }
    logger.info("Pipeline 1 完成: %s", stats)
    return stats


def _crawl_prefecture(
    client: HelloworkClient,
    store: HelloworkStore,
    pref_code: str,
    pref_name: str,
    detail_workers: int,
) -> int:
    """爬取一个都道府県的所有企业。"""
    # 断点续跑
    checkpoint = store.get_checkpoint(pref_code)
    start_page = 1
    if checkpoint and checkpoint["status"] == "running":
        start_page = checkpoint["last_page"] + 1
        logger.info("断点续跑 %s: 从第 %d 页开始", pref_name, start_page)

    # 第一页搜索 — 获取总件数
    logger.info("搜索 %s (%s)...", pref_name, pref_code)
    html = client.search(pref_code, page=1, per_page=PER_PAGE)
    if not html:
        logger.error("%s 搜索失败", pref_name)
        return 0

    total_count = parse_total_count(html)
    total_pages = parse_total_pages(total_count, PER_PAGE)
    logger.info("%s: %d 件求人, %d 页", pref_name, total_count, total_pages)

    if total_count == 0:
        store.update_checkpoint(pref_code, 0, 0, status="done")
        return 0

    store.upsert_prefs([{"pref_code": pref_code, "name": pref_name, "total": total_count}])

    new_companies = 0

    # 如果从第 1 页开始，处理当前页
    if start_page == 1:
        new_companies += _process_search_page(
            client, store, pref_code, html, 1, total_pages, detail_workers,
        )
        store.update_checkpoint(pref_code, 1, total_pages, status="running")
        start_page = 2

    # 翻页（串行，因为依赖 session 状态）
    for page in range(start_page, total_pages + 1):
        html = client.search(pref_code, page=page, per_page=PER_PAGE, total_count=total_count)
        if not html:
            logger.warning("%s 第 %d 页搜索失败，保存断点", pref_name, page)
            store.update_checkpoint(pref_code, page - 1, total_pages, status="running")
            return new_companies

        count = parse_total_count(html)
        if count == 0:
            logger.info("%s 第 %d 页件数为 0，结束", pref_name, page)
            break

        new_companies += _process_search_page(
            client, store, pref_code, html, page, total_pages, detail_workers,
        )
        store.update_checkpoint(pref_code, page, total_pages, status="running")

    store.update_checkpoint(pref_code, total_pages, total_pages, status="done")
    return new_companies


def _process_search_page(
    client: HelloworkClient,
    store: HelloworkStore,
    pref_code: str,
    page_html: str,
    page_num: int,
    total_pages: int,
    detail_workers: int,
) -> int:
    """处理一个搜索结果页：提取详情 URL → 并发 GET → 解析入库。"""
    detail_urls = parse_detail_urls(page_html)
    if not detail_urls:
        logger.warning("第 %d/%d 页未找到详情链接", page_num, total_pages)
        return 0

    logger.info("第 %d/%d 页: %d 个独立企业, 并发=%d", page_num, total_pages, len(detail_urls), detail_workers)

    new_count = 0

    if detail_workers <= 1:
        # 串行
        for url in detail_urls:
            html = client.fetch_detail(url)
            if html:
                company = parse_detail_page(html)
                if company:
                    company["detail_url"] = url
                    if store.upsert_company(pref_code, company):
                        new_count += 1
    else:
        # 并发抓详情页
        lock = threading.Lock()

        def _fetch_one(url: str) -> dict | None:
            html = client.fetch_detail(url)
            if not html:
                return None
            company = parse_detail_page(html)
            if company:
                company["detail_url"] = url
            return company

        with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="Detail") as executor:
            futures = {executor.submit(_fetch_one, u): u for u in detail_urls}
            for future in as_completed(futures):
                company = future.result()
                if company:
                    with lock:
                        if store.upsert_company(pref_code, company):
                            new_count += 1

    logger.info("  第 %d 页入库: %d 家", page_num, new_count)
    return new_count
