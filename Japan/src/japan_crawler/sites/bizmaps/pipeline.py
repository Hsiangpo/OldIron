"""bizmaps Pipeline 1 — 列表页全量采集。

流程：
  1. 调 /arearequest 获取全日本 47 都道府県目录
  2. 遍历每个都道府県，逐页抓取 /s/prefs/{code}?page=N
  3. 解析 HTML 提取公司名、代表者、地址等
  4. 存入 SQLite，支持断点续跑
"""

from __future__ import annotations

import logging
from pathlib import Path

from .client import BizmapsClient, PER_PAGE
from .parser import (
    parse_company_list,
    parse_current_page,
    parse_next_page_params,
    parse_total_pages,
    parse_total_results,
)
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline")


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.5,
    proxy: str | None = None,
    max_prefs: int = 0,
) -> dict[str, int]:
    """执行 Pipeline 1：列表页全量采集。

    Args:
        output_dir: 输出目录（output/bizmaps/）
        request_delay: 请求间隔秒数
        proxy: HTTP 代理地址
        max_prefs: 最大采集都道府県数（0=全部47个，调试用）
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    store = BizmapsStore(output_dir / "bizmaps_store.db")
    client = BizmapsClient(request_delay=request_delay, proxy=proxy)

    # 第一步：获取地区目录
    existing = store.get_all_prefs()
    if not existing:
        logger.info("正在获取全日本 47 都道府県目录...")
        prefs = client.fetch_areas()
        if not prefs:
            logger.error("无法获取地区目录 — 检查代理设置（需 --proxy http://127.0.0.1:7897）")
            return {"prefs": 0, "companies": 0, "errors": 1}
        stored = store.upsert_prefs(prefs)
        total = sum(p["total"] for p in prefs)
        logger.info("写入 %d 个都道府県，全国企業数约 %d 家", stored, total)
    else:
        logger.info("已有 %d 个都道府県在库中", len(existing))

    # 第二步：逐都道府県逐页采集
    pending = store.get_pending_prefs()
    if max_prefs > 0:
        pending = pending[:max_prefs]

    total_new = 0
    total_done = 0

    for pref in pending:
        pref_code = pref["pref_code"]
        pref_name = pref["name"]
        last_page = pref.get("last_page", 0)
        start_page = last_page + 1 if last_page > 0 else 1

        logger.info("━━ [%s] %s (预计 %d 家) ━━", pref_code, pref_name, pref["total"])

        # 获取首页确定总页数
        next_ph = ""  # 下一页的 ph 签名 token
        if start_page == 1:
            result = _process_first_page(client, store, pref_code, pref_name)
            if result["count"] < 0:
                continue  # 首页获取失败，跳过该都道府県
            total_new += result["count"]
            next_ph = result["next_ph"]
            start_page = 2
        else:
            # 续跑：从 checkpoint 恢复上次保存的 ph
            cp = store.get_checkpoint(pref_code)
            if cp:
                next_ph = cp.get("last_ph", "")
                if next_ph:
                    logger.info("  续跑 page=%d，从 checkpoint 恢复 ph", start_page)
                else:
                    # ph 丢失（旧库或首次升级），重置为 page=1 重新开始
                    # UNIQUE(company_name, address) 约束保证已入库数据不会重复
                    logger.warning("  ph 缺失，重置 %s 从 page=1 重新开始（已有数据受 UNIQUE 约束保护）", pref_name)
                    start_page = 1
                    result = _process_first_page(client, store, pref_code, pref_name)
                    if result["count"] < 0:
                        continue
                    total_new += result["count"]
                    next_ph = result["next_ph"]
                    start_page = 2

        cp = store.get_checkpoint(pref_code)
        total_pages = cp["total_pages"] if cp else 1

        # 翻页采集
        page_ok = _crawl_remaining_pages(
            client, store, pref_code, pref_name,
            start_page, total_pages, next_ph,
        )
        total_new += page_ok["new"]

        if page_ok["completed"]:
            store.update_checkpoint(pref_code, total_pages, total_pages, "done")
            total_done += 1
            logger.info("  ✓ %s 完成 (%d 页)", pref_name, total_pages)

    total_companies = store.get_company_count()
    stats = client.stats
    logger.info(
        "Pipeline 1 小结: %d 个都道府県完成, 新增 %d 家, 库内总计 %d 家, 请求 %d 次, 错误 %d 次",
        total_done, total_new, total_companies, stats["requests"], stats["errors"],
    )
    return {
        "prefs_done": total_done,
        "new_companies": total_new,
        "total_companies": total_companies,
        **stats,
    }


def _process_first_page(client: BizmapsClient, store: BizmapsStore, pref_code: str, pref_name: str) -> dict:
    """处理首页，返回 {count: 新增数, next_ph: 下一页签名}。count=-1 表示获取失败。"""
    html_text = client.fetch_list_page(pref_code, 1)
    if html_text is None:
        logger.warning("跳过 %s（无法获取首页）", pref_name)
        return {"count": -1, "next_ph": ""}

    companies = parse_company_list(html_text)
    total_results = parse_total_results(html_text)
    total_pages = parse_total_pages(html_text, PER_PAGE)

    # 提取下一页的 ph 签名
    next_params = parse_next_page_params(html_text, 1)
    next_ph = next_params["ph"] if next_params else ""

    new = 0
    if companies:
        new = store.upsert_companies(pref_code, companies)
        logger.info("  页 1/%d: 解析 %d 家, 新增 %d 家 (总计 %d 件)", total_pages, len(companies), new, total_results)
    else:
        logger.warning("  页 1: 未解析到公司（HTML 长度 %d）", len(html_text))

    store.update_checkpoint(pref_code, 1, total_pages, "running", last_ph=next_ph)
    return {"count": new, "next_ph": next_ph}


def _crawl_remaining_pages(
    client: BizmapsClient,
    store: BizmapsStore,
    pref_code: str,
    pref_name: str,
    start_page: int,
    total_pages: int,
    initial_ph: str = "",
) -> dict[str, object]:
    """翻页采集剩余页面。每一页从 HTML 中提取下一页的 ph 签名。"""
    new_total = 0
    current_ph = initial_ph  # 当前页要用的 ph
    for page in range(start_page, total_pages + 1):
        html_text = client.fetch_list_page(pref_code, page, ph=current_ph)
        if html_text is None:
            logger.warning("  页 %d 获取失败，停止 %s", page, pref_name)
            store.update_checkpoint(pref_code, page - 1, total_pages, "error", last_ph=current_ph)
            return {"new": new_total, "completed": False}

        actual_page = parse_current_page(html_text)
        if actual_page is not None and actual_page != page:
            logger.warning(
                "  页 %d 实际返回的是页 %d（通常表示 ph 缺失或失效），停止 %s 以避免误采第一页数据",
                page,
                actual_page,
                pref_name,
            )
            store.update_checkpoint(pref_code, page - 1, total_pages, "error", last_ph="")
            return {"new": new_total, "completed": False}

        companies = parse_company_list(html_text)
        if companies:
            new = store.upsert_companies(pref_code, companies)
            new_total += new
            # 每 20 页或最后一页打印进度
            if page % 20 == 0 or page == total_pages:
                logger.info("  页 %d/%d: 解析 %d 家, 新增 %d 家", page, total_pages, len(companies), new)
        else:
            logger.warning("  页 %d: 未解析到公司", page)

        # 提取下一页的 ph 签名（链式传递）
        next_params = parse_next_page_params(html_text, page)
        next_ph = next_params["ph"] if next_params else ""
        if not next_ph and page < total_pages:
            logger.warning("  页 %d: ph 提取失败（翻页链断裂），停止 %s，等待下轮从安全位置恢复", page, pref_name)
            store.update_checkpoint(pref_code, page, total_pages, "error", last_ph="")
            return {"new": new_total, "completed": False}
        current_ph = next_ph

        # 持久化断点（含 ph）
        store.update_checkpoint(pref_code, page, total_pages, "running", last_ph=current_ph)

    return {"new": new_total, "completed": True}
