"""bizmaps Pipeline 2 — Google Maps 官网补全。

对 Pipeline 1 中没有 website 的公司，用 "公司名 所在地" 查 Google Maps，
补全官网 URL。使用多线程并发。
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from japan_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline2")

# 过滤掉的社交/信息网站
BLOCKED_HOST_HINTS = (
    "wikipedia.org", "wikidata.org", "facebook.com", "instagram.com",
    "twitter.com", "x.com", "linkedin.com", "youtube.com", "tiktok.com",
    "google.", "gstatic.", "g.page", "goo.gl", "tabelog.com",
    "hotpepper.jp", "gnavi.co.jp", "rakuten.co.jp", "amazon.co.jp",
)

DEFAULT_CONCURRENCY = 16
DEFAULT_COMMIT_INTERVAL = 50


def run_pipeline_gmap(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 2: Google Maps 官网补全。

    从 SQLite 中读取没有 website 的公司，用 GMap 查询补全。
    """
    store = BizmapsStore(output_dir / "bizmaps_store.db")

    # 筛选需要补全的公司 — 没有 website 且有公司名的记录
    all_companies = store.export_all_companies()
    pending = [
        c for c in all_companies
        if not c.get("website") and c.get("company_name")
    ]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        logger.info("没有需要 GMap 补全的公司")
        return {"processed": 0, "found": 0}

    logger.info("GMap 官网补全：待处理 %d 家, 并发=%d", len(pending), concurrency)

    # 构建 GMap 客户端（日语搜索、日本地区）
    _thread_local = threading.local()

    def _get_client() -> GoogleMapsClient:
        if not hasattr(_thread_local, "client"):
            _thread_local.client = GoogleMapsClient(
                GoogleMapsConfig(
                    hl="ja",
                    gl="jp",
                    min_delay=0.5,
                    max_delay=1.2,
                    long_rest_interval=150,
                    long_rest_seconds=8.0,
                )
            )
        return _thread_local.client

    processed = 0
    found = 0
    lock = threading.Lock()

    def _worker(company: dict[str, str]) -> tuple[str, str, str]:
        """查询一家公司，返回 (company_name, address, website)。"""
        name = company["company_name"]
        addr = company.get("address", "")
        # 查询关键词：公司名 + 地址中的都道府県/市（去掉详细门牌）
        location = _extract_location_prefix(addr)
        query = f"{name} {location}" if location else name
        try:
            website = _get_client().search_official_website(query)
            website = _clean_website(website)
            return name, addr, website
        except Exception as exc:
            logger.debug("GMap 查询失败: %s — %s", name, exc)
            return name, addr, ""

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_worker, c): c for c in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    name, addr, website = future.result()
                    with lock:
                        processed += 1
                        if website:
                            # 直接更新 SQLite
                            _update_website(store, name, addr, website)
                            found += 1
                        if processed <= 5 or processed % 50 == 0:
                            logger.info(
                                "[GMap %d/%d] %.1f%% %s → %s",
                                processed, len(pending),
                                processed / len(pending) * 100,
                                name[:30], website[:50] if website else "-",
                            )
                except Exception as exc:
                    with lock:
                        processed += 1
                    logger.warning("GMap 工作线程异常: %s", exc)
    except KeyboardInterrupt:
        logger.info("GMap 用户中断")

    total = store.get_company_count()
    logger.info("GMap 完成：处理 %d 家, 找到官网 %d 家, 库内总计 %d 家", processed, found, total)
    return {"processed": processed, "found": found, "total": total}


def _update_website(store: BizmapsStore, company_name: str, address: str, website: str) -> None:
    """更新单个公司的 website 字段。"""
    conn = store._conn()
    conn.execute(
        "UPDATE companies SET website = ? WHERE company_name = ? AND address = ? AND (website = '' OR website IS NULL)",
        (website, company_name, address),
    )
    conn.commit()


def _extract_location_prefix(address: str) -> str:
    """从日本地址中提取都道府県+市（用于 GMap 查询精准化）。

    例：'東京都港区南青山' → '東京都港区'
        '北海道札幌市中央区' → '北海道札幌市'
    """
    if not address:
        return ""
    # 匹配到市/区/郡/町/村（取前面部分）
    match = re.match(r"(.+?[都道府県].+?[市区郡町村])", address)
    if match:
        return match.group(1)
    # 回退到都道府県
    match = re.match(r"(.+?[都道府県])", address)
    if match:
        return match.group(1)
    return ""


def _clean_website(url: str) -> str:
    """清洗 GMap 返回的官网 URL。"""
    if not url:
        return ""
    # 过滤社交/信息站点
    try:
        host = urlparse(url).netloc.lower()
        if any(hint in host for hint in BLOCKED_HOST_HINTS):
            return ""
    except Exception:
        return ""
    return url
