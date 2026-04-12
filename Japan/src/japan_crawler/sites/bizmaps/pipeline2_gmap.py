"""bizmaps Pipeline 2 — Google Maps 官网补全。

对 Pipeline 1 中没有 website 的公司，用 "公司名 所在地" 查 Google Maps，
补全官网 URL。使用多线程并发。

增加 gmap_status 列追踪已查过的公司，避免重复查询。
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

from oldiron_core.google_maps import GoogleMapsClient, GoogleMapsConfig
from oldiron_core.google_maps.client import _is_blocked_host as _gmap_is_blocked_host
from oldiron_core.google_maps.client import _normalize_url as _gmap_normalize_url
from .store import BizmapsStore

logger = logging.getLogger("bizmaps.pipeline2")
_DIRTY_WEBSITE_SQL_HINTS = (
    "%booking.com%",
    "%tripadvisor.com%",
    "%expedia.com%",
    "%hotels.com%",
    "%hoteis.com%",
    "%decolar.com%",
    "%facebook.com%",
    "%instagram.com%",
    "%x.com%",
    "%twitter.com%",
    "%linkedin.com%",
    "%youtube.com%",
    "%wa.me%",
    "%whatsapp.com%",
    "%linktr.ee%",
)

DEFAULT_CONCURRENCY = 64
DEFAULT_COMMIT_INTERVAL = 50


def run_pipeline_gmap(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, int]:
    """Pipeline 2: Google Maps 官网补全。

    从 SQLite 中读取没有 website 且未被 GMap 查过的公司，用 GMap 查询补全。
    """
    store = BizmapsStore(output_dir / "bizmaps_store.db")

    # 确保 gmap_status 列存在
    _ensure_gmap_status(store)
    repaired = _repair_dirty_gmap_websites(store)
    if repaired:
        logger.info("GMap 启动自愈：重置 %d 条脏官网并重新放回待补队列", repaired)

    # 筛选需要补全的公司 — 没有 website 且 gmap_status='pending'
    pending = _load_gmap_pending(store)
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
            result = _get_client().search_company_profile(query, name)
            website = result.website if result else ""
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
                            # 更新 SQLite：写入 website + 标记 gmap_status=done
                            _update_website(store, name, addr, website)
                            found += 1
                        else:
                            # 没找到也标记为 done，避免重复查
                            _mark_gmap_done(store, name, addr)
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


def _ensure_gmap_status(store: BizmapsStore) -> None:
    """确保 companies 表有 gmap_status 列。"""
    conn = store._conn()
    try:
        conn.execute("ALTER TABLE companies ADD COLUMN gmap_status TEXT DEFAULT 'pending'")
        conn.commit()
    except Exception:
        pass  # 列已存在


def _load_gmap_pending(store: BizmapsStore) -> list[dict]:
    """加载需要 GMap 查询的公司列表 — website 为空且 gmap_status=pending。"""
    conn = store._conn()
    rows = conn.execute("""
        SELECT company_name, address
        FROM companies
        WHERE (website = '' OR website IS NULL)
          AND (gmap_status = 'pending' OR gmap_status IS NULL)
          AND company_name != ''
        ORDER BY id
    """).fetchall()
    return [dict(r) for r in rows]


def _update_website(store: BizmapsStore, company_name: str, address: str, website: str) -> None:
    """更新单个公司的 website 字段并标记 gmap_status=done。"""
    conn = store._conn()
    conn.execute(
        "UPDATE companies SET website = ?, gmap_status = 'done' WHERE company_name = ? AND address = ? AND (website = '' OR website IS NULL)",
        (website, company_name, address),
    )
    conn.commit()


def _mark_gmap_done(store: BizmapsStore, company_name: str, address: str) -> None:
    """标记该公司的 GMap 查询完成（即使没找到官网）。"""
    conn = store._conn()
    conn.execute(
        "UPDATE companies SET gmap_status = 'done' WHERE company_name = ? AND address = ?",
        (company_name, address),
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
    normalized = _gmap_normalize_url(url)
    if not normalized:
        return ""
    try:
        host = urlparse(normalized).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if not host or "." not in host:
            return ""
        if _gmap_is_blocked_host(host):
            return ""
        if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host):
            return ""
    except Exception:
        return ""
    return normalized


def _repair_dirty_gmap_websites(store: BizmapsStore) -> int:
    """清理已写入库的脏官网，重新放回 GMap 待补队列。"""
    conn = store._conn()
    clauses = " OR ".join(["LOWER(website) LIKE ?"] * len(_DIRTY_WEBSITE_SQL_HINTS))
    rows = conn.execute(
        f"""
        SELECT id, website
        FROM companies
        WHERE website IS NOT NULL
          AND TRIM(website) != ''
          AND ({clauses}
               OR LOWER(website) = 't.n'
               OR website GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*')
        """,
        _DIRTY_WEBSITE_SQL_HINTS,
    ).fetchall()
    dirty_ids: list[int] = []
    for row in rows:
        if not _clean_website(str(row["website"] or "")):
            dirty_ids.append(int(row["id"]))
    if not dirty_ids:
        return 0
    placeholders = ",".join("?" for _ in dirty_ids)
    conn.execute(
        f"""
        UPDATE companies
        SET website = '',
            gmap_status = 'pending',
            emails = '',
            email_status = 'pending'
        WHERE id IN ({placeholders})
        """,
        dirty_ids,
    )
    conn.commit()
    return len(dirty_ids)
