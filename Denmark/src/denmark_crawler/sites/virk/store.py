"""Virk SQLite 断点存储。"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_phone(value: str) -> str:
    text = re.sub(r"[^\d+]+", "", str(value or "").strip())
    if text.startswith("+45"):
        text = text[3:]
    return text


def _dump_json_list(items: list[str]) -> str:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _parse_json_list(raw: str) -> list[str]:
    try:
        payload = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(x or "").strip().lower() for x in payload if str(x or "").strip()]


def _extract_domain(url: str) -> str:
    value = str(url or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        value = value.split("://", 1)[1]
    value = value.split("/", 1)[0]
    if value.startswith("www."):
        value = value[4:]
    return value


def _tmp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl_atomic(path: Path, rows: list[dict[str, object]]) -> None:
    tmp = _tmp_path(path)
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    tmp.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp, path)


def _sqlite_retry(conn, operation, *, attempts=6, base_delay=0.05, cap=0.5):
    """SQLite 锁冲突短退避重试。"""
    for i in range(max(attempts, 1)):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or i + 1 >= attempts:
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(min(base_delay * (2 ** i), cap))
    raise RuntimeError("sqlite retry unreachable")


# ---- 任务数据类 ----

@dataclass(slots=True)
class VirkSearchTask:
    """搜索分段任务。"""
    segment_key: str       # "kommune_kode:virksomhedsform_kode" 的唯一标识
    kommune_kode: str
    virksomhedsform_kode: str
    page_index: int
    retries: int = 0


@dataclass(slots=True)
class VirkDetailTask:
    """详情拉取任务。"""
    cvr: str
    retries: int = 0


@dataclass(slots=True)
class VirkGMapTask:
    """GMap 补全任务。"""
    cvr: str
    company_name: str
    address: str
    virk_phone: str
    retries: int = 0


@dataclass(slots=True)
class VirkFirecrawlTask:
    """Firecrawl 邮箱补全任务。"""
    cvr: str
    company_name: str
    website: str
    domain: str
    retries: int = 0


@dataclass(slots=True)
class VirkProgress:
    """运行进度。"""
    search_total: int
    search_done: int
    detail_total: int
    detail_done: int
    detail_pending: int
    detail_running: int
    gmap_pending: int
    gmap_running: int
    firecrawl_pending: int
    firecrawl_running: int
    companies_total: int
    final_total: int


class VirkStore:
    """Virk 断点与快照存储。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout = 30000;")
        self._init_schema()
        self._repair_runtime_state()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS search_segments (
                    segment_key TEXT PRIMARY KEY,
                    kommune_kode TEXT NOT NULL,
                    kommune_navn TEXT NOT NULL DEFAULT '',
                    virksomhedsform_kode TEXT NOT NULL DEFAULT '',
                    virksomhedsform_navn TEXT NOT NULL DEFAULT '',
                    total_companies INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS search_tasks (
                    segment_key TEXT NOT NULL,
                    page_index INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(segment_key, page_index)
                );

                CREATE TABLE IF NOT EXISTS companies (
                    cvr TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    postcode TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    industry_code TEXT NOT NULL DEFAULT '',
                    industry_name TEXT NOT NULL DEFAULT '',
                    company_type TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    start_date TEXT NOT NULL DEFAULT '',
                    kommune TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    representative_role TEXT NOT NULL DEFAULT '',
                    purpose TEXT NOT NULL DEFAULT '',
                    registered_capital TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    owners_json TEXT NOT NULL DEFAULT '[]',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    gmap_phone TEXT NOT NULL DEFAULT '',
                    gmap_company_name TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source_segment TEXT NOT NULL DEFAULT '',
                    source_page INTEGER NOT NULL DEFAULT 0,
                    detail_status TEXT NOT NULL DEFAULT 'pending',
                    gmap_status TEXT NOT NULL DEFAULT '',
                    firecrawl_status TEXT NOT NULL DEFAULT '',
                    firecrawl_retry_at TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS detail_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gmap_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS firecrawl_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS final_companies (
                    cvr TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    representative TEXT NOT NULL,
                    email TEXT NOT NULL,
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(cvr, representative, email)
                );

                CREATE INDEX IF NOT EXISTS idx_virk_search_claim
                ON search_tasks(status, next_run_at, updated_at, segment_key, page_index);
                CREATE INDEX IF NOT EXISTS idx_virk_detail_claim
                ON detail_queue(status, next_run_at, updated_at, cvr);
                CREATE INDEX IF NOT EXISTS idx_virk_gmap_claim
                ON gmap_queue(status, next_run_at, updated_at, cvr);
                CREATE INDEX IF NOT EXISTS idx_virk_firecrawl_claim
                ON firecrawl_queue(status, next_run_at, updated_at, cvr);
            """)
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        """启动时将所有 running 状态重置为 pending。"""
        now = _utc_now()
        with self._lock:
            for table in ("search_tasks", "detail_queue", "gmap_queue", "firecrawl_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (now,),
                )
            self._conn.commit()

    # ---- 搜索分段管理 ----

    def get_planned_segments(self) -> set[str]:
        """获取已规划的搜索分段。"""
        with self._lock:
            rows = self._conn.execute("SELECT segment_key FROM search_segments").fetchall()
            return {row["segment_key"] for row in rows}

    def save_segment(
        self, segment_key: str, kommune_kode: str, kommune_navn: str,
        vf_kode: str, vf_navn: str, total: int, pages: int,
    ) -> None:
        """保存分段信息并创建搜索任务页。"""
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO search_segments
                (segment_key, kommune_kode, kommune_navn, virksomhedsform_kode,
                 virksomhedsform_navn, total_companies, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'done', ?)""",
                (segment_key, kommune_kode, kommune_navn, vf_kode, vf_navn, total, now),
            )
            for p in range(pages):
                self._conn.execute(
                    """INSERT OR IGNORE INTO search_tasks
                    (segment_key, page_index, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, ?, 'pending', 0, ?, '', ?)""",
                    (segment_key, p, now, now),
                )
            self._conn.commit()

    # ---- 搜索任务领取/完成 ----

    def claim_search_task(self) -> VirkSearchTask | None:
        """领取一个待执行搜索任务。"""
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT segment_key, page_index, retries FROM search_tasks
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                """UPDATE search_tasks SET status = 'running', updated_at = ?
                WHERE segment_key = ? AND page_index = ?""",
                (now, row["segment_key"], row["page_index"]),
            )
            self._conn.commit()
            # 从 search_segments 取 kommune_kode 和 virksomhedsform_kode
            seg = self._conn.execute(
                "SELECT kommune_kode, virksomhedsform_kode FROM search_segments WHERE segment_key = ?",
                (row["segment_key"],),
            ).fetchone()
            return VirkSearchTask(
                segment_key=row["segment_key"],
                kommune_kode=seg["kommune_kode"] if seg else "",
                virksomhedsform_kode=seg["virksomhedsform_kode"] if seg else "",
                page_index=row["page_index"],
                retries=row["retries"],
            )

    def mark_search_done(self, segment_key: str, page_index: int) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE search_tasks SET status = 'done', updated_at = ? WHERE segment_key = ? AND page_index = ?",
                (now, segment_key, page_index),
            )
            self._conn.commit()

    def mark_search_failed(self, segment_key: str, page_index: int, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """UPDATE search_tasks SET status = 'failed', last_error = ?, updated_at = ?
                WHERE segment_key = ? AND page_index = ?""",
                (error_text[:500], now, segment_key, page_index),
            )
            self._conn.commit()

    def defer_search_task(self, segment_key: str, page_index: int,
                          retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE search_tasks SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE segment_key = ? AND page_index = ?""",
                (retries, run_at, error_text[:500], now, segment_key, page_index),
            )
            self._conn.commit()

    def has_search_work(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) as c FROM search_tasks WHERE status IN ('pending', 'running')"
            ).fetchone()
            return (row["c"] if row else 0) > 0

    # ---- 公司 upsert ----

    def upsert_company(self, company) -> None:
        """插入或更新公司记录（搜索阶段）。"""
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO companies
                (cvr, company_name, address, postcode, city, phone, email,
                 industry_code, industry_name, company_type, status, start_date,
                 source_segment, source_page, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cvr) DO UPDATE SET
                    company_name = COALESCE(NULLIF(excluded.company_name, ''), companies.company_name),
                    address = COALESCE(NULLIF(excluded.address, ''), companies.address),
                    phone = COALESCE(NULLIF(excluded.phone, ''), companies.phone),
                    email = COALESCE(NULLIF(excluded.email, ''), companies.email),
                    updated_at = excluded.updated_at
                """,
                (
                    company.cvr, company.company_name, company.address,
                    company.postcode, company.city, company.phone, company.email,
                    company.industry_code, company.industry_name,
                    company.company_type, company.status, company.start_date,
                    company.source_segment, company.source_page, now,
                ),
            )
            # 同时创建详情任务
            self._conn.execute(
                """INSERT OR IGNORE INTO detail_queue
                (cvr, status, retries, next_run_at, last_error, updated_at)
                VALUES (?, 'pending', 0, ?, '', ?)""",
                (company.cvr, now, now),
            )
            self._conn.commit()

    def upsert_company_detail(self, company) -> None:
        """更新公司详情数据（详情阶段）。"""
        now = _utc_now()
        phone = _normalize_phone(company.phone)
        email = str(company.email or "").strip().lower()
        deliverable = bool(email and company.representative and company.company_name)
        with self._lock:
            self._conn.execute(
                """UPDATE companies SET
                    phone = COALESCE(NULLIF(?, ''), phone),
                    email = COALESCE(NULLIF(?, ''), email),
                    kommune = ?, representative = ?, representative_role = ?,
                    purpose = ?, registered_capital = ?, owners_json = ?,
                    detail_status = 'done', updated_at = ?
                WHERE cvr = ?""",
                (
                    phone, email, company.kommune,
                    company.representative, company.representative_role,
                    company.purpose, company.registered_capital,
                    company.owners_json, now, company.cvr,
                ),
            )
            # 如果数据齐全直接落盘 final
            if deliverable:
                self._insert_final(company.cvr, company.company_name,
                                   company.representative, email, "", "virk-direct")
            else:
                # 缺数据 → 派生 GMap 任务
                self._conn.execute(
                    """INSERT OR IGNORE INTO gmap_queue
                    (cvr, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (company.cvr, now, now),
                )
                self._conn.execute(
                    "UPDATE companies SET gmap_status = 'pending', updated_at = ? WHERE cvr = ?",
                    (now, company.cvr),
                )
            self._conn.commit()

    def _insert_final(self, cvr: str, name: str, rep: str, email: str,
                      evidence: str, source: str) -> None:
        """插入最终交付记录。"""
        now = _utc_now()
        self._conn.execute(
            """INSERT OR IGNORE INTO final_companies
            (cvr, company_name, representative, email, evidence_url, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cvr, name, rep, email, evidence, source, now),
        )

    # ---- 详情任务管理 ----

    def claim_detail_task(self) -> VirkDetailTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT cvr, retries FROM detail_queue
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE cvr = ?",
                (now, row["cvr"]),
            )
            self._conn.commit()
            return VirkDetailTask(cvr=row["cvr"], retries=row["retries"])

    def mark_detail_done(self, cvr: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'done', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            self._conn.commit()

    def mark_detail_failed(self, cvr: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE cvr = ?",
                (error_text[:500], now, cvr),
            )
            self._conn.commit()

    def defer_detail_task(self, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE detail_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE cvr = ?""",
                (retries, run_at, error_text[:500], now, cvr),
            )
            self._conn.commit()

    def get_company_for_detail(self, cvr: str):
        """获取公司记录用于详情补全。"""
        from denmark_crawler.sites.virk.models import VirkCompany
        with self._lock:
            row = self._conn.execute("SELECT * FROM companies WHERE cvr = ?", (cvr,)).fetchone()
            if not row:
                return None
            return VirkCompany(
                cvr=row["cvr"], company_name=row["company_name"],
                address=row["address"], postcode=row["postcode"],
                city=row["city"], phone=row["phone"], email=row["email"],
                industry_code=row["industry_code"], industry_name=row["industry_name"],
                company_type=row["company_type"], status=row["status"],
                start_date=row["start_date"], source_segment=row["source_segment"],
            )

    # ---- GMap 任务管理 ----

    def claim_gmap_task(self) -> VirkGMapTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT g.cvr, g.retries, c.company_name, c.address, c.phone
                FROM gmap_queue g JOIN companies c ON g.cvr = c.cvr
                WHERE g.status = 'pending' AND g.next_run_at <= ?
                ORDER BY g.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'running', updated_at = ? WHERE cvr = ?",
                (now, row["cvr"]),
            )
            self._conn.commit()
            return VirkGMapTask(
                cvr=row["cvr"], company_name=row["company_name"],
                address=row["address"], virk_phone=row["phone"],
                retries=row["retries"],
            )

    def mark_gmap_done(self, *, cvr: str, website: str, source: str,
                       phone: str, company_name: str) -> None:
        now = _utc_now()
        domain = _extract_domain(website)
        with self._lock:
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'done', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            self._conn.execute(
                """UPDATE companies SET
                    homepage = COALESCE(NULLIF(?, ''), homepage),
                    domain = COALESCE(NULLIF(?, ''), domain),
                    gmap_phone = ?, gmap_company_name = ?,
                    gmap_status = 'done', updated_at = ?
                WHERE cvr = ?""",
                (website, domain, _normalize_phone(phone), company_name, now, cvr),
            )
            if domain:
                self._conn.execute(
                    """INSERT OR IGNORE INTO firecrawl_queue
                    (cvr, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (cvr, now, now),
                )
                self._conn.execute(
                    "UPDATE companies SET firecrawl_status = 'pending', updated_at = ? WHERE cvr = ?",
                    (now, cvr),
                )
            else:
                self._conn.execute(
                    "UPDATE companies SET gmap_status = 'done', updated_at = ? WHERE cvr = ?",
                    (now, cvr),
                )
            self._conn.commit()

    def mark_gmap_failed(self, *, cvr: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE cvr = ?",
                (error_text[:500], now, cvr),
            )
            self._conn.execute(
                "UPDATE companies SET gmap_status = 'failed', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            self._conn.commit()

    def defer_gmap_task(self, *, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE gmap_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE cvr = ?""",
                (retries, run_at, error_text[:500], now, cvr),
            )
            self._conn.commit()

    # ---- Firecrawl 任务管理 ----

    def claim_firecrawl_task(self) -> VirkFirecrawlTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT f.cvr, f.retries, c.company_name, c.homepage, c.domain
                FROM firecrawl_queue f JOIN companies c ON f.cvr = c.cvr
                WHERE f.status = 'pending' AND f.next_run_at <= ?
                ORDER BY f.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'running', updated_at = ? WHERE cvr = ?",
                (now, row["cvr"]),
            )
            self._conn.commit()
            return VirkFirecrawlTask(
                cvr=row["cvr"], company_name=row["company_name"],
                website=row["homepage"], domain=row["domain"],
                retries=row["retries"],
            )

    def mark_firecrawl_done(
        self, *, cvr: str, emails: list[str],
        representative: str = "", company_name: str = "",
        evidence_url: str = "", retry_after_seconds: float = 0.0,
    ) -> None:
        """标记 Firecrawl 完成，支持官网公司名/代表人覆盖。

        逻辑：如果 LLM 从官网提取到 company_name，就用官网的公司名
        和代表人覆盖 Virk 注册信息。
        """
        now = _utc_now()
        with self._lock:
            # 读取当前公司信息
            current = self._conn.execute(
                "SELECT company_name, representative, gmap_company_name, gmap_phone, emails_json FROM companies WHERE cvr = ?",
                (cvr,),
            ).fetchone()
            if not current:
                self._conn.execute(
                    "UPDATE firecrawl_queue SET status = 'done', updated_at = ? WHERE cvr = ?",
                    (now, cvr),
                )
                self._conn.commit()
                return

            # 合并邮箱
            merged = _parse_json_list(str(current["emails_json"] or "[]"))
            for email in emails:
                value = str(email or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            has_email = bool(merged)

            # —— 官网信息优先覆盖 ——
            # 公司名：优先用 LLM 从官网提取到的，其次用 GMap 的，最后保留 Virk 原始
            next_company_name = str(current["company_name"] or "").strip()
            website_name = str(company_name or "").strip()
            if website_name:
                next_company_name = website_name
            elif str(current["gmap_company_name"] or "").strip():
                next_company_name = str(current["gmap_company_name"]).strip()

            # 代表人：优先用 LLM 提取到的
            next_representative = str(current["representative"] or "").strip()
            website_rep = str(representative or "").strip()
            if website_rep:
                next_representative = website_rep

            next_evidence_url = str(evidence_url or "").strip()

            self._conn.execute(
                """UPDATE companies SET
                    company_name = ?, representative = ?,
                    emails_json = ?, evidence_url = ?,
                    firecrawl_status = ?, firecrawl_retry_at = ?,
                    last_error = '', updated_at = ?
                WHERE cvr = ?""",
                (
                    next_company_name, next_representative,
                    _dump_json_list(merged), next_evidence_url,
                    "done" if has_email else "zero",
                    "" if has_email or retry_after_seconds <= 0 else _utc_after(retry_after_seconds),
                    now, cvr,
                ),
            )
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'done', last_error = '', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            # 有邮箱就落盘 final（用覆盖后的公司名和代表人）
            if has_email:
                for em in merged:
                    self._insert_final(cvr, next_company_name, next_representative, em,
                                       next_evidence_url, "virk-firecrawl")
            self._conn.commit()

    def mark_firecrawl_failed(self, *, cvr: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE cvr = ?",
                (error_text[:500], now, cvr),
            )
            self._conn.execute(
                "UPDATE companies SET firecrawl_status = 'failed', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            self._conn.commit()

    def defer_firecrawl_task(
        self, *, cvr: str, retries: int, delay_seconds: float, error_text: str,
    ) -> None:
        """将 Firecrawl 任务延迟重试。"""
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE firecrawl_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE cvr = ?""",
                (retries, run_at, error_text[:500], now, cvr),
            )
            self._conn.commit()

    # ---- 进度 ----

    def get_progress(self) -> VirkProgress:
        with self._lock:
            def _count(table, status):
                r = self._conn.execute(
                    f"SELECT COUNT(*) as c FROM {table} WHERE status = ?", (status,)
                ).fetchone()
                return r["c"] if r else 0

            st = self._conn.execute("SELECT COUNT(*) as c FROM search_tasks").fetchone()
            sd = self._conn.execute("SELECT COUNT(*) as c FROM search_tasks WHERE status='done'").fetchone()
            dt = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue").fetchone()
            dd = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue WHERE status='done'").fetchone()
            ct = self._conn.execute("SELECT COUNT(*) as c FROM companies").fetchone()
            ft = self._conn.execute("SELECT COUNT(*) as c FROM final_companies").fetchone()

            return VirkProgress(
                search_total=st["c"] if st else 0,
                search_done=sd["c"] if sd else 0,
                detail_total=dt["c"] if dt else 0,
                detail_done=dd["c"] if dd else 0,
                detail_pending=_count("detail_queue", "pending"),
                detail_running=_count("detail_queue", "running"),
                gmap_pending=_count("gmap_queue", "pending"),
                gmap_running=_count("gmap_queue", "running"),
                firecrawl_pending=_count("firecrawl_queue", "pending"),
                firecrawl_running=_count("firecrawl_queue", "running"),
                companies_total=ct["c"] if ct else 0,
                final_total=ft["c"] if ft else 0,
            )

    def company_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) as c FROM companies").fetchone()
            return row["c"] if row else 0

    # ---- 陈旧任务回收 ----

    def requeue_stale_running_tasks(self, *, older_than_seconds: float = 300.0) -> dict[str, int]:
        cutoff = _utc_after(-older_than_seconds)
        now = _utc_now()
        recovered = {}
        with self._lock:
            for table in ("search_tasks", "detail_queue", "gmap_queue", "firecrawl_queue"):
                cur = self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running' AND updated_at < ?",
                    (now, cutoff),
                )
                if cur.rowcount > 0:
                    recovered[table] = cur.rowcount
            self._conn.commit()
        return recovered

    # ---- JSONL 快照导出 ----

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            # 全部公司
            rows = self._conn.execute("SELECT * FROM companies ORDER BY cvr").fetchall()
            companies = [dict(r) for r in rows]
            _write_jsonl_atomic(output_dir / "virk_companies.jsonl", companies)

            # 最终交付
            rows = self._conn.execute("SELECT * FROM final_companies ORDER BY cvr").fetchall()
            finals = [dict(r) for r in rows]
            _write_jsonl_atomic(output_dir / "virk_final.jsonl", finals)
