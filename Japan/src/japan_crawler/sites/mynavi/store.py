"""mynavi 站点 SQLite 存储。"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class MynaviStore:
    """线程安全的 mynavi 数据存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._max_write_retries = 6
        self._init_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=60000")
            self._local.conn = conn
        return conn

    def _run_write(self, action) -> Any:
        for attempt in range(self._max_write_retries):
            try:
                conn = self._conn()
                result = action(conn)
                conn.commit()
                return result
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower():
                    raise
                if attempt == self._max_write_retries - 1:
                    raise
                time.sleep(0.2 * (attempt + 1))
        raise RuntimeError("SQLite 写入重试失败")

    def _init_tables(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companies (
                company_key TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                representative TEXT DEFAULT '',
                website TEXT DEFAULT '',
                address TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                detail_url TEXT DEFAULT '',
                source_job_url TEXT DEFAULT '',
                emails TEXT DEFAULT '',
                gmap_status TEXT DEFAULT 'pending',
                email_status TEXT DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                pref_code TEXT PRIMARY KEY,
                last_page INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending'
            );
            """
        )
        conn.commit()

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for company in companies:
                company_key = build_company_key(
                    company.get("company_name", ""),
                    company.get("website", ""),
                    company.get("address", ""),
                )
                company_name = str(company.get("company_name", "") or "").strip()
                if not company_key or not company_name:
                    continue
                existed = conn.execute(
                    "SELECT 1 FROM companies WHERE company_key = ?",
                    (company_key,),
                ).fetchone() is not None
                conn.execute(
                    """
                    INSERT INTO companies (
                        company_key, company_name, representative, website,
                        address, industry, phone, detail_url, source_job_url,
                        emails, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(company_key) DO UPDATE SET
                        company_name = excluded.company_name,
                        representative = CASE
                            WHEN excluded.representative NOT IN ('', '-')
                            THEN excluded.representative
                            ELSE companies.representative
                        END,
                        website = CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                        address = CASE WHEN excluded.address != '' THEN excluded.address ELSE companies.address END,
                        industry = CASE WHEN excluded.industry != '' THEN excluded.industry ELSE companies.industry END,
                        phone = CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                        detail_url = CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END,
                        source_job_url = CASE
                            WHEN excluded.source_job_url != '' THEN excluded.source_job_url
                            ELSE companies.source_job_url
                        END,
                        emails = CASE WHEN excluded.emails != '' THEN excluded.emails ELSE companies.emails END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        company_key,
                        company_name,
                        company.get("representative", ""),
                        company.get("website", ""),
                        company.get("address", ""),
                        company.get("industry", ""),
                        company.get("phone", ""),
                        company.get("detail_url", ""),
                        company.get("source_job_url", ""),
                        company.get("emails", ""),
                        _now_text(),
                    ),
                )
                inserted += int(not existed)
            return inserted

        return int(self._run_write(_action) or 0)

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return int(row["cnt"] if row else 0)

    def get_checkpoint(self, pref_code: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, status FROM checkpoints WHERE pref_code = ?",
            (pref_code,),
        ).fetchone()
        return dict(row) if row else None

    def update_checkpoint(self, pref_code: str, last_page: int, status: str = "running") -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                INSERT INTO checkpoints (pref_code, last_page, status)
                VALUES (?, ?, ?)
                ON CONFLICT(pref_code) DO UPDATE SET
                    last_page = excluded.last_page,
                    status = excluded.status
                """,
                (pref_code, last_page, status),
            )
        )

    def get_gmap_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_key, company_name, address
            FROM companies
            WHERE (website = '' OR website IS NULL)
              AND (gmap_status = 'pending' OR gmap_status IS NULL)
            ORDER BY company_key
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def update_website(self, company_key: str, website: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET website = ?, gmap_status = 'done', updated_at = ? WHERE company_key = ?",
                (website, _now_text(), company_key),
            )
        )

    def mark_gmap_done(self, company_key: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET gmap_status = 'done', updated_at = ? WHERE company_key = ?",
                (_now_text(), company_key),
            )
        )

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_key, company_name, address, website, representative
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status = 'pending' OR email_status IS NULL)
            ORDER BY company_key
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def save_email_result(self, company_key: str, emails: list[str], representative: str = "") -> None:
        email_str = "; ".join(_dedupe_emails(emails))

        def _action(conn: sqlite3.Connection) -> None:
            if representative:
                conn.execute(
                    """
                    UPDATE companies
                    SET emails = ?, email_status = 'done', representative = ?, updated_at = ?
                    WHERE company_key = ?
                    """,
                    (email_str, representative, _now_text(), company_key),
                )
                return
            conn.execute(
                "UPDATE companies SET emails = ?, email_status = 'done', updated_at = ? WHERE company_key = ?",
                (email_str, _now_text(), company_key),
            )

        self._run_write(_action)


def build_company_key(company_name: str, website: str, address: str) -> str:
    """按公司维度生成去重键。"""
    normalized_name = _normalize_company_name(company_name)
    if not normalized_name:
        return ""
    host = _website_host(website)
    if host:
        return f"{normalized_name}|{host}"
    normalized_address = _normalize_text(address)
    if normalized_address:
        return f"{normalized_name}|{normalized_address}"
    return normalized_name


def _normalize_company_name(value: str) -> str:
    return _normalize_text(value)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^0-9a-zA-Z一-龥ぁ-んァ-ヴー]+", "", str(value or "").strip().lower())


def _website_host(website: str) -> str:
    parsed = urlparse(str(website or "").strip())
    return parsed.netloc.lower().lstrip("www.").rstrip("/") if parsed.netloc else ""


def _dedupe_emails(emails: list[str]) -> list[str]:
    result: list[str] = []
    for email in emails:
        clean = str(email or "").strip().lower()
        if clean and clean not in result:
            result.append(clean)
    return result
