"""OpenWork 站点 SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


_REP_ONLY_QUEUE_MIGRATION_VERSION = 20260408


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class OpenworkStore:
    """线程安全的 OpenWork 数据存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._max_write_retries = 6
        self._init_tables()
        self._migrate_rep_only_queue()

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
                company_id TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                representative TEXT DEFAULT '',
                website TEXT DEFAULT '',
                address TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                detail_url TEXT DEFAULT '',
                emails TEXT DEFAULT '',
                gmap_status TEXT DEFAULT 'pending',
                email_status TEXT DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                scope TEXT PRIMARY KEY,
                last_page INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending'
            );
            """
        )
        conn.commit()

    def _migrate_rep_only_queue(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            current_version = int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)
            if current_version >= _REP_ONLY_QUEUE_MIGRATION_VERSION:
                return
            conn.execute(
                """
                UPDATE companies
                SET email_status = 'rep_pending'
                WHERE website != ''
                  AND website IS NOT NULL
                  AND trim(coalesce(representative, '')) = ''
                  AND email_status = 'done'
                """
            )
            conn.execute(f"PRAGMA user_version = {_REP_ONLY_QUEUE_MIGRATION_VERSION}")

        self._run_write(_action)

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for company in companies:
                company_id = str(company.get("company_id", "") or "").strip()
                company_name = str(company.get("company_name", "") or "").strip()
                if not company_id or not company_name:
                    continue
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT INTO companies (
                        company_id, company_name, representative, website,
                        address, industry, detail_url, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(company_id) DO UPDATE SET
                        company_name = excluded.company_name,
                        representative = CASE
                            WHEN excluded.representative NOT IN ('', '-')
                            THEN excluded.representative
                            ELSE companies.representative
                        END,
                        website = CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                        address = CASE WHEN excluded.address != '' THEN excluded.address ELSE companies.address END,
                        industry = CASE WHEN excluded.industry != '' THEN excluded.industry ELSE companies.industry END,
                        detail_url = CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        company_id,
                        company_name,
                        company.get("representative", ""),
                        company.get("website", ""),
                        company.get("address", ""),
                        company.get("industry", ""),
                        company.get("detail_url", ""),
                        _now_text(),
                    ),
                )
                inserted += int(conn.total_changes > before)
            return inserted

        return int(self._run_write(_action) or 0)

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return int(row["cnt"] if row else 0)

    def get_checkpoint(self, scope: str = "company_list") -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, total_pages, status FROM checkpoints WHERE scope = ?",
            (scope,),
        ).fetchone()
        return dict(row) if row else None

    def update_checkpoint(
        self,
        scope: str,
        last_page: int,
        total_pages: int,
        status: str = "running",
    ) -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                INSERT INTO checkpoints (scope, last_page, total_pages, status)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    last_page = excluded.last_page,
                    total_pages = excluded.total_pages,
                    status = excluded.status
                """,
                (scope, last_page, total_pages, status),
            )
        )

    def get_gmap_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_id, company_name, address
            FROM companies
            WHERE (website = '' OR website IS NULL)
              AND (gmap_status = 'pending' OR gmap_status IS NULL)
            ORDER BY company_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def update_website(self, company_id: str, website: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET website = ?, gmap_status = 'done', updated_at = ? WHERE company_id = ?",
                (website, _now_text(), company_id),
            )
        )

    def mark_gmap_done(self, company_id: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET gmap_status = 'done', updated_at = ? WHERE company_id = ?",
                (_now_text(), company_id),
            )
        )

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_id, company_name, address, website, representative
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status IN ('pending', 'rep_pending') OR email_status IS NULL)
            ORDER BY company_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def save_email_result(self, company_id: str, emails: list[str], representative: str = "") -> None:
        email_str = "; ".join(_dedupe_emails(emails))

        def _action(conn: sqlite3.Connection) -> None:
            if representative:
                conn.execute(
                    """
                    UPDATE companies
                    SET emails = ?, email_status = 'done', representative = ?, updated_at = ?
                    WHERE company_id = ?
                    """,
                    (email_str, representative, _now_text(), company_id),
                )
                return
            conn.execute(
                "UPDATE companies SET emails = ?, email_status = 'done', updated_at = ? WHERE company_id = ?",
                (email_str, _now_text(), company_id),
            )

        self._run_write(_action)

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT company_name, representative, website, address, industry, detail_url, emails
            FROM companies
            ORDER BY company_id
            """
        ).fetchall()
        return [dict(row) for row in rows]


def _dedupe_emails(emails: list[str]) -> list[str]:
    result: list[str] = []
    for email in emails:
        clean = str(email or "").strip()
        if clean and clean not in result:
            result.append(clean)
    return result

