"""阿联酋通用 SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from oldiron_core.fc_email.normalization import split_emails


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_company_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _merge_semicolon_values(*values: str) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for clean in split_emails(str(raw_value or "")):
            lowered = clean.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(clean)
    return ";".join(merged)


class UaeCompanyStore:
    """站点内按公司名去重的通用存储。"""

    _INIT_LOCK = threading.Lock()
    _INITIALIZED_DBS: set[str] = set()

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._max_write_retries = 6
        self._ensure_db_ready()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA synchronous=NORMAL")
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

    def _ensure_db_ready(self) -> None:
        db_key = str(self._db_path.resolve())
        if db_key in self._INITIALIZED_DBS:
            return
        with self._INIT_LOCK:
            if db_key in self._INITIALIZED_DBS:
                return
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            try:
                conn.execute("PRAGMA busy_timeout=60000")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS companies (
                        record_id TEXT PRIMARY KEY,
                        company_name TEXT NOT NULL,
                        source_pdl_id TEXT DEFAULT '',
                        p1_status TEXT DEFAULT 'pending',
                        representative_p1 TEXT DEFAULT '',
                        representative_p3 TEXT DEFAULT '',
                        representative_final TEXT DEFAULT '',
                        website TEXT DEFAULT '',
                        address TEXT DEFAULT '',
                        phone TEXT DEFAULT '',
                        emails TEXT DEFAULT '',
                        detail_url TEXT DEFAULT '',
                        summary TEXT DEFAULT '',
                        evidence_url TEXT DEFAULT '',
                        gmap_status TEXT DEFAULT 'pending',
                        email_status TEXT DEFAULT 'pending',
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        scope TEXT PRIMARY KEY,
                        last_page INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',
                        updated_at TEXT NOT NULL
                    );
                    """
                )
                self._ensure_column(conn, "companies", "source_pdl_id", "TEXT DEFAULT ''")
                self._ensure_column(conn, "companies", "p1_status", "TEXT DEFAULT 'pending'")
                conn.commit()
            finally:
                conn.close()
            self._INITIALIZED_DBS.add(db_key)

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column in columns:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for company in companies:
                company_name = str(company.get("company_name", "")).strip()
                record_id = _normalize_company_key(company_name)
                if not record_id or not company_name:
                    continue
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT INTO companies (
                        record_id, company_name, source_pdl_id, p1_status, representative_p1, representative_final,
                        website, address, phone, emails, detail_url, summary,
                        evidence_url, gmap_status, email_status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(record_id) DO UPDATE SET
                        company_name = excluded.company_name,
                        source_pdl_id = CASE
                            WHEN excluded.source_pdl_id != '' THEN excluded.source_pdl_id
                            ELSE companies.source_pdl_id
                        END,
                        p1_status = CASE
                            WHEN excluded.representative_p1 != '' THEN 'done'
                            WHEN excluded.p1_status = 'pending' THEN 'pending'
                            WHEN companies.p1_status = 'done' THEN 'done'
                            ELSE excluded.p1_status
                        END,
                        representative_p1 = CASE
                            WHEN excluded.representative_p1 != '' THEN excluded.representative_p1
                            ELSE companies.representative_p1
                        END,
                        representative_final = CASE
                            WHEN excluded.representative_final != '' THEN excluded.representative_final
                            ELSE companies.representative_final
                        END,
                        website = CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                        address = CASE WHEN excluded.address != '' THEN excluded.address ELSE companies.address END,
                        phone = CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                        emails = CASE
                            WHEN companies.emails != '' OR excluded.emails != ''
                            THEN trim(
                                CASE
                                    WHEN companies.emails = '' THEN excluded.emails
                                    WHEN excluded.emails = '' THEN companies.emails
                                    ELSE companies.emails || ';' || excluded.emails
                                END,
                                ';'
                            )
                            ELSE ''
                        END,
                        detail_url = CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END,
                        summary = CASE WHEN excluded.summary != '' THEN excluded.summary ELSE companies.summary END,
                        evidence_url = CASE WHEN excluded.evidence_url != '' THEN excluded.evidence_url ELSE companies.evidence_url END,
                        gmap_status = CASE
                            WHEN excluded.website != '' THEN 'done'
                            ELSE companies.gmap_status
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        record_id,
                        company_name,
                        str(company.get("source_pdl_id", "")).strip(),
                        str(company.get("p1_status", "pending")).strip() or "pending",
                        str(company.get("representative_p1", "")).strip(),
                        str(company.get("representative_final", "")).strip(),
                        str(company.get("website", "")).strip(),
                        str(company.get("address", "")).strip(),
                        str(company.get("phone", "")).strip(),
                        _merge_semicolon_values(str(company.get("emails", "")).strip()),
                        str(company.get("detail_url", "")).strip(),
                        str(company.get("summary", "")).strip(),
                        str(company.get("evidence_url", "")).strip(),
                        "done" if str(company.get("website", "")).strip() else "pending",
                        "pending",
                        _now_text(),
                    ),
                )
                inserted += int(conn.total_changes > before)
            return inserted

        return int(self._run_write(_action) or 0)

    def get_checkpoint(self, scope: str = "list") -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, status FROM checkpoints WHERE scope = ?",
            (scope,),
        ).fetchone()
        return dict(row) if row else None

    def update_checkpoint(self, scope: str, last_page: int, status: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                INSERT INTO checkpoints (scope, last_page, status, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    last_page = excluded.last_page,
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                (scope, int(last_page), status, _now_text()),
            )
        )

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return int(row["cnt"] if row else 0)

    def get_gmap_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT record_id, company_name, address
            FROM companies
            WHERE (website = '' OR website IS NULL)
              AND (gmap_status = 'pending' OR gmap_status IS NULL)
            ORDER BY record_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        return [dict(row) for row in conn.execute(sql).fetchall()]

    def save_gmap_result(self, record_id: str, website: str, phone: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                UPDATE companies
                SET website = CASE WHEN ? != '' THEN ? ELSE website END,
                    phone = CASE WHEN ? != '' THEN ? ELSE phone END,
                    gmap_status = 'done',
                    updated_at = ?
                WHERE record_id = ?
                """,
                (website, website, phone, phone, _now_text(), record_id),
            )
        )

    def mark_gmap_done(self, record_id: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET gmap_status = 'done', updated_at = ? WHERE record_id = ?",
                (_now_text(), record_id),
            )
        )

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT record_id, company_name, representative_p1, website, emails
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status = 'pending' OR email_status IS NULL)
            ORDER BY record_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        return [dict(row) for row in conn.execute(sql).fetchall()]

    def get_p1_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT record_id, company_name, source_pdl_id, representative_p1
            FROM companies
            WHERE source_pdl_id != '' AND source_pdl_id IS NOT NULL
              AND representative_p1 = ''
              AND (p1_status = 'pending' OR p1_status IS NULL)
            ORDER BY record_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        return [dict(row) for row in conn.execute(sql).fetchall()]

    def finalize_pending_p1(self) -> int:
        """把不再需要站内详情回补的遗留 P1 任务直接收口。"""
        def _action(conn: sqlite3.Connection) -> int:
            cursor = conn.execute(
                """
                UPDATE companies
                SET p1_status = 'done',
                    updated_at = ?
                WHERE source_pdl_id != '' AND source_pdl_id IS NOT NULL
                  AND (p1_status = 'pending' OR p1_status IS NULL)
                """,
                (_now_text(),),
            )
            return int(cursor.rowcount or 0)

        return int(self._run_write(_action) or 0)

    def save_p1_result(self, record_id: str, representative_p1: str, representative_final: str, status: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                UPDATE companies
                SET representative_p1 = CASE WHEN ? != '' THEN ? ELSE representative_p1 END,
                    representative_final = CASE WHEN ? != '' THEN ? ELSE representative_final END,
                    p1_status = ?,
                    updated_at = ?
                WHERE record_id = ?
                """,
                (
                    representative_p1,
                    representative_p1,
                    representative_final,
                    representative_final,
                    status,
                    _now_text(),
                    record_id,
                ),
            )
        )

    def save_email_result(
        self,
        record_id: str,
        emails: list[str],
        representative_p3: str,
        representative_final: str,
        evidence_url: str,
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            current = conn.execute(
                "SELECT emails FROM companies WHERE record_id = ?",
                (record_id,),
            ).fetchone()
            merged_emails = _merge_semicolon_values(
                str(current["emails"] if current else "").strip(),
                ";".join(emails),
            )
            conn.execute(
                """
                UPDATE companies
                SET emails = ?,
                    representative_p3 = ?,
                    representative_final = ?,
                    evidence_url = CASE WHEN ? != '' THEN ? ELSE evidence_url END,
                    email_status = 'done',
                    updated_at = ?
                WHERE record_id = ?
                """,
                (
                    merged_emails,
                    representative_p3,
                    representative_final,
                    evidence_url,
                    evidence_url,
                    _now_text(),
                    record_id,
                ),
            )

        self._run_write(_action)

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT company_name, representative_final AS representative, emails,
                   website, phone, evidence_url
            FROM companies
            ORDER BY company_name
            """
        ).fetchall()
        return [dict(row) for row in rows]
