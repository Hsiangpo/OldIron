"""Italy Wiza SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_company_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


class ItalyWizaStore:
    """站点内按公司名去重的最小存储。"""

    _INIT_LOCK = threading.Lock()
    _INITIALIZED_DBS: set[str] = set()

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
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
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS companies (
                        record_id TEXT PRIMARY KEY,
                        company_name TEXT NOT NULL,
                        website TEXT DEFAULT '',
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
                conn.commit()
            finally:
                conn.close()
            self._INITIALIZED_DBS.add(db_key)

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        conn = self._conn()
        inserted = 0
        for company in companies:
            company_name = str(company.get("company_name", "")).strip()
            record_id = _normalize_company_key(company_name)
            if not record_id or not company_name:
                continue
            before = conn.total_changes
            conn.execute(
                """
                INSERT INTO companies (record_id, company_name, website, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(record_id) DO UPDATE SET
                    company_name = excluded.company_name,
                    website = CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                    updated_at = excluded.updated_at
                """,
                (record_id, company_name, str(company.get("website", "")).strip(), _now_text()),
            )
            inserted += int(conn.total_changes > before)
        conn.commit()
        return inserted

    def update_checkpoint(self, scope: str, last_page: int, status: str) -> None:
        conn = self._conn()
        conn.execute(
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
        conn.commit()

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return int(row["cnt"] if row else 0)

    def export_websites(self) -> list[str]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT website
            FROM companies
            WHERE website != '' AND website IS NOT NULL
            ORDER BY website
            """
        ).fetchall()
        return sorted({str(row["website"] or "").strip() for row in rows if str(row["website"] or "").strip()})
