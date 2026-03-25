"""xlsximport SQLite 存储。

表结构：
  companies — 从 xlsx 导入的公司（官网+邮箱已有，公司名+代表人待补全）
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any


class XlsxImportStore:
    """线程安全的 xlsximport 数据存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._init_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def _init_tables(self) -> None:
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                website         TEXT NOT NULL,
                email           TEXT DEFAULT '',
                company_name    TEXT DEFAULT '',
                representative  TEXT DEFAULT '',
                status          TEXT DEFAULT 'pending',
                UNIQUE(website, email)
            );
        """)
        conn.commit()

    # ── 数据导入 ──

    def import_from_rows(self, rows: list[dict[str, str]]) -> int:
        """从 xlsx 解析后的行批量导入，跳过已存在的。"""
        conn = self._conn()
        inserted = 0
        for row in rows:
            website = (row.get("website") or "").strip()
            email = (row.get("email") or "").strip()
            if not website:
                continue
            try:
                conn.execute(
                    "INSERT INTO companies (website, email) VALUES (?, ?)"
                    " ON CONFLICT(website, email) DO NOTHING",
                    (website, email),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        return inserted

    def get_total_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return row["cnt"] if row else 0

    def get_pending_companies(self) -> list[dict[str, Any]]:
        """获取待处理的公司列表（status=pending）。"""
        conn = self._conn()
        rows = conn.execute("""
            SELECT id, website, email, company_name, representative
            FROM companies
            WHERE status = 'pending'
            ORDER BY id
        """).fetchall()
        return [dict(r) for r in rows]

    def update_result(
        self, company_id: int, company_name: str, representative: str, status: str = "done"
    ) -> None:
        """更新单条记录的公司名和代表人。"""
        conn = self._conn()
        conn.execute(
            "UPDATE companies SET company_name = ?, representative = ?, status = ? WHERE id = ?",
            (company_name, representative, status, company_id),
        )
        conn.commit()

    def export_all(self) -> list[dict[str, str]]:
        """导出全部数据。"""
        conn = self._conn()
        rows = conn.execute("""
            SELECT website, email, company_name, representative, status
            FROM companies ORDER BY id
        """).fetchall()
        return [dict(r) for r in rows]
