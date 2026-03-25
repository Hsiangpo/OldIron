"""bizmaps 站点 SQLite 存储。

表结构：
  prefs       — 都道府県列表（47个）
  companies   — 公司基本信息（列表页解析）
  checkpoints — 各都道府県的爬取进度
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class BizmapsStore:
    """线程安全的 bizmaps 数据存储。"""

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
            CREATE TABLE IF NOT EXISTS prefs (
                pref_code   TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                total       INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS companies (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pref_code       TEXT NOT NULL,
                company_name    TEXT NOT NULL,
                representative  TEXT DEFAULT '',
                website         TEXT DEFAULT '',
                address         TEXT DEFAULT '',
                industry        TEXT DEFAULT '',
                phone           TEXT DEFAULT '',
                founded_year    TEXT DEFAULT '',
                capital         TEXT DEFAULT '',
                detail_url      TEXT DEFAULT '',
                UNIQUE(company_name, address)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                pref_code   TEXT PRIMARY KEY,
                last_page   INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                status      TEXT DEFAULT 'pending'
            );
        """)
        conn.commit()

    # ── 都道府県管理 ──

    def upsert_prefs(self, prefs: list[dict[str, Any]]) -> int:
        conn = self._conn()
        count = 0
        for p in prefs:
            conn.execute(
                "INSERT INTO prefs (pref_code, name, total) VALUES (?, ?, ?)"
                " ON CONFLICT(pref_code) DO UPDATE SET name=excluded.name, total=excluded.total",
                (p["pref_code"], p["name"], p.get("total", 0)),
            )
            count += 1
        conn.commit()
        return count

    def get_pending_prefs(self) -> list[dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute("""
            SELECT p.pref_code, p.name, p.total,
                   COALESCE(cp.last_page, 0) AS last_page,
                   COALESCE(cp.total_pages, 0) AS total_pages,
                   COALESCE(cp.status, 'pending') AS status
            FROM prefs p
            LEFT JOIN checkpoints cp ON cp.pref_code = p.pref_code
            WHERE COALESCE(cp.status, 'pending') != 'done'
            ORDER BY p.pref_code
        """).fetchall()
        return [dict(r) for r in rows]

    def get_all_prefs(self) -> list[dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute("SELECT pref_code, name, total FROM prefs ORDER BY pref_code").fetchall()
        return [dict(r) for r in rows]

    # ── 公司管理 ──

    def upsert_companies(self, pref_code: str, companies: list[dict[str, str]]) -> int:
        conn = self._conn()
        inserted = 0
        for comp in companies:
            name = comp.get("company_name", "").strip()
            addr = comp.get("address", "").strip()
            if not name:
                continue
            try:
                conn.execute(
                    """INSERT INTO companies
                       (pref_code, company_name, representative, website, address,
                        industry, phone, founded_year, capital, detail_url)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(company_name, address) DO UPDATE SET
                           representative=CASE WHEN excluded.representative != '' THEN excluded.representative ELSE companies.representative END,
                           website=CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                           phone=CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                           detail_url=CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END
                    """,
                    (
                        pref_code, name,
                        comp.get("representative", ""),
                        comp.get("website", ""),
                        addr,
                        comp.get("industry", ""),
                        comp.get("phone", ""),
                        comp.get("founded_year", ""),
                        comp.get("capital", ""),
                        comp.get("detail_url", ""),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        return inserted

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return row["cnt"] if row else 0

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute("""
            SELECT company_name, representative, website, address,
                   industry, phone, founded_year, capital, detail_url
            FROM companies ORDER BY id
        """).fetchall()
        return [dict(r) for r in rows]

    # ── 断点管理 ──

    def update_checkpoint(self, pref_code: str, last_page: int, total_pages: int, status: str = "running") -> None:
        conn = self._conn()
        conn.execute(
            """INSERT INTO checkpoints (pref_code, last_page, total_pages, status) VALUES (?, ?, ?, ?)
               ON CONFLICT(pref_code) DO UPDATE SET last_page=excluded.last_page,
               total_pages=excluded.total_pages, status=excluded.status""",
            (pref_code, last_page, total_pages, status),
        )
        conn.commit()

    def get_checkpoint(self, pref_code: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, total_pages, status FROM checkpoints WHERE pref_code = ?",
            (pref_code,),
        ).fetchone()
        return dict(row) if row else None
