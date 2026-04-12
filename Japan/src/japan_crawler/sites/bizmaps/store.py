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
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from shared.oldiron_core.fc_email.normalization import analyze_email_set
from shared.oldiron_core.fc_email.normalization import join_emails
from shared.oldiron_core.google_maps.client import _is_blocked_host as _gmap_is_blocked_host
from shared.oldiron_core.google_maps.client import _normalize_url as _gmap_normalize_url

_REP_ONLY_QUEUE_MIGRATION_VERSION = 20260408
_SOURCE_DIRTY_HOST_FRAGMENTS = (
    "booking.com",
    "carsensor.net",
    "getyourguide.com",
    "giatamedia.com",
    "goo-net.com",
)


def _sanitize_source_website(url: str) -> str:
    normalized = _gmap_normalize_url(url)
    if not normalized:
        return ""
    lowered_url = normalized.lower()
    host = urlparse(normalized).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return ""
    if _gmap_is_blocked_host(host):
        return ""
    if any(fragment in host for fragment in _SOURCE_DIRTY_HOST_FRAGMENTS):
        return ""
    if any(fragment in lowered_url for fragment in _SOURCE_DIRTY_HOST_FRAGMENTS):
        return ""
    return normalized


class BizmapsStore:
    """线程安全的 bizmaps 数据存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._max_write_retries = 6
        self._init_tables()
        self.ensure_email_columns()
        self.repair_email_quality()
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
        for _attempt in range(5):
            try:
                conn = self._conn()
                break
            except sqlite3.OperationalError:
                time.sleep(1)
        else:
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
                gmap_status     TEXT DEFAULT 'pending',
                UNIQUE(company_name, address)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                pref_code   TEXT PRIMARY KEY,
                last_page   INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                status      TEXT DEFAULT 'pending',
                last_ph     TEXT DEFAULT ''
            );
        """)
        # 兼容旧库：如果 checkpoints 表缺少 last_ph 列则补加
        try:
            conn.execute("SELECT last_ph FROM checkpoints LIMIT 1")
        except sqlite3.OperationalError:
            try:
                conn.execute("ALTER TABLE checkpoints ADD COLUMN last_ph TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # 另一个线程已添加该列
        conn.commit()

    def ensure_email_columns(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            try:
                conn.execute("ALTER TABLE companies ADD COLUMN emails TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE companies ADD COLUMN email_status TEXT DEFAULT 'pending'")
            except sqlite3.OperationalError:
                pass

        self._run_write(_action)

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

    # ── 都道府県管理 ──

    def upsert_prefs(self, prefs: list[dict[str, Any]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            count = 0
            for p in prefs:
                conn.execute(
                    "INSERT INTO prefs (pref_code, name, total) VALUES (?, ?, ?)"
                    " ON CONFLICT(pref_code) DO UPDATE SET name=excluded.name, total=excluded.total",
                    (p["pref_code"], p["name"], p.get("total", 0)),
                )
                count += 1
            return count

        return int(self._run_write(_action) or 0)

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
            ORDER BY
                CASE COALESCE(cp.status, 'pending')
                    WHEN 'error' THEN 0
                    WHEN 'running' THEN 1
                    ELSE 2
                END,
                p.pref_code
        """).fetchall()
        return [dict(r) for r in rows]

    def get_prefs_by_status(self, status: str) -> list[dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT p.pref_code, p.name, p.total,
                   COALESCE(cp.last_page, 0) AS last_page,
                   COALESCE(cp.total_pages, 0) AS total_pages,
                   COALESCE(cp.status, 'pending') AS status
            FROM prefs p
            LEFT JOIN checkpoints cp ON cp.pref_code = p.pref_code
            WHERE COALESCE(cp.status, 'pending') = ?
            ORDER BY p.pref_code
            """,
            (status,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_prefs(self) -> list[dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute("SELECT pref_code, name, total FROM prefs ORDER BY pref_code").fetchall()
        return [dict(r) for r in rows]

    # ── 公司管理 ──

    def upsert_companies(self, pref_code: str, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for comp in companies:
                name = comp.get("company_name", "").strip()
                addr = comp.get("address", "").strip()
                website = _sanitize_source_website(comp.get("website", ""))
                if not name:
                    continue
                try:
                    conn.execute(
                        """INSERT INTO companies
                           (pref_code, company_name, representative, website, address,
                            industry, phone, founded_year, capital, detail_url)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(company_name, address) DO UPDATE SET
                               representative=CASE
                                   WHEN excluded.representative NOT IN ('', '-')
                                   THEN excluded.representative
                                   ELSE companies.representative
                               END,
                               website=CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                               phone=CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                               detail_url=CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END
                        """,
                        (
                            pref_code, name,
                            comp.get("representative", ""),
                            website,
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
            return inserted

        return int(self._run_write(_action) or 0)

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return row["cnt"] if row else 0

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute("""
            SELECT company_name, representative, website, address,
                   industry, phone, founded_year, capital, detail_url,
                   COALESCE(emails, '') AS emails
            FROM companies ORDER BY id
        """).fetchall()
        return [dict(r) for r in rows]

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_name, address, website, representative
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status IN ('pending', 'rep_pending') OR email_status IS NULL)
            ORDER BY id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def save_email_result(self, company_name: str, address: str, website: str, emails: list[str], representative: str = "") -> None:
        normalized = join_emails(emails)

        def _action(conn: sqlite3.Connection) -> None:
            if representative:
                conn.execute(
                    """
                    UPDATE companies
                    SET emails = ?, email_status = 'done', representative = ?
                    WHERE company_name = ? AND address = ? AND website = ?
                    """,
                    (normalized, representative, company_name, address, website),
                )
                return
            conn.execute(
                """
                UPDATE companies
                SET emails = ?, email_status = 'done'
                WHERE company_name = ? AND address = ? AND website = ?
                """,
                (normalized, company_name, address, website),
            )

        self._run_write(_action)

    def repair_email_quality(self) -> None:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT id, website, COALESCE(emails, '') AS emails
            FROM companies
            WHERE COALESCE(emails, '') != ''
            """
        ).fetchall()
        if not rows:
            return

        updates: list[tuple[str, str, int]] = []
        for row in rows:
            analysis = analyze_email_set(str(row["website"] or ""), str(row["emails"] or ""))
            if analysis.suspicious_directory_like:
                updates.append(("", "pending", int(row["id"])))
                continue
            normalized = "; ".join(analysis.emails)
            if normalized != str(row["emails"] or ""):
                updates.append((normalized, "", int(row["id"])))

        if not updates:
            return

        def _action(inner_conn: sqlite3.Connection) -> None:
            for emails, email_status, row_id in updates:
                if email_status:
                    inner_conn.execute(
                        "UPDATE companies SET emails = ?, email_status = ? WHERE id = ?",
                        (emails, email_status, row_id),
                    )
                    continue
                inner_conn.execute(
                    "UPDATE companies SET emails = ? WHERE id = ?",
                    (emails, row_id),
                )

        self._run_write(_action)

    # ── 断点管理 ──

    def update_checkpoint(
        self, pref_code: str, last_page: int, total_pages: int,
        status: str = "running", last_ph: str = "",
    ) -> None:
        """更新采集断点，last_ph 保存当前页对应的下一页 ph token。"""
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO checkpoints (pref_code, last_page, total_pages, status, last_ph)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(pref_code) DO UPDATE SET last_page=excluded.last_page,
                   total_pages=excluded.total_pages, status=excluded.status,
                   last_ph=excluded.last_ph""",
                (pref_code, last_page, total_pages, status, last_ph),
            )

        self._run_write(_action)

    def get_checkpoint(self, pref_code: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, total_pages, status, COALESCE(last_ph, '') AS last_ph FROM checkpoints WHERE pref_code = ?",
            (pref_code,),
        ).fetchone()
        return dict(row) if row else None
