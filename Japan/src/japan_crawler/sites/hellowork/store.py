"""hellowork 站点 SQLite 存储。

表结构：
  prefs       — 47 都道府県及其求人件数
  companies   — 企业信息（法人番号去重）
  checkpoints — 各都道府県的爬取进度
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_REP_ONLY_QUEUE_MIGRATION_VERSION = 20260408


class HelloworkStore:
    """线程安全的 hellowork 数据存储。"""

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
        """写入时在锁冲突场景自动重试，避免并发写导致任务失败。"""
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
        import time as _time
        for _attempt in range(5):
            try:
                conn = self._conn()
                break
            except sqlite3.OperationalError:
                _time.sleep(1)
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
                employees       TEXT DEFAULT '',
                capital         TEXT DEFAULT '',
                founded_year    TEXT DEFAULT '',
                corp_number     TEXT DEFAULT '',
                detail_url      TEXT DEFAULT '',
                emails          TEXT DEFAULT '',
                email_status    TEXT DEFAULT 'pending',
                UNIQUE(corp_number),
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

        return self._run_write(_action)

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

    # ── 企业管理 ──

    def upsert_company(self, pref_code: str, company: dict[str, str]) -> bool:
        """插入或更新一家企业，返回是否成功插入新记录。

        去重优先级：法人番号 > (公司名+地址)。
        """
        conn = self._conn()
        name = company.get("company_name", "").strip()
        if not name:
            return False

        corp_num = company.get("corp_number", "").strip()
        addr = company.get("address", "").strip()

        try:
            def _action(inner_conn: sqlite3.Connection) -> bool:
                if corp_num:
                    # 有法人番号：按法人番号去重，冲突时更新非空字段
                    inner_conn.execute("""
                        INSERT INTO companies
                            (pref_code, company_name, representative, website, address,
                             industry, phone, employees, capital, founded_year,
                             corp_number, detail_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(corp_number) DO UPDATE SET
                            representative=CASE
                                WHEN excluded.representative NOT IN ('', '-')
                                THEN excluded.representative
                                ELSE companies.representative
                            END,
                            website=CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                            phone=CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                            employees=CASE WHEN excluded.employees != '' THEN excluded.employees ELSE companies.employees END,
                            capital=CASE WHEN excluded.capital != '' THEN excluded.capital ELSE companies.capital END
                    """, (
                        pref_code, name,
                        company.get("representative", ""),
                        company.get("website", ""),
                        addr,
                        company.get("industry", ""),
                        company.get("phone", ""),
                        company.get("employees", ""),
                        company.get("capital", ""),
                        company.get("founded_year", ""),
                        corp_num,
                        company.get("detail_url", ""),
                    ))
                else:
                    # 无法人番号：按 (公司名+地址) 去重
                    inner_conn.execute("""
                        INSERT INTO companies
                            (pref_code, company_name, representative, website, address,
                             industry, phone, employees, capital, founded_year,
                             corp_number, detail_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(company_name, address) DO UPDATE SET
                            representative=CASE
                                WHEN excluded.representative NOT IN ('', '-')
                                THEN excluded.representative
                                ELSE companies.representative
                            END,
                            website=CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END
                    """, (
                        pref_code, name,
                        company.get("representative", ""),
                        company.get("website", ""),
                        addr,
                        company.get("industry", ""),
                        company.get("phone", ""),
                        company.get("employees", ""),
                        company.get("capital", ""),
                        company.get("founded_year", ""),
                        corp_num,
                        company.get("detail_url", ""),
                    ))
                return True

            return self._run_write(_action)
        except sqlite3.IntegrityError:
            return False

    def upsert_companies_batch(self, pref_code: str, companies: list[dict[str, str]]) -> int:
        """批量入库，返回成功数。"""
        inserted = 0
        for comp in companies:
            if self.upsert_company(pref_code, comp):
                inserted += 1
        return inserted

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return row["cnt"] if row else 0

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        """获取有官网但未提取邮箱的企业。"""
        conn = self._conn()
        sql = """
            SELECT id, company_name, address, website, representative
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status IN ('pending', 'rep_pending') OR email_status IS NULL)
            ORDER BY id
        """
        if limit > 0:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def save_email_result(self, company_id: int, emails: str, representative: str = "") -> None:
        """保存邮箱提取结果。"""
        def _action(conn: sqlite3.Connection) -> None:
            if representative:
                conn.execute(
                    "UPDATE companies SET emails = ?, email_status = 'done', representative = ? WHERE id = ?",
                    (emails, representative, company_id),
                )
            else:
                conn.execute(
                    "UPDATE companies SET emails = ?, email_status = 'done' WHERE id = ?",
                    (emails, company_id),
                )

        self._run_write(_action)

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute("""
            SELECT company_name, representative, website, address,
                   industry, phone, employees, capital, founded_year,
                   corp_number, detail_url, emails
            FROM companies ORDER BY id
        """).fetchall()
        return [dict(r) for r in rows]

    # ── 断点管理 ──

    def update_checkpoint(
        self, pref_code: str, last_page: int, total_pages: int,
        status: str = "running",
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO checkpoints (pref_code, last_page, total_pages, status)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(pref_code) DO UPDATE SET last_page=excluded.last_page,
                   total_pages=excluded.total_pages, status=excluded.status""",
                (pref_code, last_page, total_pages, status),
            )

        self._run_write(_action)

    def get_checkpoint(self, pref_code: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, total_pages, status FROM checkpoints WHERE pref_code = ?",
            (pref_code,),
        ).fetchone()
        return dict(row) if row else None
