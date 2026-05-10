"""CNPJ Biz 站点 SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _merge_emails(*values: str) -> str:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values:
        for item in str(raw or "").split(";"):
            email = item.strip().lower()
            if not email or "@" not in email or email in seen:
                continue
            seen.add(email)
            result.append(email)
    return "; ".join(result)


@dataclass(slots=True)
class CnpjBizListTask:
    page_url: str
    depth: int
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class CnpjBizDetailTask:
    cnpj: str
    detail_url: str
    company_name: str
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class CnpjBizProgress:
    list_pending: int
    list_running: int
    detail_pending: int
    detail_running: int
    companies_total: int
    final_total: int
    list_done: int


class CnpjBizStore:
    """线程安全的 CNPJ Biz 存储。"""

    _MAX_RETRIES = 3
    _WRITE_MUTEX = threading.RLock()

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._shared_conn: sqlite3.Connection | None = None
        self._init_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = self._shared_conn
        if conn is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self._db_path), timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._shared_conn = conn
        return conn

    def _init_tables(self) -> None:
        with self._WRITE_MUTEX:
            conn = self._conn()
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS list_queue (
                    page_url TEXT PRIMARY KEY,
                    depth INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS detail_queue (
                    cnpj TEXT PRIMARY KEY,
                    detail_url TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS companies (
                    cnpj TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    trade_name TEXT NOT NULL DEFAULT '',
                    status_text TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    opened_at TEXT NOT NULL DEFAULT '',
                    detail_url TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    emails TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    representative_candidates_json TEXT NOT NULL DEFAULT '[]',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    detail_status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS final_companies (
                    cnpj TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    emails TEXT NOT NULL DEFAULT '',
                    website TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                """
            )
            conn.commit()

    def close(self) -> None:
        with self._WRITE_MUTEX:
            if self._shared_conn is not None:
                self._shared_conn.close()
                self._shared_conn = None

    def seed_start_page(self, page_url: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO list_queue (page_url, depth, status, retries, updated_at)
                VALUES (?, 0, 'pending', 0, ?)
                ON CONFLICT(page_url) DO NOTHING
                """,
                (page_url, _now_text()),
            )

        self._run_write(_action)

    def requeue_running_tasks(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            now_text = _now_text()
            total = 0
            total += conn.execute(
                "UPDATE list_queue SET status = 'pending', updated_at = ? WHERE status = 'running'",
                (now_text,),
            ).rowcount
            total += conn.execute(
                "UPDATE detail_queue SET status = 'pending', updated_at = ? WHERE status = 'running'",
                (now_text,),
            ).rowcount
            return total

        return self._run_write(_action)

    def requeue_failed_tasks(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            now_text = _now_text()
            total = 0
            total += conn.execute(
                "UPDATE list_queue SET status = 'pending', retries = 0, updated_at = ? WHERE status = 'failed'",
                (now_text,),
            ).rowcount
            total += conn.execute(
                "UPDATE detail_queue SET status = 'pending', retries = 0, updated_at = ? WHERE status = 'failed'",
                (now_text,),
            ).rowcount
            return total

        return self._run_write(_action)

    def claim_list_task(self) -> CnpjBizListTask | None:
        def _action(conn: sqlite3.Connection) -> CnpjBizListTask | None:
            row = conn.execute(
                """
                SELECT page_url, depth, status, retries, updated_at
                FROM list_queue
                WHERE status = 'pending'
                ORDER BY depth, updated_at, page_url
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE list_queue SET status = 'running', updated_at = ? WHERE page_url = ? AND status = 'pending'",
                (_now_text(), row["page_url"]),
            ).rowcount
            if updated != 1:
                return None
            return CnpjBizListTask(**dict(row))

        return self._run_write(_action)

    def complete_list_task(self, page_url: str, depth: int, records: list[dict[str, str]], next_url: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            now_text = _now_text()
            for record in records:
                conn.execute(
                    """
                    INSERT INTO companies (
                        cnpj, company_name, status_text, city, region, opened_at, detail_url, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cnpj) DO UPDATE SET
                        company_name = excluded.company_name,
                        status_text = excluded.status_text,
                        city = excluded.city,
                        region = excluded.region,
                        opened_at = excluded.opened_at,
                        detail_url = excluded.detail_url,
                        updated_at = excluded.updated_at
                    """,
                    (
                        record.get("cnpj", ""),
                        record.get("company_name", ""),
                        record.get("status_text", ""),
                        record.get("city", ""),
                        record.get("region", ""),
                        record.get("opened_at", ""),
                        record.get("detail_url", ""),
                        now_text,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO detail_queue (cnpj, detail_url, company_name, status, retries, updated_at)
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    ON CONFLICT(cnpj) DO UPDATE SET
                        detail_url = excluded.detail_url,
                        company_name = excluded.company_name
                    """,
                    (
                        record.get("cnpj", ""),
                        record.get("detail_url", ""),
                        record.get("company_name", ""),
                        now_text,
                    ),
                )
            conn.execute(
                "UPDATE list_queue SET status = 'done', updated_at = ? WHERE page_url = ?",
                (now_text, page_url),
            )
            if next_url:
                conn.execute(
                    """
                    INSERT INTO list_queue (page_url, depth, status, retries, updated_at)
                    VALUES (?, ?, 'pending', 0, ?)
                    ON CONFLICT(page_url) DO NOTHING
                    """,
                    (next_url, depth + 1, now_text),
                )

        self._run_write(_action)

    def defer_list_task(self, page_url: str) -> None:
        self._retry_task("list_queue", "page_url", page_url)

    def claim_detail_task(self) -> CnpjBizDetailTask | None:
        def _action(conn: sqlite3.Connection) -> CnpjBizDetailTask | None:
            row = conn.execute(
                """
                SELECT cnpj, detail_url, company_name, status, retries, updated_at
                FROM detail_queue
                WHERE status = 'pending'
                ORDER BY updated_at, cnpj
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE cnpj = ? AND status = 'pending'",
                (_now_text(), row["cnpj"]),
            ).rowcount
            if updated != 1:
                return None
            return CnpjBizDetailTask(**dict(row))

        return self._run_write(_action)

    def complete_detail_task(
        self,
        *,
        cnpj: str,
        company_name: str,
        trade_name: str,
        representative: str,
        representative_candidates_json: str,
        emails: str,
        phone: str,
        address: str,
        city: str,
        region: str,
        status_text: str,
        opened_at: str,
        evidence_url: str,
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            now_text = _now_text()
            current = conn.execute(
                """
                SELECT company_name, trade_name, representative, emails, phone, address, city, region, status_text, opened_at
                FROM companies
                WHERE cnpj = ?
                """,
                (cnpj,),
            ).fetchone()
            current_company_name = str(current["company_name"] or "").strip() if current else ""
            current_trade_name = str(current["trade_name"] or "").strip() if current else ""
            current_rep = str(current["representative"] or "").strip() if current else ""
            current_emails = str(current["emails"] or "").strip() if current else ""
            current_phone = str(current["phone"] or "").strip() if current else ""
            current_address = str(current["address"] or "").strip() if current else ""
            current_city = str(current["city"] or "").strip() if current else ""
            current_region = str(current["region"] or "").strip() if current else ""
            current_status = str(current["status_text"] or "").strip() if current else ""
            current_opened_at = str(current["opened_at"] or "").strip() if current else ""
            existing_final = conn.execute(
                """
                SELECT company_name, representative, emails, phone, address, evidence_url
                FROM final_companies
                WHERE cnpj = ?
                """,
                (cnpj,),
            ).fetchone()
            merged_company_name = str(company_name or current_company_name).strip()
            merged_trade_name = str(trade_name or current_trade_name).strip()
            merged_rep = str(representative or current_rep).strip()
            merged_emails = _merge_emails(
                current_emails,
                emails,
                str(existing_final["emails"] or "") if existing_final else "",
            )
            merged_phone = str(phone or current_phone).strip()
            merged_address = str(address or current_address).strip()
            merged_city = str(city or current_city).strip()
            merged_region = str(region or current_region).strip()
            merged_status = str(status_text or current_status).strip()
            merged_opened_at = str(opened_at or current_opened_at).strip()
            merged_evidence = str(evidence_url or (str(existing_final["evidence_url"] or "").strip() if existing_final else "")).strip()
            conn.execute(
                """
                UPDATE companies
                SET company_name = ?, trade_name = ?, representative = ?, representative_candidates_json = ?, emails = ?,
                    phone = ?, address = ?, city = ?, region = ?, status_text = ?, opened_at = ?,
                    evidence_url = ?, detail_status = 'done', updated_at = ?
                WHERE cnpj = ?
                """,
                (
                    merged_company_name,
                    merged_trade_name,
                    merged_rep,
                    representative_candidates_json,
                    merged_emails,
                    merged_phone,
                    merged_address,
                    merged_city,
                    merged_region,
                    merged_status,
                    merged_opened_at,
                    merged_evidence,
                    now_text,
                    cnpj,
                ),
            )
            if merged_company_name and merged_rep and merged_emails:
                conn.execute(
                    """
                    INSERT INTO final_companies (cnpj, company_name, representative, emails, website, phone, address, evidence_url, updated_at)
                    VALUES (?, ?, ?, ?, '', ?, ?, ?, ?)
                    ON CONFLICT(cnpj) DO UPDATE SET
                        company_name = excluded.company_name,
                        representative = excluded.representative,
                        emails = excluded.emails,
                        phone = excluded.phone,
                        address = excluded.address,
                        evidence_url = excluded.evidence_url,
                        updated_at = excluded.updated_at
                    """,
                    (
                        cnpj,
                        merged_company_name,
                        merged_rep,
                        merged_emails,
                        merged_phone,
                        merged_address,
                        merged_evidence,
                        now_text,
                    ),
                )
            conn.execute(
                "UPDATE detail_queue SET status = 'done', updated_at = ? WHERE cnpj = ?",
                (now_text, cnpj),
            )

        self._run_write(_action)

    def fail_detail_task(self, cnpj: str) -> None:
        self._retry_task("detail_queue", "cnpj", cnpj)

    def progress(self) -> CnpjBizProgress:
        with self._WRITE_MUTEX:
            conn = self._conn()
            return CnpjBizProgress(
                list_pending=self._count_where(conn, "list_queue", "status = 'pending'"),
                list_running=self._count_where(conn, "list_queue", "status = 'running'"),
                detail_pending=self._count_where(conn, "detail_queue", "status = 'pending'"),
                detail_running=self._count_where(conn, "detail_queue", "status = 'running'"),
                companies_total=self._count_where(conn, "companies", "1 = 1"),
                final_total=self._count_where(conn, "final_companies", "1 = 1"),
                list_done=self._count_where(conn, "list_queue", "status = 'done'"),
            )

    def _retry_task(self, table_name: str, key_column: str, key_value: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            row = conn.execute(
                f"SELECT retries FROM {table_name} WHERE {key_column} = ?",  # noqa: S608
                (key_value,),
            ).fetchone()
            if row is None:
                return
            retries = int(row["retries"] or 0) + 1
            status = "failed" if retries >= self._MAX_RETRIES else "pending"
            conn.execute(
                f"UPDATE {table_name} SET retries = ?, status = ?, updated_at = ? WHERE {key_column} = ?",  # noqa: S608
                (retries, status, _now_text(), key_value),
            )

        self._run_write(_action)

    def _count_where(self, conn: sqlite3.Connection, table_name: str, where_sql: str) -> int:
        row = conn.execute(
            f"SELECT COUNT(1) AS total FROM {table_name} WHERE {where_sql}"  # noqa: S608
        ).fetchone()
        return int(row["total"] or 0) if row else 0

    def _run_write(self, callback):
        with self._WRITE_MUTEX:
            conn = self._conn()
            result = callback(conn)
            conn.commit()
            return result
