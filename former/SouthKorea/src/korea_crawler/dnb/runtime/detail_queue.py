"""DNB 详情重试队列。"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class DetailTask:
    duns: str
    company_name_en_dnb: str
    company_name_url: str
    address: str
    city: str
    region: str
    country: str
    postal_code: str
    sales_revenue: str
    retries: int


class DetailQueueStore:
    """基于 sqlite 的详情重试队列。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._conn = self._new_connection()
        self._init_schema()
        self._repair_runtime_state()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout = 30000;")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS detail_queue (
                    duns TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running'",
                (now, now),
            )
            self._conn.commit()

    def sync_from_companies(self) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO detail_queue(duns, status, retries, next_run_at, last_error, updated_at)
                SELECT c.duns, 'pending', 0, ?, '', ?
                FROM companies c
                LEFT JOIN detail_queue q ON q.duns = c.duns
                WHERE c.detail_done = 0 AND q.duns IS NULL
                """,
                (now, now),
            )
            self._conn.execute(
                """
                UPDATE detail_queue
                SET status = 'pending', next_run_at = ?, updated_at = ?
                WHERE duns IN (SELECT duns FROM companies WHERE detail_done = 0) AND status = 'failed'
                """,
                (now, now),
            )
            self._conn.commit()

    def enqueue(self, duns: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO detail_queue(duns, status, retries, next_run_at, last_error, updated_at)
                VALUES(?, 'pending', 0, ?, '', ?)
                ON CONFLICT(duns) DO UPDATE SET
                    status = CASE WHEN detail_queue.status = 'done' THEN detail_queue.status ELSE 'pending' END,
                    next_run_at = CASE WHEN detail_queue.status = 'done' THEN detail_queue.next_run_at ELSE excluded.next_run_at END,
                    updated_at = excluded.updated_at,
                    last_error = CASE WHEN detail_queue.status = 'done' THEN detail_queue.last_error ELSE '' END
                """,
                (duns, now, now),
            )
            self._conn.commit()

    def claim(self) -> DetailTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT q.duns, q.retries, c.company_name_en_dnb, c.company_name_url, c.address, c.city,
                       c.region, c.country, c.postal_code, c.sales_revenue
                FROM detail_queue q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending' AND q.next_run_at <= ? AND c.detail_done = 0
                ORDER BY q.next_run_at ASC, q.updated_at ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE duns = ?",
                (now, str(row["duns"])),
            )
            self._conn.commit()
            return DetailTask(
                duns=str(row["duns"]),
                company_name_en_dnb=str(row["company_name_en_dnb"]),
                company_name_url=str(row["company_name_url"]),
                address=str(row["address"]),
                city=str(row["city"]),
                region=str(row["region"]),
                country=str(row["country"]),
                postal_code=str(row["postal_code"]),
                sales_revenue=str(row["sales_revenue"]),
                retries=int(row["retries"]),
            )

    def mark_done(self, duns: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (_utc_now(), duns),
            )
            self._conn.commit()

    def defer(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ? WHERE duns = ?",
                (max(retries, 0), _utc_after(delay_seconds), error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def stats(self) -> tuple[int, int]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count
                FROM detail_queue
                """
            ).fetchone()
            pending = int(row["pending_count"] or 0) if row is not None else 0
            running = int(row["running_count"] or 0) if row is not None else 0
            return pending, running
