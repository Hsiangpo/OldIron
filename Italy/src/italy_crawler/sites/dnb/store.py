"""Italy DNB SQLite 存储。"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _time_text_at(epoch: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))


def _normalize_email_candidate(value: object) -> str:
    text = str(value or "").strip().lower()
    match = re.search(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})", text)
    if match is None:
        return ""
    return str(match.group(1) or "").strip().lower()


def _clean_site_emails(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for raw in values:
        for part in re.split(r"[;,]", str(raw or "")):
            email = _normalize_email_candidate(part)
            if email and email not in cleaned:
                cleaned.append(email)
    return cleaned


@dataclass(slots=True)
class DnbSegmentTask:
    segment_id: str
    industry_path: str
    country_iso_two_code: str
    region_name: str
    city_name: str
    expected_count: int
    next_page: int
    status: str
    updated_at: str


@dataclass(slots=True)
class VerifTask:
    duns: str
    company_name: str
    region: str
    city: str
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class SiteTask:
    duns: str
    company_name: str
    representative: str
    website: str
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class DnbProgress:
    segment_pending: int
    segment_running: int
    verif_pending: int
    verif_running: int
    site_pending: int
    site_running: int
    companies_total: int
    final_total: int


class ItalyDnbStore:
    """线程安全的 Italy DNB 存储。"""

    _MAX_RETRIES = 3
    _MAX_WRITE_RETRIES = 15
    _WRITE_MUTEX = threading.RLock()

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._shared_conn: sqlite3.Connection | None = None
        self._init_tables()

    def close(self) -> None:
        with self._WRITE_MUTEX:
            if self._shared_conn is not None:
                self._shared_conn.close()
                self._shared_conn = None

    def _conn(self) -> sqlite3.Connection:
        with self._WRITE_MUTEX:
            conn = self._shared_conn
            if conn is None:
                self._db_path.parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(str(self._db_path), timeout=30.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=60000")
                self._shared_conn = conn
            return conn

    def _run_write(self, action):
        with self._WRITE_MUTEX:
            for attempt in range(self._MAX_WRITE_RETRIES):
                try:
                    conn = self._conn()
                    result = action(conn)
                    conn.commit()
                    return result
                except sqlite3.OperationalError as exc:
                    conn = self._conn()
                    conn.rollback()
                    if "database is locked" not in str(exc).lower():
                        raise
                    if attempt == self._MAX_WRITE_RETRIES - 1:
                        raise
                    time.sleep(min(0.3 * (2**attempt), 10.0))
        raise RuntimeError("Italy DNB SQLite 写入重试失败")

    def _init_tables(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dnb_segments (
                segment_id TEXT PRIMARY KEY,
                industry_path TEXT NOT NULL,
                country_iso_two_code TEXT NOT NULL,
                region_name TEXT NOT NULL DEFAULT '',
                city_name TEXT NOT NULL DEFAULT '',
                expected_count INTEGER NOT NULL DEFAULT 0,
                next_page INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS companies (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                postal_code TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                industry_path TEXT NOT NULL DEFAULT '',
                representative TEXT NOT NULL DEFAULT '',
                website TEXT NOT NULL DEFAULT '',
                site_emails TEXT NOT NULL DEFAULT '',
                evidence_url TEXT NOT NULL DEFAULT '',
                verif_status TEXT NOT NULL DEFAULT 'pending',
                site_status TEXT NOT NULL DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS verif_queue (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS site_queue (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                representative TEXT NOT NULL DEFAULT '',
                website TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS final_companies (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                representative TEXT NOT NULL DEFAULT '',
                emails TEXT NOT NULL DEFAULT '',
                website TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                evidence_url TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_it_dnb_segments_status_segment
            ON dnb_segments(status, segment_id);
            CREATE INDEX IF NOT EXISTS idx_it_verif_queue_status_updated
            ON verif_queue(status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_it_site_queue_status_updated
            ON site_queue(status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_it_companies_verif_status
            ON companies(verif_status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_it_companies_site_status
            ON companies(site_status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_it_companies_name_norm
            ON companies(lower(trim(company_name)));
            CREATE INDEX IF NOT EXISTS idx_it_final_name_norm
            ON final_companies(lower(trim(company_name)));
            """
        )
        conn.commit()

    def seed_segments(self, segments: list[dict[str, str | int]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            count = 0
            for segment in segments:
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dnb_segments (
                        segment_id, industry_path, country_iso_two_code, region_name,
                        city_name, expected_count, next_page, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment["segment_id"],
                        segment["industry_path"],
                        segment["country_iso_two_code"],
                        segment["region_name"],
                        segment["city_name"],
                        segment["expected_count"],
                        segment["next_page"],
                        segment["status"],
                        _now_text(),
                    ),
                )
                count += int(conn.total_changes > before)
            return count

        return int(self._run_write(_action) or 0)

    def requeue_running_tasks(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            for table in ("dnb_segments", "verif_queue", "site_queue"):
                conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (_now_text(),),
                )

        self._run_write(_action)

    def requeue_stale_running_tasks(self, max_age_seconds: float = 900.0) -> int:
        cutoff = _time_text_at(time.time() - max(float(max_age_seconds or 0.0), 0.0))

        def _action(conn: sqlite3.Connection) -> int:
            recovered = 0
            for table in ("dnb_segments", "verif_queue", "site_queue"):
                before = conn.total_changes
                conn.execute(
                    f"""
                    UPDATE {table}
                    SET status = 'pending', updated_at = ?
                    WHERE status = 'running' AND updated_at <= ?
                    """,
                    (_now_text(), cutoff),
                )
                recovered += conn.total_changes - before
            return recovered

        return int(self._run_write(_action) or 0)

    def requeue_failed_tasks(self) -> int:
        now = _now_text()

        def _action(conn: sqlite3.Connection) -> int:
            recovered = 0
            for table in ("verif_queue", "site_queue"):
                before = conn.total_changes
                conn.execute(
                    f"""
                    UPDATE {table}
                    SET status = 'pending', retries = 0, updated_at = ?
                    WHERE status = 'failed'
                    """,
                    (now,),
                )
                recovered += conn.total_changes - before
            conn.execute(
                """
                UPDATE companies
                SET verif_status = CASE WHEN verif_status = 'failed' THEN 'pending' ELSE verif_status END,
                    site_status = CASE WHEN site_status = 'failed' THEN 'pending' ELSE site_status END,
                    updated_at = ?
                WHERE verif_status = 'failed' OR site_status = 'failed'
                """,
                (now,),
            )
            return recovered

        return int(self._run_write(_action) or 0)

    def claim_segment(self) -> DnbSegmentTask | None:
        def _action(conn: sqlite3.Connection) -> DnbSegmentTask | None:
            row = conn.execute(
                """
                SELECT * FROM dnb_segments
                WHERE status = 'pending'
                ORDER BY segment_id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE dnb_segments SET status = 'running', updated_at = ? WHERE segment_id = ?",
                (_now_text(), row["segment_id"]),
            )
            return DnbSegmentTask(**dict(row))

        return self._run_write(_action)

    def update_segment_page(self, segment_id: str, next_page: int, expected_count: int) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE dnb_segments SET next_page = ?, expected_count = ?, updated_at = ? WHERE segment_id = ?",
                (next_page, expected_count, _now_text(), segment_id),
            )
        )

    def complete_segment(self, segment_id: str) -> None:
        self._set_status("dnb_segments", segment_id, "done")

    def defer_segment(self, segment_id: str) -> None:
        self._set_status("dnb_segments", segment_id, "pending")

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for company in companies:
                duns = str(company.get("duns", "")).strip()
                company_name = str(company.get("company_name", "")).strip()
                if not duns or not company_name:
                    continue
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT INTO companies (
                        duns, company_name, region, city, postal_code, address, industry_path,
                        representative, website, site_emails, evidence_url, verif_status, site_status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, '', '', '', '', 'pending', 'pending', ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        region = excluded.region,
                        city = excluded.city,
                        postal_code = excluded.postal_code,
                        address = excluded.address,
                        industry_path = excluded.industry_path,
                        updated_at = excluded.updated_at
                    """,
                    (
                        duns,
                        company_name,
                        str(company.get("region", "") or "").strip(),
                        str(company.get("city", "") or "").strip(),
                        str(company.get("postal_code", "") or "").strip(),
                        str(company.get("address", "") or "").strip(),
                        str(company.get("industry_path", "") or "").strip(),
                        _now_text(),
                    ),
                )
                inserted += int(conn.total_changes > before)
            return inserted

        return int(self._run_write(_action) or 0)

    def enqueue_verif_tasks(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            count = 0
            for company in companies:
                duns = str(company.get("duns", "")).strip()
                company_name = str(company.get("company_name", "")).strip()
                if not duns or not company_name:
                    continue
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT INTO verif_queue (duns, company_name, region, city, status, retries, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        region = excluded.region,
                        city = excluded.city,
                        status = 'pending',
                        updated_at = excluded.updated_at
                    """,
                    (
                        duns,
                        company_name,
                        str(company.get("region", "") or "").strip(),
                        str(company.get("city", "") or "").strip(),
                        _now_text(),
                    ),
                )
                count += int(conn.total_changes > before)
            return count

        return int(self._run_write(_action) or 0)

    def claim_verif_task(self) -> VerifTask | None:
        def _action(conn: sqlite3.Connection) -> VerifTask | None:
            row = conn.execute(
                """
                SELECT *
                FROM verif_queue
                WHERE status = 'pending'
                ORDER BY retries, updated_at, duns
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE verif_queue SET status = 'running', updated_at = ? WHERE duns = ? AND status = 'pending'",
                (_now_text(), row["duns"]),
            ).rowcount
            if updated != 1:
                return None
            return VerifTask(**dict(row))

        return self._run_write(_action)

    def complete_verif_task(
        self,
        duns: str,
        *,
        website: str,
        representative: str,
        evidence_url: str,
        company_name: str = "",
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE companies
                SET company_name = CASE WHEN ? != '' THEN ? ELSE company_name END,
                    website = CASE WHEN ? != '' THEN ? ELSE website END,
                    representative = CASE WHEN ? != '' THEN ? ELSE representative END,
                    evidence_url = CASE WHEN ? != '' THEN ? ELSE evidence_url END,
                    verif_status = 'done',
                    site_status = CASE WHEN ? != '' THEN 'pending' ELSE site_status END,
                    updated_at = ?
                WHERE duns = ?
                """,
                (
                    company_name,
                    company_name,
                    website,
                    website,
                    representative,
                    representative,
                    evidence_url,
                    evidence_url,
                    website,
                    _now_text(),
                    duns,
                ),
            )
            self._enqueue_site_if_ready(conn, duns)
            conn.execute(
                "UPDATE verif_queue SET status = 'done', updated_at = ? WHERE duns = ?",
                (_now_text(), duns),
            )

        self._run_write(_action)

    def fail_verif_task(self, duns: str) -> None:
        self._retry_task("verif_queue", duns, company_status_field="verif_status")

    def claim_site_task(self) -> SiteTask | None:
        def _action(conn: sqlite3.Connection) -> SiteTask | None:
            row = conn.execute(
                """
                SELECT q.duns, q.company_name, q.representative, q.website, q.status, q.retries, q.updated_at
                FROM site_queue q
                WHERE q.status = 'pending'
                ORDER BY q.retries, q.updated_at, q.duns
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE site_queue SET status = 'running', updated_at = ? WHERE duns = ? AND status = 'pending'",
                (_now_text(), row["duns"]),
            ).rowcount
            if updated != 1:
                return None
            return SiteTask(**dict(row))

        return self._run_write(_action)

    def complete_site_task(
        self,
        duns: str,
        *,
        emails: list[str],
        evidence_url: str,
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            current = conn.execute(
                """
                SELECT company_name, representative, website, address, site_emails, evidence_url
                FROM companies
                WHERE duns = ?
                """,
                (duns,),
            ).fetchone()
            if current is None:
                return None
            merged_emails = _clean_site_emails(
                [*str(current["site_emails"] or "").split(";"), *list(emails or [])]
            )
            company_name = str(current["company_name"] or "").strip()
            representative = str(current["representative"] or "").strip()
            website = str(current["website"] or "").strip()
            address = str(current["address"] or "").strip()
            final_evidence_url = str(evidence_url or "").strip() or str(current["evidence_url"] or "").strip() or website
            conn.execute(
                """
                UPDATE companies
                SET site_emails = ?, evidence_url = ?, site_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                ("; ".join(merged_emails), final_evidence_url, _now_text(), duns),
            )
            conn.execute(
                """
                INSERT INTO final_companies (duns, company_name, representative, emails, website, address, evidence_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(duns) DO UPDATE SET
                    company_name = excluded.company_name,
                    representative = excluded.representative,
                    emails = excluded.emails,
                    website = excluded.website,
                    address = excluded.address,
                    evidence_url = excluded.evidence_url,
                    updated_at = excluded.updated_at
                """,
                (
                    duns,
                    company_name,
                    representative,
                    "; ".join(merged_emails),
                    website,
                    address,
                    final_evidence_url,
                    _now_text(),
                ),
            )
            conn.execute(
                "UPDATE site_queue SET status = 'done', updated_at = ? WHERE duns = ?",
                (_now_text(), duns),
            )

        self._run_write(_action)

    def fail_site_task(self, duns: str) -> None:
        self._retry_task("site_queue", duns, company_status_field="site_status")

    def progress(self) -> DnbProgress:
        conn = self._conn()
        return DnbProgress(
            segment_pending=self._count_where(conn, "dnb_segments", "status = 'pending'"),
            segment_running=self._count_where(conn, "dnb_segments", "status = 'running'"),
            verif_pending=self._count_where(conn, "verif_queue", "status = 'pending'"),
            verif_running=self._count_where(conn, "verif_queue", "status = 'running'"),
            site_pending=self._count_where(conn, "site_queue", "status = 'pending'"),
            site_running=self._count_where(conn, "site_queue", "status = 'running'"),
            companies_total=self._count_where(conn, "companies", "1 = 1"),
            final_total=self._count_where(conn, "final_companies", "1 = 1"),
        )

    def _enqueue_site_if_ready(self, conn: sqlite3.Connection, duns: str) -> None:
        row = conn.execute(
            """
            SELECT duns, company_name, representative, website
            FROM companies
            WHERE duns = ?
            """,
            (duns,),
        ).fetchone()
        if row is None:
            return None
        website = str(row["website"] or "").strip()
        if not website:
            return None
        conn.execute(
            """
            INSERT INTO site_queue (duns, company_name, representative, website, status, retries, updated_at)
            VALUES (?, ?, ?, ?, 'pending', 0, ?)
            ON CONFLICT(duns) DO UPDATE SET
                company_name = excluded.company_name,
                representative = excluded.representative,
                website = excluded.website,
                status = 'pending',
                updated_at = excluded.updated_at
            """,
            (
                row["duns"],
                row["company_name"],
                row["representative"],
                website,
                _now_text(),
            ),
        )

    def _retry_task(self, table: str, task_id: str, *, company_status_field: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            row = conn.execute(
                f"SELECT retries FROM {table} WHERE duns = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                return None
            retries = int(row["retries"] or 0) + 1
            if retries >= self._MAX_RETRIES:
                conn.execute(
                    f"UPDATE {table} SET status = 'failed', retries = ?, updated_at = ? WHERE duns = ?",
                    (retries, _now_text(), task_id),
                )
                conn.execute(
                    f"UPDATE companies SET {company_status_field} = 'failed', updated_at = ? WHERE duns = ?",
                    (_now_text(), task_id),
                )
                return None
            conn.execute(
                f"UPDATE {table} SET status = 'pending', retries = ?, updated_at = ? WHERE duns = ?",
                (retries, _now_text(), task_id),
            )

        self._run_write(_action)

    def _set_status(self, table: str, task_id: str, status: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                f"UPDATE {table} SET status = ?, updated_at = ? WHERE segment_id = ?" if table == "dnb_segments"
                else f"UPDATE {table} SET status = ?, updated_at = ? WHERE duns = ?",
                (status, _now_text(), task_id),
            )
        )

    def _count_where(self, conn: sqlite3.Connection, table: str, where_clause: str) -> int:
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where_clause}").fetchone()
        return int(row["cnt"] if row else 0)
