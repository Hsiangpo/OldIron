"""DNB 美国站点 SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_company_name(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


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
class DnbDetailTask:
    duns: str
    detail_url: str
    company_name: str
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class DnbGMapTask:
    duns: str
    company_name: str
    address: str
    region: str
    city: str
    status: str
    retries: int
    updated_at: str


@dataclass(slots=True)
class DnbSiteTask:
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
    detail_pending: int
    detail_running: int
    gmap_pending: int
    gmap_running: int
    site_pending: int
    site_running: int
    companies_total: int
    final_total: int


class DnbUsStore:
    """线程安全的 DNB 美国存储。"""

    _MAX_RETRIES = 3
    _MAX_WRITE_RETRIES = 15

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
            conn.execute("PRAGMA busy_timeout=60000")
            self._local.conn = conn
        return conn

    def _run_write(self, action):
        for attempt in range(self._MAX_WRITE_RETRIES):
            try:
                conn = self._conn()
                result = action(conn)
                conn.commit()
                return result
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower():
                    raise
                if attempt == self._MAX_WRITE_RETRIES - 1:
                    raise
                time.sleep(min(0.3 * (2 ** attempt), 10.0))
        raise RuntimeError("DNB SQLite 写入重试失败")

    def _init_tables(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
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
                representative TEXT NOT NULL DEFAULT '',
                website TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                region TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                postal_code TEXT NOT NULL DEFAULT '',
                detail_url TEXT NOT NULL DEFAULT '',
                industry_path TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'dnb',
                detail_status TEXT NOT NULL DEFAULT 'pending',
                gmap_status TEXT NOT NULL DEFAULT 'pending',
                site_status TEXT NOT NULL DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS detail_queue (
                duns TEXT PRIMARY KEY,
                detail_url TEXT NOT NULL,
                company_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gmap_queue (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                address TEXT NOT NULL DEFAULT '',
                region TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS site_queue (
                duns TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
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
                phone TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                evidence_url TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
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

        return self._run_write(_action)

    def requeue_running_tasks(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            for table in ("dnb_segments", "detail_queue", "gmap_queue", "site_queue"):
                conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (_now_text(),),
                )

        self._run_write(_action)

    def requeue_empty_detail_tasks(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                """
                SELECT duns, detail_url, company_name
                FROM companies
                WHERE detail_url != ''
                  AND detail_status = 'done'
                  AND representative = ''
                  AND website = ''
                """
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO detail_queue (duns, detail_url, company_name, status, retries, updated_at)
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        detail_url = excluded.detail_url,
                        company_name = excluded.company_name,
                        status = 'pending',
                        retries = 0,
                        updated_at = excluded.updated_at
                    """,
                    (row["duns"], row["detail_url"], row["company_name"], _now_text()),
                )
            if rows:
                conn.execute(
                    """
                    UPDATE companies
                    SET detail_status = 'pending', updated_at = ?
                    WHERE detail_url != ''
                      AND detail_status = 'done'
                      AND representative = ''
                      AND website = ''
                    """,
                    (_now_text(),),
                )
            return len(rows)

        return self._run_write(_action)

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
                if not duns:
                    continue
                conn.execute(
                    """
                    INSERT INTO companies (
                        duns, company_name, representative, website, phone, address,
                        region, city, postal_code, detail_url, industry_path,
                        detail_status, gmap_status, site_status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        address = excluded.address,
                        region = excluded.region,
                        city = excluded.city,
                        postal_code = excluded.postal_code,
                        detail_url = excluded.detail_url,
                        industry_path = excluded.industry_path,
                        updated_at = excluded.updated_at
                    """,
                    (
                        duns,
                        company["company_name"],
                        company.get("representative", ""),
                        company.get("website", ""),
                        company.get("phone", ""),
                        company.get("address", ""),
                        company.get("region", ""),
                        company.get("city", ""),
                        company.get("postal_code", ""),
                        company.get("detail_url", ""),
                        company.get("industry_path", ""),
                        "pending",
                        "pending",
                        "pending",
                        _now_text(),
                    ),
                )
                inserted += 1
            return inserted

        return self._run_write(_action)

    def enqueue_detail_tasks(self, companies: list[dict[str, str]]) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            for company in companies:
                if not company.get("detail_url") or not company.get("duns"):
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO detail_queue (duns, detail_url, company_name, status, retries, updated_at)
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    """,
                    (company["duns"], company["detail_url"], company["company_name"], _now_text()),
                )

        self._run_write(_action)

    def claim_detail_task(self) -> DnbDetailTask | None:
        return self._claim_simple_task("detail_queue", DnbDetailTask)

    def complete_detail_task(self, duns: str, representative: str, website: str, phone: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE companies
                SET representative = CASE WHEN ? != '' THEN ? ELSE representative END,
                    website = CASE WHEN ? != '' THEN ? ELSE website END,
                    phone = CASE WHEN ? != '' THEN ? ELSE phone END,
                    detail_status = 'done',
                    gmap_status = CASE WHEN ? != '' THEN 'done' ELSE gmap_status END,
                    site_status = CASE WHEN ? != '' THEN 'pending' ELSE site_status END,
                    updated_at = ?
                WHERE duns = ?
                """,
                (representative, representative, website, website, phone, phone, website, website, _now_text(), duns),
            )
            if website:
                row = conn.execute(
                    "SELECT duns, company_name, website FROM companies WHERE duns = ?",
                    (duns,),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        """
                        INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at)
                        VALUES (?, ?, ?, 'pending', 0, ?)
                        ON CONFLICT(duns) DO UPDATE SET
                            company_name = excluded.company_name,
                            website = excluded.website,
                            status = 'pending',
                            updated_at = excluded.updated_at
                        """,
                        (row["duns"], row["company_name"], row["website"], _now_text()),
                    )
            conn.execute(
                "UPDATE detail_queue SET status = 'done', updated_at = ? WHERE duns = ?",
                (_now_text(), duns),
            )

        self._run_write(_action)

    def fail_detail_task(self, duns: str) -> None:
        self._retry_task("detail_queue", duns)

    def enqueue_gmap_for_missing_websites(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                """
                SELECT duns, company_name, address, region, city
                FROM companies
                WHERE (website = '' OR website IS NULL)
                  AND gmap_status = 'pending'
                """
            ).fetchall()
            count = 0
            for row in rows:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO gmap_queue (duns, company_name, address, region, city, status, retries, updated_at)
                    VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)
                    """,
                    (row["duns"], row["company_name"], row["address"], row["region"], row["city"], _now_text()),
                )
                count += 1
            return count

        return self._run_write(_action)

    def claim_gmap_task(self) -> DnbGMapTask | None:
        return self._claim_simple_task("gmap_queue", DnbGMapTask)

    def complete_gmap_task(self, duns: str, website: str, phone: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE companies
                SET website = CASE WHEN ? != '' THEN ? ELSE website END,
                    phone = CASE WHEN ? != '' THEN ? ELSE phone END,
                    gmap_status = 'done',
                    site_status = CASE WHEN ? != '' THEN 'pending' ELSE site_status END,
                    updated_at = ?
                WHERE duns = ?
                """,
                (website, website, phone, phone, website, _now_text(), duns),
            )
            if website:
                row = conn.execute(
                    "SELECT duns, company_name, website FROM companies WHERE duns = ?",
                    (duns,),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        """
                        INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at)
                        VALUES (?, ?, ?, 'pending', 0, ?)
                        ON CONFLICT(duns) DO UPDATE SET
                            company_name = excluded.company_name,
                            website = excluded.website,
                            status = 'pending',
                            updated_at = excluded.updated_at
                        """,
                        (row["duns"], row["company_name"], row["website"], _now_text()),
                    )
            conn.execute("UPDATE gmap_queue SET status = 'done', updated_at = ? WHERE duns = ?", (_now_text(), duns))

        self._run_write(_action)

    def fail_gmap_task(self, duns: str) -> None:
        self._retry_task("gmap_queue", duns)

    def claim_site_task(self) -> DnbSiteTask | None:
        def _action(conn: sqlite3.Connection) -> DnbSiteTask | None:
            row = conn.execute(
                """
                SELECT q.duns, q.company_name, c.representative, q.website, q.status, q.retries, q.updated_at
                FROM site_queue q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending'
                ORDER BY q.updated_at, q.duns
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE site_queue SET status = 'running', updated_at = ? WHERE duns = ?", (_now_text(), row["duns"]))
            return DnbSiteTask(**dict(row))

        return self._run_write(_action)

    def complete_site_task(
        self,
        duns: str,
        company_name: str,
        representative: str,
        emails: list[str],
        website: str,
        phone: str,
        address: str,
        evidence_url: str,
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            current = conn.execute(
                """
                SELECT company_name, representative, website, phone, address
                FROM companies
                WHERE duns = ?
                """,
                (duns,),
            ).fetchone()
            current_company_name = str(current["company_name"] or "").strip() if current else ""
            current_representative = str(current["representative"] or "").strip() if current else ""
            current_website = str(current["website"] or "").strip() if current else ""
            current_phone = str(current["phone"] or "").strip() if current else ""
            current_address = str(current["address"] or "").strip() if current else ""
            final_company_name = str(company_name or "").strip() or current_company_name
            names_match = (
                not final_company_name
                or not current_company_name
                or _normalize_company_name(final_company_name) == _normalize_company_name(current_company_name)
            )
            if names_match:
                final_representative = str(representative or "").strip() or current_representative
            else:
                final_representative = str(representative or "").strip()
            final_website = str(website or "").strip() or current_website
            final_phone = str(phone or "").strip() or current_phone
            final_address = str(address or "").strip() or current_address
            conn.execute(
                """
                UPDATE companies
                SET company_name = ?, representative = ?, website = ?, phone = ?, address = ?, site_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                (final_company_name, final_representative, final_website, final_phone, final_address, _now_text(), duns),
            )
            conn.execute(
                """
                INSERT INTO final_companies (duns, company_name, representative, emails, website, phone, address, evidence_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(duns) DO UPDATE SET
                    company_name = excluded.company_name,
                    representative = excluded.representative,
                    emails = excluded.emails,
                    website = excluded.website,
                    phone = excluded.phone,
                    address = excluded.address,
                    evidence_url = excluded.evidence_url,
                    updated_at = excluded.updated_at
                """,
                (
                    duns,
                    final_company_name,
                    final_representative,
                    "; ".join(emails),
                    final_website,
                    final_phone,
                    final_address,
                    str(evidence_url or "").strip() or final_website,
                    _now_text(),
                ),
            )
            conn.execute("UPDATE site_queue SET status = 'done', updated_at = ? WHERE duns = ?", (_now_text(), duns))

        self._run_write(_action)

    def fail_site_task(self, duns: str) -> None:
        self._retry_task("site_queue", duns)

    def progress(self) -> DnbProgress:
        conn = self._conn()
        return DnbProgress(
            segment_pending=self._count_where(conn, "dnb_segments", "status = 'pending'"),
            segment_running=self._count_where(conn, "dnb_segments", "status = 'running'"),
            detail_pending=self._count_where(conn, "detail_queue", "status = 'pending'"),
            detail_running=self._count_where(conn, "detail_queue", "status = 'running'"),
            gmap_pending=self._count_where(conn, "gmap_queue", "status = 'pending'"),
            gmap_running=self._count_where(conn, "gmap_queue", "status = 'running'"),
            site_pending=self._count_where(conn, "site_queue", "status = 'pending'"),
            site_running=self._count_where(conn, "site_queue", "status = 'running'"),
            companies_total=self._count_where(conn, "companies", "1 = 1"),
            final_total=self._count_where(conn, "final_companies", "1 = 1"),
        )

    def export_final_records(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT company_name, representative, emails, website, phone, address, evidence_url
            FROM final_companies
            ORDER BY company_name
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def _count_where(self, conn: sqlite3.Connection, table: str, where_sql: str) -> int:
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where_sql}").fetchone()
        return int(row["cnt"] if row else 0)

    def _set_status(self, table: str, key: str, status: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                f"UPDATE {table} SET status = ?, updated_at = ? WHERE segment_id = ?"
                if table == "dnb_segments"
                else f"UPDATE {table} SET status = ?, updated_at = ? WHERE duns = ?",
                (status, _now_text(), key),
            )
        )

    def _retry_task(self, table: str, duns: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            row = conn.execute(f"SELECT retries FROM {table} WHERE duns = ?", (duns,)).fetchone()
            retries = int(row["retries"] if row else 0) + 1
            status = "failed" if retries >= self._MAX_RETRIES else "pending"
            conn.execute(
                f"UPDATE {table} SET status = ?, retries = ?, updated_at = ? WHERE duns = ?",
                (status, retries, _now_text(), duns),
            )
            if table == "detail_queue":
                conn.execute(
                    "UPDATE companies SET detail_status = ?, updated_at = ? WHERE duns = ?",
                    (status, _now_text(), duns),
                )
            if table == "gmap_queue":
                conn.execute(
                    "UPDATE companies SET gmap_status = ?, updated_at = ? WHERE duns = ?",
                    (status, _now_text(), duns),
                )
            if table == "site_queue":
                conn.execute(
                    "UPDATE companies SET site_status = ?, updated_at = ? WHERE duns = ?",
                    (status, _now_text(), duns),
                )

        self._run_write(_action)

    def _claim_simple_task(self, table: str, model):
        def _action(conn: sqlite3.Connection):
            row = conn.execute(
                f"SELECT * FROM {table} WHERE status = 'pending' ORDER BY updated_at, duns LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            conn.execute(f"UPDATE {table} SET status = 'running', updated_at = ? WHERE duns = ?", (_now_text(), row["duns"]))
            return model(**dict(row))

        return self._run_write(_action)
