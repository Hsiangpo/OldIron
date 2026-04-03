"""DNB 美国站点 SQLite 存储。"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlparse
from urllib.parse import urlunparse


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _time_text_at(epoch: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))


def _normalize_company_name(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


_BAD_EMAIL_TLDS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico", "avif",
    "mp4", "webm", "mov", "pdf", "js", "css", "woff", "woff2", "ttf", "eot", "heic",
}
_BAD_EMAIL_HOST_HINTS = (
    "example.com",
    "example.org",
    "example.net",
    "sample.com",
    "sample.co.jp",
    "eksempel.dk",
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
    "mysite.com",
    "mysite.co.jp",
)
_BAD_WEBSITE_HOST_HINTS = (
    "booking.com",
    "tripadvisor.",
    "bluepillow.",
    "orbitz.com",
    "expedia.",
    "hoteis.com",
    "decolar.com",
    "staticontent.com",
    "stays.net",
    "anota.ai",
    "app.cardapioweb.com",
    "api.whatsapp.com",
    "fb.me",
    "ifood.com.br",
    "instadelivery.com.br",
    "linktr.ee",
    "menudino.com",
    "goomer.app",
    "ola.click",
    "parceiromagalu.com.br",
    "pedido.anota.ai",
    "rvpedidos.com.br",
    "saipos.com",
    "sigmenu.com",
    "viaverdeshopping.com.br",
    "wa.me",
    "whatsapp.com",
)
_BAD_WEBSITE_SUFFIXES = (
    ".gov",
    ".gov.br",
    ".edu",
    ".edu.br",
    ".jus.br",
    ".leg.br",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".gif",
    ".svg",
    ".bmp",
    ".avif",
    ".phones",
)
_BAD_WEBSITE_PATH_HINTS = (
    "/image/",
    "/images/",
    "/media/pictures/",
    "/showuserreviews-",
)


def _clean_site_emails(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for raw in values:
        for part in re.split(r"[;,]", str(raw or "")):
            email = _normalize_email_candidate(part)
            if not email or "@" not in email:
                continue
            domain = email.split("@", 1)[1]
            suffix = domain.rsplit(".", 1)[-1] if "." in domain else ""
            if suffix in _BAD_EMAIL_TLDS:
                continue
            if any(flag in domain for flag in _BAD_EMAIL_HOST_HINTS):
                continue
            if email not in cleaned:
                cleaned.append(email)
    return cleaned


def _normalize_email_candidate(value: object) -> str:
    text = unquote(str(value or "")).strip().lower()
    if not text:
        return ""
    text = text.replace("mailto:", "")
    text = re.sub(r"^(?:u003e|u003c|>|<)+", "", text)
    match = re.search(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})", text)
    if match is None:
        return ""
    email = str(match.group(1) or "").strip().lower()
    if "@" not in email:
        return ""
    local, domain = email.split("@", 1)
    local = re.sub(r"^\++", "", local)
    local = re.sub(r"^\d+\++", "", local)
    local = re.sub(r"^\++", "", local)
    if not local:
        return ""
    return f"{local}@{domain}"


def _normalize_website_candidate(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = f"https://{text}"
    parsed = urlparse(text)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host or " " in host or ".." in host or host.startswith(".") or host.endswith("."):
        return ""
    if not re.fullmatch(r"[a-z0-9.-]+", host):
        return ""
    labels = [part for part in host.split(".") if part]
    if len(labels) < 2 or not re.fullmatch(r"[a-z]{2,}", labels[-1]):
        return ""
    cleaned = parsed._replace(scheme="https", netloc=host, fragment="")
    return urlunparse(cleaned).rstrip("/")


def _looks_like_bad_website(value: object) -> bool:
    website = _normalize_website_candidate(value)
    if not website:
        return True
    parsed = urlparse(website)
    host = (parsed.netloc or "").strip().lower()
    path = (parsed.path or "").strip().lower()
    if any(hint in host for hint in _BAD_WEBSITE_HOST_HINTS):
        return True
    if any(host.endswith(suffix) for suffix in _BAD_WEBSITE_SUFFIXES):
        return True
    if any(path.endswith(suffix) for suffix in _BAD_WEBSITE_SUFFIXES):
        return True
    if any(hint in path for hint in _BAD_WEBSITE_PATH_HINTS):
        return True
    return False


def _clean_website_candidate(value: object) -> str:
    website = _normalize_website_candidate(value)
    if not website or _looks_like_bad_website(website):
        return ""
    return website


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
    _WRITE_MUTEX = threading.RLock()

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
                site_emails TEXT NOT NULL DEFAULT '',
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
            CREATE INDEX IF NOT EXISTS idx_us_dnb_segments_status_segment
            ON dnb_segments(status, segment_id);
            CREATE INDEX IF NOT EXISTS idx_us_detail_queue_status_updated
            ON detail_queue(status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_gmap_queue_status_updated
            ON gmap_queue(status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_site_queue_status_updated
            ON site_queue(status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_companies_detail_status
            ON companies(detail_status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_companies_gmap_status
            ON companies(gmap_status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_companies_site_status
            ON companies(site_status, updated_at, duns);
            CREATE INDEX IF NOT EXISTS idx_us_companies_name_norm
            ON companies(lower(trim(company_name)));
            CREATE INDEX IF NOT EXISTS idx_us_final_name_norm
            ON final_companies(lower(trim(company_name)));
            """
        )
        self._ensure_companies_columns(conn)
        conn.commit()

    def _ensure_companies_columns(self, conn: sqlite3.Connection) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        if "site_emails" not in columns:
            conn.execute("ALTER TABLE companies ADD COLUMN site_emails TEXT NOT NULL DEFAULT ''")

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

    def requeue_stale_running_tasks(self, max_age_seconds: float = 900.0) -> int:
        cutoff = _time_text_at(time.time() - max(float(max_age_seconds or 0.0), 0.0))

        def _action(conn: sqlite3.Connection) -> int:
            recovered = 0
            for table in ("dnb_segments", "detail_queue", "gmap_queue", "site_queue"):
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
            for table in ("detail_queue", "gmap_queue", "site_queue"):
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
                SET detail_status = 'pending', gmap_status = 'pending', site_status = 'pending', updated_at = ?
                WHERE detail_status = 'failed' OR gmap_status = 'failed' OR site_status = 'failed'
                """,
                (now,),
            )
            return recovered

        return int(self._run_write(_action) or 0)

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

    def enqueue_site_for_ready_websites(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                """
                SELECT duns, company_name, website
                FROM companies
                WHERE website != ''
                  AND site_status = 'pending'
                """
            ).fetchall()
            count = 0
            for row in rows:
                website = _clean_website_candidate(row["website"])
                if not website:
                    continue
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at)
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        website = excluded.website,
                        status = 'pending',
                        retries = 0,
                        updated_at = excluded.updated_at
                    """,
                    (row["duns"], row["company_name"], website, _now_text()),
                )
                count += conn.total_changes - before
            return count

        return int(self._run_write(_action) or 0)

    def purge_bad_websites(self) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                """
                SELECT duns, website
                FROM companies
                WHERE website != ''
                """
            ).fetchall()
            bad_duns = [
                str(row["duns"])
                for row in rows
                if _looks_like_bad_website(row["website"])
            ]
            if not bad_duns:
                return 0
            placeholders = ",".join("?" for _ in bad_duns)
            conn.execute(
                f"""
                UPDATE companies
                SET website = '', gmap_status = 'pending', site_status = 'pending', updated_at = ?
                WHERE duns IN ({placeholders})
                """,
                (_now_text(), *bad_duns),
            )
            conn.execute(f"DELETE FROM site_queue WHERE duns IN ({placeholders})", bad_duns)
            conn.execute(f"DELETE FROM final_companies WHERE duns IN ({placeholders})", bad_duns)
            return len(bad_duns)

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
        def _action(conn: sqlite3.Connection) -> DnbDetailTask | None:
            now_text = _now_text()
            row = conn.execute(
                """
                SELECT q.*
                FROM detail_queue q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending'
                  AND q.updated_at <= ?
                  AND c.website != ''
                ORDER BY q.updated_at, q.duns
                LIMIT 1
                """,
                (now_text,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT q.*
                    FROM detail_queue q
                    WHERE q.status = 'pending'
                      AND q.updated_at <= ?
                    ORDER BY q.updated_at, q.duns
                    LIMIT 1
                    """,
                    (now_text,),
                ).fetchone()
                if row is None:
                    return None
            updated = conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE duns = ? AND status = 'pending'",
                (now_text, row["duns"]),
            ).rowcount
            if updated != 1:
                return None
            return DnbDetailTask(**dict(row))

        return self._run_write(_action)

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
            self._enqueue_site_if_ready(conn, duns)
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
        def _action(conn: sqlite3.Connection) -> DnbGMapTask | None:
            now_text = _now_text()
            row = conn.execute(
                """
                SELECT q.*
                FROM gmap_queue q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending'
                  AND q.updated_at <= ?
                ORDER BY
                    CASE WHEN c.representative != '' THEN 0 ELSE 1 END,
                    q.updated_at,
                    q.duns
                LIMIT 1
                """,
                (now_text,),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE gmap_queue SET status = 'running', updated_at = ? WHERE duns = ? AND status = 'pending'",
                (now_text, row["duns"]),
            ).rowcount
            if updated != 1:
                return None
            return DnbGMapTask(**dict(row))

        return self._run_write(_action)

    def complete_gmap_task(self, duns: str, website: str, phone: str) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            clean_website = _clean_website_candidate(website)
            conn.execute(
                """
                UPDATE companies
                SET website = CASE WHEN ? != '' THEN ? ELSE website END,
                    phone = CASE WHEN ? != '' THEN ? ELSE phone END,
                    gmap_status = 'done',
                    updated_at = ?
                WHERE duns = ?
                """,
                (clean_website, clean_website, phone, phone, _now_text(), duns),
            )
            self._enqueue_site_if_ready(conn, duns)
            conn.execute("UPDATE gmap_queue SET status = 'done', updated_at = ? WHERE duns = ?", (_now_text(), duns))

        self._run_write(_action)

    def fail_gmap_task(self, duns: str) -> None:
        self._retry_task("gmap_queue", duns)

    def claim_site_task(self) -> DnbSiteTask | None:
        def _action(conn: sqlite3.Connection) -> DnbSiteTask | None:
            now_text = _now_text()
            row = conn.execute(
                """
                SELECT q.duns, q.company_name, c.representative, q.website, q.status, q.retries, q.updated_at
                FROM site_queue q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending'
                  AND q.updated_at <= ?
                ORDER BY
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM final_companies f
                            WHERE lower(trim(f.company_name)) = lower(trim(c.company_name))
                        ) THEN 1
                        ELSE 0
                    END,
                    CASE WHEN c.representative != '' THEN 0 ELSE 1 END,
                    q.updated_at,
                    q.duns
                LIMIT 1
                """
                ,
                (now_text,),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                "UPDATE site_queue SET status = 'running', updated_at = ? WHERE duns = ? AND status = 'pending'",
                (now_text, row["duns"]),
            ).rowcount
            if updated != 1:
                return None
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
            final_emails = _clean_site_emails(emails)
            current = conn.execute(
                """
                SELECT company_name, representative, website, phone, address, site_emails
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
            current_site_emails = _clean_site_emails(str(current["site_emails"] or "").split(";")) if current else []
            existing_final = conn.execute(
                """
                SELECT company_name, representative, emails, website, phone, address, evidence_url
                FROM final_companies
                WHERE duns = ?
                """,
                (duns,),
            ).fetchone()
            if existing_final is not None:
                existing_emails = _clean_site_emails(str(existing_final["emails"] or "").split(";"))
                final_emails = _clean_site_emails([*existing_emails, *current_site_emails, *final_emails])
            else:
                final_emails = _clean_site_emails([*current_site_emails, *final_emails])
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
            final_website = _clean_website_candidate(website) or _clean_website_candidate(current_website)
            final_phone = str(phone or "").strip() or current_phone
            final_address = str(address or "").strip() or current_address
            conn.execute(
                """
                UPDATE companies
                SET company_name = ?, representative = ?, website = ?, phone = ?, address = ?, site_emails = ?, site_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                (
                    final_company_name,
                    final_representative,
                    final_website,
                    final_phone,
                    final_address,
                    "; ".join(final_emails),
                    _now_text(),
                    duns,
                ),
            )
            if final_company_name and final_representative and final_emails:
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
                        "; ".join(final_emails),
                        final_website,
                        final_phone,
                        final_address,
                        str(evidence_url or "").strip() or final_website,
                        _now_text(),
                    ),
                )
            elif existing_final is None:
                conn.execute("DELETE FROM final_companies WHERE duns = ?", (duns,))
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
                if status == "failed":
                    self._enqueue_site_if_ready(conn, duns)
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

    def _enqueue_site_if_ready(self, conn: sqlite3.Connection, duns: str) -> None:
        row = conn.execute(
            """
            SELECT duns, company_name, website, representative, detail_status
            FROM companies
            WHERE duns = ?
            """,
            (duns,),
        ).fetchone()
        if row is None:
            return
        website = _clean_website_candidate(row["website"])
        if website:
            conn.execute(
                "UPDATE companies SET website = ?, site_status = 'pending', updated_at = ? WHERE duns = ?",
                (website, _now_text(), duns),
            )
        if not website:
            return
        conn.execute(
            """
            INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at)
            VALUES (?, ?, ?, 'pending', 0, ?)
            ON CONFLICT(duns) DO UPDATE SET
                company_name = excluded.company_name,
                website = excluded.website,
                status = 'pending',
                retries = 0,
                updated_at = excluded.updated_at
            """,
            (row["duns"], row["company_name"], website, _now_text()),
        )

    def _claim_simple_task(self, table: str, model):
        def _action(conn: sqlite3.Connection):
            now_text = _now_text()
            row = conn.execute(
                f"""
                SELECT *
                FROM {table}
                WHERE status = 'pending'
                  AND updated_at <= ?
                ORDER BY updated_at, duns
                LIMIT 1
                """,
                (now_text,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(f"UPDATE {table} SET status = 'running', updated_at = ? WHERE duns = ?", (now_text, row["duns"]))
            return model(**dict(row))

        return self._run_write(_action)
