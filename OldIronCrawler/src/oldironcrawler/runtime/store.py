from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, fields
from pathlib import Path

from oldironcrawler.importer import ImportedWebsite


@dataclass
class SiteTask:
    id: int
    input_index: int
    website: str
    dedupe_key: str
    retry_count: int
    company_name: str = ""


@dataclass
class SiteResult:
    company_name: str
    representative: str
    emails: str
    website: str
    phones: str = ""
    searched_representative: str = ""
    searched_representative_evidence_url: str = ""
    searched_representative_confidence: str = ""
    evidence_url: str = ""
    evidence_quote: str = ""


@dataclass
class CachedSiteOutcome:
    status: str
    result: SiteResult
    last_error: str = ""


@dataclass
class SiteStageMetrics:
    discover_ms: int = 0
    llm_pick_ms: int = 0
    fetch_pages_ms: int = 0
    llm_extract_ms: int = 0
    ai_email_ms: int = 0
    search_rep_ms: int = 0
    email_rule_ms: int = 0
    company_rule_ms: int = 0
    discovered_url_count: int = 0
    rep_url_count: int = 0
    email_url_count: int = 0
    target_url_count: int = 0
    fetched_page_count: int = 0


class RuntimeStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._thread_connections: dict[int, sqlite3.Connection] = {}
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        thread_id = threading.get_ident()
        with self._conn_lock:
            conn = self._thread_connections.get(thread_id)
            if conn is not None and _connection_is_alive(conn):
                return conn
            if conn is not None:
                _close_connection_quietly(conn)
            conn = self._open_connection()
            self._thread_connections[thread_id] = conn
            return conn

    def _open_connection(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def close(self) -> None:
        with self._conn_lock:
            connections = list(self._thread_connections.values())
            self._thread_connections.clear()
        for conn in connections:
            _close_connection_quietly(conn)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS job_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    input_name TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    total_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    input_index INTEGER NOT NULL,
                    input_company_name TEXT NOT NULL DEFAULT '',
                    raw_website TEXT NOT NULL,
                    website TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    emails TEXT NOT NULL DEFAULT '',
                    phones TEXT NOT NULL DEFAULT '',
                    searched_representative TEXT NOT NULL DEFAULT '',
                    searched_representative_evidence_url TEXT NOT NULL DEFAULT '',
                    searched_representative_confidence TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    evidence_quote TEXT NOT NULL DEFAULT '',
                    discover_ms INTEGER NOT NULL DEFAULT 0,
                    llm_pick_ms INTEGER NOT NULL DEFAULT 0,
                    fetch_pages_ms INTEGER NOT NULL DEFAULT 0,
                    llm_extract_ms INTEGER NOT NULL DEFAULT 0,
                    ai_email_ms INTEGER NOT NULL DEFAULT 0,
                    search_rep_ms INTEGER NOT NULL DEFAULT 0,
                    email_rule_ms INTEGER NOT NULL DEFAULT 0,
                    company_rule_ms INTEGER NOT NULL DEFAULT 0,
                    discovered_url_count INTEGER NOT NULL DEFAULT 0,
                    rep_url_count INTEGER NOT NULL DEFAULT 0,
                    email_url_count INTEGER NOT NULL DEFAULT 0,
                    target_url_count INTEGER NOT NULL DEFAULT 0,
                    fetched_page_count INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_sites_status_input
                ON sites(status, retry_count, input_index);

                CREATE TABLE IF NOT EXISTS learned_tokens (
                    kind TEXT NOT NULL,
                    token TEXT NOT NULL,
                    weight INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(kind, token)
                );

                CREATE TABLE IF NOT EXISTS site_result_cache (
                    dedupe_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    emails TEXT NOT NULL DEFAULT '',
                    website TEXT NOT NULL DEFAULT '',
                    phones TEXT NOT NULL DEFAULT '',
                    searched_representative TEXT NOT NULL DEFAULT '',
                    searched_representative_evidence_url TEXT NOT NULL DEFAULT '',
                    searched_representative_confidence TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    evidence_quote TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            self._ensure_site_text_columns(conn)
            self._ensure_site_search_columns(conn)
            self._ensure_site_metrics_columns(conn)
            self._backfill_result_cache(conn)

    def _ensure_site_text_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sites)").fetchall()
        }
        if "phones" not in existing:
            conn.execute("ALTER TABLE sites ADD COLUMN phones TEXT NOT NULL DEFAULT ''")

    def _ensure_site_search_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sites)").fetchall()
        }
        additions = {
            "input_company_name": "ALTER TABLE sites ADD COLUMN input_company_name TEXT NOT NULL DEFAULT ''",
            "searched_representative": "ALTER TABLE sites ADD COLUMN searched_representative TEXT NOT NULL DEFAULT ''",
            "searched_representative_evidence_url": (
                "ALTER TABLE sites ADD COLUMN searched_representative_evidence_url TEXT NOT NULL DEFAULT ''"
            ),
            "searched_representative_confidence": (
                "ALTER TABLE sites ADD COLUMN searched_representative_confidence TEXT NOT NULL DEFAULT ''"
            ),
        }
        for name, sql in additions.items():
            if name not in existing:
                conn.execute(sql)

    def _ensure_site_metrics_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sites)").fetchall()
        }
        for name in _METRIC_COLUMNS:
            if name in existing:
                continue
            conn.execute(f"ALTER TABLE sites ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")

    def prepare_job(self, *, input_name: str, fingerprint: str, rows: list[ImportedWebsite]) -> None:
        with self._write_lock, self._connect() as conn:
            current = conn.execute("SELECT input_name, fingerprint FROM job_meta WHERE id = 1").fetchone()
            if current is not None and current["input_name"] == input_name and current["fingerprint"] == fingerprint:
                existing = conn.execute("SELECT COUNT(*) AS cnt FROM sites").fetchone()
                if existing is not None and int(existing["cnt"] or 0) > 0:
                    return
            conn.executescript(
                """
                DELETE FROM job_meta;
                DELETE FROM sites;
                DELETE FROM sqlite_sequence WHERE name = 'sites';
                """
            )
            conn.execute(
                "INSERT INTO job_meta(id, input_name, fingerprint, total_count) VALUES(1, ?, ?, ?)",
                (input_name, fingerprint, len(rows)),
            )
            conn.executemany(
                """
                INSERT INTO sites(input_index, input_company_name, raw_website, website, dedupe_key)
                VALUES(?, ?, ?, ?, ?)
                """,
                [
                    (row.input_index, row.company_name, row.raw_website, row.website, row.dedupe_key)
                    for row in rows
                ],
            )

    def reset_running_tasks(self) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'pending',
                    started_at = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    ai_email_ms = 0,
                    search_rep_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0
                WHERE status = 'running'
                """
            )

    def reset_completed_job_for_rerun(self) -> bool:
        with self._write_lock, self._connect() as conn:
            counts = {
                status: int(
                    conn.execute("SELECT COUNT(*) AS cnt FROM sites WHERE status = ?", (status,)).fetchone()["cnt"]
                )
                for status in ("pending", "running", "failed_temp", "done", "dropped")
            }
            total = sum(counts.values())
            if total <= 0:
                return False
            if counts["pending"] > 0 or counts["running"] > 0 or counts["failed_temp"] > 0:
                return False
            self._clear_result_cache_for_current_job(conn)
            conn.execute(
                """
                UPDATE sites
                SET status = 'pending',
                    retry_count = 0,
                    last_error = '',
                    company_name = '',
                    representative = '',
                    emails = '',
                    phones = '',
                    searched_representative = '',
                    searched_representative_evidence_url = '',
                    searched_representative_confidence = '',
                    evidence_url = '',
                    evidence_quote = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    ai_email_ms = 0,
                    search_rep_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0,
                    started_at = '',
                    finished_at = ''
                """
            )
            return True

    def _clear_result_cache_for_current_job(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            DELETE FROM site_result_cache
            WHERE dedupe_key IN (
                SELECT dedupe_key
                FROM sites
                WHERE dedupe_key != ''
            )
            """
        )

    def load_cached_outcome(self, dedupe_key: str) -> CachedSiteOutcome | None:
        key = str(dedupe_key or "").strip()
        if not key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status,
                       last_error,
                       company_name,
                       representative,
                       emails,
                       website,
                       phones,
                       searched_representative,
                       searched_representative_evidence_url,
                       searched_representative_confidence,
                       evidence_url,
                       evidence_quote
                FROM site_result_cache
                WHERE dedupe_key = ?
                """,
                (key,),
            ).fetchone()
        if row is None:
            return None
        status = str(row["status"] or "").strip()
        if status not in {"done", "dropped"}:
            return None
        return CachedSiteOutcome(
            status=status,
            last_error=str(row["last_error"] or ""),
            result=SiteResult(
                company_name=str(row["company_name"] or ""),
                representative=str(row["representative"] or ""),
                emails=str(row["emails"] or ""),
                website=str(row["website"] or ""),
                phones=str(row["phones"] or ""),
                searched_representative=str(row["searched_representative"] or ""),
                searched_representative_evidence_url=str(row["searched_representative_evidence_url"] or ""),
                searched_representative_confidence=str(row["searched_representative_confidence"] or ""),
                evidence_url=str(row["evidence_url"] or ""),
                evidence_quote=str(row["evidence_quote"] or ""),
            ),
        )

    def claim_next_site(self) -> SiteTask | None:
        with self._write_lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, input_index, input_company_name, website, dedupe_key, retry_count
                FROM sites
                WHERE status IN ('pending', 'failed_temp')
                ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, input_index ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE sites
                SET status = 'running',
                    started_at = CURRENT_TIMESTAMP,
                    finished_at = '',
                    last_error = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    ai_email_ms = 0,
                    search_rep_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0
                WHERE id = ?
                """,
                (int(row["id"]),),
            )
            return SiteTask(
                id=int(row["id"]),
                input_index=int(row["input_index"]),
                website=str(row["website"]),
                dedupe_key=str(row["dedupe_key"]),
                retry_count=int(row["retry_count"] or 0),
                company_name=str(row["input_company_name"] or ""),
            )

    def mark_done(self, site_id: int, result: SiteResult) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'done',
                    company_name = ?,
                    representative = ?,
                    emails = ?,
                    website = ?,
                    phones = ?,
                    searched_representative = ?,
                    searched_representative_evidence_url = ?,
                    searched_representative_confidence = ?,
                    evidence_url = ?,
                    evidence_quote = ?,
                    finished_at = CURRENT_TIMESTAMP,
                    last_error = ''
                WHERE id = ?
                """,
                (
                    result.company_name,
                    result.representative,
                    result.emails,
                    result.website,
                    result.phones,
                    result.searched_representative,
                    result.searched_representative_evidence_url,
                    result.searched_representative_confidence,
                    result.evidence_url,
                    result.evidence_quote,
                    site_id,
                ),
            )
            self._cache_terminal_site(conn, site_id)

    def update_stage_metrics(self, site_id: int, metrics: SiteStageMetrics) -> None:
        values = tuple(int(getattr(metrics, name) or 0) for name in _METRIC_COLUMNS)
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET discover_ms = ?,
                    llm_pick_ms = ?,
                    fetch_pages_ms = ?,
                    llm_extract_ms = ?,
                    ai_email_ms = ?,
                    search_rep_ms = ?,
                    email_rule_ms = ?,
                    company_rule_ms = ?,
                    discovered_url_count = ?,
                    rep_url_count = ?,
                    email_url_count = ?,
                    target_url_count = ?,
                    fetched_page_count = ?
                WHERE id = ?
                """,
                (*values, site_id),
            )

    def load_stage_metrics(self, site_id: int) -> SiteStageMetrics:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT {', '.join(_METRIC_COLUMNS)} FROM sites WHERE id = ?",
                (site_id,),
            ).fetchone()
        if row is None:
            return SiteStageMetrics()
        return SiteStageMetrics(**{name: int(row[name] or 0) for name in _METRIC_COLUMNS})

    def mark_failed(self, site_id: int, error_text: str) -> str:
        with self._write_lock, self._connect() as conn:
            row = conn.execute("SELECT retry_count FROM sites WHERE id = ?", (site_id,)).fetchone()
            retry_count = int(row["retry_count"] or 0) if row is not None else 0
            max_retry_count = _max_retry_count_for_error(error_text)
            if retry_count < max_retry_count:
                conn.execute(
                    """
                    UPDATE sites
                    SET status = 'failed_temp',
                        retry_count = retry_count + 1,
                        last_error = ?,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (error_text, site_id),
                )
                return "failed_temp"
            conn.execute(
                """
                UPDATE sites
                SET status = 'dropped',
                    retry_count = retry_count + 1,
                    last_error = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (error_text, site_id),
            )
            self._cache_terminal_site(conn, site_id)
            return "dropped"

    def mark_dropped(self, site_id: int, error_text: str) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'dropped',
                    last_error = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (error_text, site_id),
            )
            self._cache_terminal_site(conn, site_id)

    def progress(self) -> dict[str, int]:
        with self._connect() as conn:
            counts = {
                status: int(
                    conn.execute("SELECT COUNT(*) AS cnt FROM sites WHERE status = ?", (status,)).fetchone()["cnt"]
                )
                for status in ("pending", "running", "done", "failed_temp", "dropped")
            }
            counts["total"] = int(conn.execute("SELECT COUNT(*) AS cnt FROM sites").fetchone()["cnt"])
            return counts

    def delivery_rows(self) -> list[dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT company_name, representative, emails, searched_representative, phones, website
                FROM sites
                WHERE status IN ('done', 'dropped')
                ORDER BY input_index ASC
                """
            ).fetchall()
        return [
            {
                "company_name": str(row["company_name"] or ""),
                "representative": str(row["representative"] or ""),
                "emails": str(row["emails"] or ""),
                "searched_representative": str(row["searched_representative"] or ""),
                "phones": str(row["phones"] or ""),
                "website": str(row["website"] or ""),
            }
            for row in rows
        ]

    def delivery_report_rows(self) -> list[dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT input_company_name,
                       company_name,
                       representative,
                       emails,
                       searched_representative,
                       phones,
                       website,
                       status,
                       last_error
                FROM sites
                WHERE status IN ('done', 'dropped')
                ORDER BY input_index ASC
                """
            ).fetchall()
        return [
            {
                "input_company_name": str(row["input_company_name"] or ""),
                "company_name": str(row["company_name"] or ""),
                "representative": str(row["representative"] or ""),
                "emails": str(row["emails"] or ""),
                "searched_representative": str(row["searched_representative"] or ""),
                "phones": str(row["phones"] or ""),
                "website": str(row["website"] or ""),
                "status": str(row["status"] or ""),
                "last_error": str(row["last_error"] or ""),
            }
            for row in rows
        ]

    def load_learned_tokens(self, kind: str) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT token, weight FROM learned_tokens WHERE kind = ? ORDER BY weight DESC, token ASC",
                (kind,),
            ).fetchall()
        return {str(row["token"]): int(row["weight"] or 0) for row in rows}

    def bump_learned_tokens(self, kind: str, tokens: list[str]) -> None:
        cleaned = [str(token or "").strip().lower() for token in tokens if str(token or "").strip()]
        if not cleaned:
            return
        with self._write_lock, self._connect() as conn:
            for token in cleaned:
                conn.execute(
                    """
                    INSERT INTO learned_tokens(kind, token, weight)
                    VALUES(?, ?, 1)
                    ON CONFLICT(kind, token) DO UPDATE SET
                        weight = learned_tokens.weight + 1,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (kind, token),
                )

    def _cache_terminal_site(self, conn: sqlite3.Connection, site_id: int) -> None:
        row = conn.execute(
            """
            SELECT dedupe_key,
                   status,
                   last_error,
                   company_name,
                   representative,
                   emails,
                   website,
                   phones,
                   searched_representative,
                   searched_representative_evidence_url,
                   searched_representative_confidence,
                   evidence_url,
                   evidence_quote
            FROM sites
            WHERE id = ? AND status IN ('done', 'dropped')
            """,
            (site_id,),
        ).fetchone()
        if row is not None:
            self._upsert_result_cache_row(conn, row)

    def _backfill_result_cache(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT dedupe_key,
                   status,
                   last_error,
                   company_name,
                   representative,
                   emails,
                   website,
                   phones,
                   searched_representative,
                   searched_representative_evidence_url,
                   searched_representative_confidence,
                   evidence_url,
                   evidence_quote
            FROM sites
            WHERE status IN ('done', 'dropped') AND dedupe_key != ''
            """
        ).fetchall()
        for row in rows:
            self._upsert_result_cache_row(conn, row)

    def _upsert_result_cache_row(self, conn: sqlite3.Connection, row: sqlite3.Row) -> None:
        dedupe_key = str(row["dedupe_key"] or "").strip()
        status = str(row["status"] or "").strip()
        if not dedupe_key or status not in {"done", "dropped"}:
            return
        conn.execute(
            """
            INSERT INTO site_result_cache(
                dedupe_key,
                status,
                last_error,
                company_name,
                representative,
                emails,
                website,
                phones,
                searched_representative,
                searched_representative_evidence_url,
                searched_representative_confidence,
                evidence_url,
                evidence_quote
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                status = excluded.status,
                last_error = excluded.last_error,
                company_name = excluded.company_name,
                representative = excluded.representative,
                emails = excluded.emails,
                website = excluded.website,
                phones = excluded.phones,
                searched_representative = excluded.searched_representative,
                searched_representative_evidence_url = excluded.searched_representative_evidence_url,
                searched_representative_confidence = excluded.searched_representative_confidence,
                evidence_url = excluded.evidence_url,
                evidence_quote = excluded.evidence_quote,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                dedupe_key,
                status,
                str(row["last_error"] or ""),
                str(row["company_name"] or ""),
                str(row["representative"] or ""),
                str(row["emails"] or ""),
                str(row["website"] or ""),
                str(row["phones"] or ""),
                str(row["searched_representative"] or ""),
                str(row["searched_representative_evidence_url"] or ""),
                str(row["searched_representative_confidence"] or ""),
                str(row["evidence_url"] or ""),
                str(row["evidence_quote"] or ""),
            ),
        )


def _connection_is_alive(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False


def _close_connection_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except sqlite3.Error:
        return None


_METRIC_COLUMNS = tuple(field.name for field in fields(SiteStageMetrics))
_FAST_FAIL_TLS_ERROR_HINTS = (
    "tls connect error",
    "tlsv1_alert",
    "sslv3_alert_handshake_failure",
    "handshake failure",
    "openssl_internal:invalid library",
)


def _max_retry_count_for_error(error_text: str) -> int:
    lowered = str(error_text or "").lower()
    if any(token in lowered for token in _FAST_FAIL_TLS_ERROR_HINTS):
        return 0
    if any(
        token in lowered
        for token in (
            "getaddrinfo() thread failed to start",
            "thread failed to start",
            "request_slot_timeout",
            "llm_queue_timeout",
            "service_temporarily_unavailable",
            "llm 服务暂时不可用",
            "resource temporarily unavailable",
            "[errno 35]",
            "page_batch_timeout",
            "empty_page_batch",
            "site_deadline_exceeded",
            "temporary_request:",
        )
    ):
        return 2
    return 1
