"""Companies House 站点 sqlite 存储。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from england_crawler.companies_house.client import normalize_company_name
from england_crawler.companies_house.models import CompanyTask
from england_crawler.companies_house.snapshot_export import export_jsonl_snapshots
from england_crawler.google_maps.pipeline import clean_homepage
from england_crawler.snov.client import extract_domain
from england_crawler.snov.client import is_valid_domain


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _dump_json_list(items: list[str]) -> str:
    values: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in values:
            values.append(text)
    return json.dumps(values, ensure_ascii=False)


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def _build_comp_id(normalized_name: str) -> str:
    return hashlib.md5(normalized_name.encode("utf-8")).hexdigest()


def _source_key(source_path: Path, scope: str) -> str:
    return f"{source_path.resolve()}|{scope.strip() or 'full'}"


class CompaniesHouseStore:
    """英国 xlsx + Companies House 队列存储。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout = 30000;")
        self._init_schema()
        self._repair_runtime_state()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS source_files (
                    source_path TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    total_rows INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS companies (
                    comp_id TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    company_number TEXT NOT NULL DEFAULT '',
                    company_status TEXT NOT NULL DEFAULT '',
                    ceo TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    ch_status TEXT NOT NULL DEFAULT 'pending',
                    gmap_status TEXT NOT NULL DEFAULT 'pending',
                    snov_status TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_companies_normalized_name ON companies(normalized_name);
                CREATE TABLE IF NOT EXISTS ch_queue (
                    comp_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gmap_queue (
                    comp_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS snov_queue (
                    comp_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ch_queue_claim
                ON ch_queue(status, next_run_at, updated_at, comp_id);
                CREATE INDEX IF NOT EXISTS idx_gmap_queue_claim
                ON gmap_queue(status, next_run_at, updated_at, comp_id);
                CREATE INDEX IF NOT EXISTS idx_snov_queue_claim
                ON snov_queue(status, next_run_at, updated_at, comp_id);
                """
            )
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            for table in ("ch_queue", "gmap_queue", "snov_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running'",
                    (now, now),
                )
            self._conn.commit()

    def source_is_loaded(self, source_path: Path, fingerprint: str, *, scope: str = "full") -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT fingerprint FROM source_files WHERE source_path = ?",
                (_source_key(source_path, scope),),
            ).fetchone()
            return row is not None and str(row["fingerprint"]) == fingerprint

    def mark_source_loaded(
        self,
        source_path: Path,
        fingerprint: str,
        total_rows: int,
        *,
        scope: str = "full",
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO source_files(source_path, fingerprint, total_rows, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    fingerprint = excluded.fingerprint,
                    total_rows = excluded.total_rows,
                    updated_at = excluded.updated_at
                """,
                (_source_key(source_path, scope), fingerprint, max(int(total_rows), 0), now),
            )
            self._conn.commit()

    def import_company_names(self, company_names: list[str]) -> int:
        inserted = 0
        now = _utc_now()
        with self._lock:
            for company_name in company_names:
                normalized = normalize_company_name(company_name)
                if not normalized:
                    continue
                comp_id = _build_comp_id(normalized)
                existing = self._conn.execute(
                    "SELECT comp_id FROM companies WHERE comp_id = ?",
                    (comp_id,),
                ).fetchone()
                if existing is None:
                    inserted += 1
                self._conn.execute(
                    """
                    INSERT INTO companies(
                        comp_id, company_name, normalized_name, updated_at
                    ) VALUES(?, ?, ?, ?)
                    ON CONFLICT(comp_id) DO UPDATE SET
                        company_name = CASE
                            WHEN TRIM(companies.company_name) = '' THEN excluded.company_name
                            ELSE companies.company_name
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (comp_id, company_name.strip(), normalized, now),
                )
                self._enqueue_task_locked("ch_queue", comp_id, now)
                self._enqueue_task_locked("gmap_queue", comp_id, now)
            self._conn.commit()
        return inserted

    def import_companies(self, rows: list[tuple[str, str, str]]) -> int:
        """兼容测试与小批量导入。"""
        now = _utc_now()
        inserted = 0
        with self._lock:
            for comp_id, company_name, normalized_name in rows:
                normalized = normalize_company_name(normalized_name or company_name)
                target_comp_id = str(comp_id).strip() or _build_comp_id(normalized)
                if not normalized or not target_comp_id:
                    continue
                existing = self._conn.execute(
                    "SELECT comp_id FROM companies WHERE comp_id = ?",
                    (target_comp_id,),
                ).fetchone()
                if existing is None:
                    inserted += 1
                self._conn.execute(
                    """
                    INSERT INTO companies(
                        comp_id, company_name, normalized_name, updated_at
                    ) VALUES(?, ?, ?, ?)
                    ON CONFLICT(comp_id) DO UPDATE SET
                        company_name = CASE
                            WHEN TRIM(companies.company_name) = '' THEN excluded.company_name
                            ELSE companies.company_name
                        END,
                        normalized_name = excluded.normalized_name,
                        updated_at = excluded.updated_at
                    """,
                    (target_comp_id, str(company_name).strip(), normalized, now),
                )
                self._enqueue_task_locked("ch_queue", target_comp_id, now)
                self._enqueue_task_locked("gmap_queue", target_comp_id, now)
            self._conn.commit()
        return inserted

    def _enqueue_task_locked(self, table: str, comp_id: str, now: str) -> None:
        self._conn.execute(
            f"""
            INSERT INTO {table}(comp_id, status, retries, next_run_at, last_error, updated_at)
            VALUES(?, 'pending', 0, ?, '', ?)
            ON CONFLICT(comp_id) DO UPDATE SET
                status = CASE WHEN {table}.status = 'done' THEN {table}.status ELSE 'pending' END,
                updated_at = excluded.updated_at,
                next_run_at = CASE WHEN {table}.status = 'done' THEN {table}.next_run_at ELSE excluded.next_run_at END,
                last_error = CASE WHEN {table}.status = 'done' THEN {table}.last_error ELSE '' END
            """,
            (comp_id, now, now),
        )

    def claim_ch_task(self) -> CompanyTask | None:
        return self._claim_task("ch_queue")

    def claim_gmap_task(self) -> CompanyTask | None:
        return self._claim_task("gmap_queue")

    def claim_snov_task(self) -> CompanyTask | None:
        return self._claim_task("snov_queue")

    def claim_firecrawl_task(self) -> CompanyTask | None:
        return self._claim_task("snov_queue")

    def _claim_task(self, table: str) -> CompanyTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT q.comp_id, q.retries, c.company_name, c.company_number, c.homepage, c.domain
                FROM {table} q
                JOIN companies c ON c.comp_id = q.comp_id
                WHERE q.status = 'pending' AND q.next_run_at <= ?
                ORDER BY q.next_run_at ASC, q.updated_at ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                f"UPDATE {table} SET status = 'running', updated_at = ? WHERE comp_id = ?",
                (now, str(row["comp_id"])),
            )
            self._conn.commit()
        return CompanyTask(
            comp_id=str(row["comp_id"]),
            company_name=str(row["company_name"]),
            company_number=str(row["company_number"]),
            homepage=str(row["homepage"]),
            domain=str(row["domain"]),
            retries=int(row["retries"]),
        )

    def mark_ch_done(
        self,
        *,
        comp_id: str,
        company_number: str,
        company_status: str,
        ceo: str,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE companies
                SET company_number = ?, company_status = ?, ceo = ?, ch_status = 'done',
                    last_error = '', updated_at = ?
                WHERE comp_id = ?
                """,
                (
                    company_number.strip(),
                    company_status.strip(),
                    ceo.strip(),
                    now,
                    comp_id,
                ),
            )
            self._conn.execute(
                "UPDATE ch_queue SET status = 'done', updated_at = ?, last_error = '' WHERE comp_id = ?",
                (now, comp_id),
            )
            self._queue_firecrawl_if_ready_locked(comp_id, now)
            self._conn.commit()

    def mark_gmap_done(self, *, comp_id: str, homepage: str, phone: str) -> None:
        now = _utc_now()
        cleaned_homepage = clean_homepage(homepage)
        domain = extract_domain(cleaned_homepage)
        with self._lock:
            self._conn.execute(
                """
                UPDATE companies
                SET homepage = ?, domain = ?, phone = ?, gmap_status = 'done',
                    last_error = '', updated_at = ?
                WHERE comp_id = ?
                """,
                (cleaned_homepage, domain, phone.strip(), now, comp_id),
            )
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'done', updated_at = ?, last_error = '' WHERE comp_id = ?",
                (now, comp_id),
            )
            self._queue_firecrawl_if_ready_locked(comp_id, now)
            self._conn.commit()

    def mark_firecrawl_done(self, *, comp_id: str, emails: list[str]) -> None:
        self.mark_snov_done(comp_id=comp_id, emails=emails)

    def mark_snov_done(self, *, comp_id: str, emails: list[str]) -> None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT emails_json FROM companies WHERE comp_id = ?",
                (comp_id,),
            ).fetchone()
            if row is None:
                return
            merged = _parse_json_list(str(row["emails_json"])) + emails
            self._conn.execute(
                """
                UPDATE companies
                SET emails_json = ?, snov_status = 'done', last_error = '', updated_at = ?
                WHERE comp_id = ?
                """,
                (_dump_json_list(merged), now, comp_id),
            )
            self._conn.execute(
                "UPDATE snov_queue SET status = 'done', updated_at = ?, last_error = '' WHERE comp_id = ?",
                (now, comp_id),
            )
            self._conn.commit()

    def defer_ch_task(self, *, comp_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("ch_queue", "ch_status", comp_id, retries, delay_seconds, error_text)

    def defer_gmap_task(self, *, comp_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("gmap_queue", "gmap_status", comp_id, retries, delay_seconds, error_text)

    def defer_snov_task(self, *, comp_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("snov_queue", "snov_status", comp_id, retries, delay_seconds, error_text)

    def defer_firecrawl_task(self, *, comp_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("snov_queue", "snov_status", comp_id, retries, delay_seconds, error_text)


    def _defer_task(
        self,
        table: str,
        field_name: str,
        comp_id: str,
        retries: int,
        delay_seconds: float,
        error_text: str,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                f"""
                UPDATE {table}
                SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ?
                WHERE comp_id = ?
                """,
                (max(retries, 0), _utc_after(delay_seconds), error_text[:500], now, comp_id),
            )
            self._conn.execute(
                f"UPDATE companies SET {field_name} = 'pending', last_error = ?, updated_at = ? WHERE comp_id = ?",
                (error_text[:500], now, comp_id),
            )
            self._conn.commit()

    def mark_ch_failed(self, *, comp_id: str, error_text: str) -> None:
        self._mark_failed("ch_queue", "ch_status", comp_id, error_text)

    def mark_gmap_failed(self, *, comp_id: str, error_text: str) -> None:
        self._mark_failed("gmap_queue", "gmap_status", comp_id, error_text)

    def mark_snov_failed(self, *, comp_id: str, error_text: str) -> None:
        self._mark_failed("snov_queue", "snov_status", comp_id, error_text)

    def mark_firecrawl_failed(self, *, comp_id: str, error_text: str) -> None:
        self._mark_failed("snov_queue", "snov_status", comp_id, error_text)


    def _mark_failed(self, table: str, field_name: str, comp_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'failed', last_error = ?, updated_at = ? WHERE comp_id = ?",
                (error_text[:500], now, comp_id),
            )
            self._conn.execute(
                f"UPDATE companies SET {field_name} = 'failed', last_error = ?, updated_at = ? WHERE comp_id = ?",
                (error_text[:500], now, comp_id),
            )
            self._conn.commit()

    def _queue_firecrawl_if_ready_locked(self, comp_id: str, now: str) -> None:
        row = self._conn.execute(
            "SELECT ceo, homepage, domain FROM companies WHERE comp_id = ?",
            (comp_id,),
        ).fetchone()
        if row is None:
            return
        domain = str(row["domain"]).strip() or extract_domain(str(row["homepage"]).strip())
        if not str(row["ceo"]).strip() or not is_valid_domain(domain):
            return
        self._enqueue_task_locked("snov_queue", comp_id, now)


    def get_company(self, comp_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM companies WHERE comp_id = ?",
                (comp_id,),
            ).fetchone()
            if row is None:
                return None
        data = dict(row)
        data["emails"] = _parse_json_list(str(data.get("emails_json", "[]")))
        return data

    def queue_done(self, table: str) -> bool:
        with self._lock:
            remaining = self._scalar(f"SELECT COUNT(*) FROM {table} WHERE status IN ('pending', 'running')")
            return remaining == 0

    def get_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "companies_total": self._scalar("SELECT COUNT(*) FROM companies"),
                "ch_total": self._scalar("SELECT COUNT(*) FROM ch_queue"),
                "ch_done": self._scalar("SELECT COUNT(*) FROM ch_queue WHERE status = 'done'"),
                "ch_pending": self._scalar("SELECT COUNT(*) FROM ch_queue WHERE status = 'pending'"),
                "ch_running": self._scalar("SELECT COUNT(*) FROM ch_queue WHERE status = 'running'"),
                "gmap_total": self._scalar("SELECT COUNT(*) FROM gmap_queue"),
                "gmap_done": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'done'"),
                "gmap_pending": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'pending'"),
                "gmap_running": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'running'"),
                "snov_total": self._scalar("SELECT COUNT(*) FROM snov_queue"),
                "snov_done": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'done'"),
                "snov_pending": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'pending'"),
                "snov_running": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'running'"),
                "firecrawl_total": self._scalar("SELECT COUNT(*) FROM snov_queue"),
                "firecrawl_done": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'done'"),
                "firecrawl_pending": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'pending'"),
                "firecrawl_running": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'running'"),
                "final_total": self._scalar(
                    """
                    SELECT COUNT(*) FROM companies
                    WHERE TRIM(company_name) != '' AND TRIM(ceo) != ''
                      AND TRIM(homepage) != '' AND emails_json != '[]'
                    """
                ),
            }

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        export_jsonl_snapshots(self._db_path, output_dir)

    def requeue_stale_running_tasks(self, *, older_than_seconds: int) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(int(older_than_seconds), 1))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        total = 0
        with self._lock:
            for table in ("ch_queue", "gmap_queue", "snov_queue"):
                rows = self._conn.execute(
                    f"SELECT comp_id FROM {table} WHERE status = 'running' AND updated_at <= ?",
                    (cutoff,),
                ).fetchall()
                if not rows:
                    continue
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running' AND updated_at <= ?",
                    (now, now, cutoff),
                )
                total += len(rows)
            self._conn.commit()
        return total

    def _scalar(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row is not None else 0
