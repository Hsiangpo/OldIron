"""Proff SQLite 断点存储。"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
import re

from denmark_crawler.sites.proff.client import TASK_KEY_FILTER_DELIMITER
from denmark_crawler.sites.proff.client import TASK_KEY_INDUSTRY_DELIMITER
from denmark_crawler.sites.proff.models import ProffCompany
from denmark_crawler.sites.proff.models import ProffSearchTask


def run_with_sqlite_retry(
    conn: sqlite3.Connection,
    operation,
    *,
    attempts: int = 6,
    base_delay: float = 0.05,
    cap_delay: float = 0.5,
):
    """遇到 SQLite 锁冲突时短退避重试。"""
    for attempt in range(max(int(attempts), 1)):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= max(int(attempts), 1):
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(min(base_delay * (2**attempt), cap_delay))
    raise RuntimeError("sqlite retry unreachable")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_domain(url: str) -> str:
    value = str(url or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        value = value.split("://", 1)[1]
    value = value.split("/", 1)[0]
    if value.startswith("www."):
        value = value[4:]
    return value


def _normalize_phone(value: str) -> str:
    text = re.sub(r"[^\d+]+", "", str(value or "").strip())
    if text.startswith("+45"):
        text = text[3:]
    return text


def _dump_json_list(items: list[str]) -> str:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _parse_json_list(raw: str) -> list[str]:
    try:
        payload = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    values: list[str] = []
    for item in payload:
        text = str(item or "").strip().lower()
        if text and text not in values:
            values.append(text)
    return values


def _tmp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl_atomic(path: Path, rows: list[dict[str, object]]) -> None:
    tmp_path = _tmp_path(path)
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    tmp_path.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp_path, path)


@dataclass(slots=True)
class GMapTask:
    """Proff 的 GMap 任务。"""

    orgnr: str
    company_name: str
    address: str
    proff_phone: str
    retries: int


@dataclass(slots=True)
class FirecrawlTask:
    """Proff 的 Firecrawl 任务。"""

    orgnr: str
    company_name: str
    website: str
    domain: str
    override_mode: str
    retries: int


@dataclass(slots=True)
class ProffProgress:
    """Proff 运行进度。"""

    search_total: int
    search_done: int
    search_pending: int
    search_running: int
    gmap_pending: int
    gmap_running: int
    firecrawl_pending: int
    firecrawl_running: int
    companies_total: int
    final_total: int


class ProffStore:
    """Proff 断点与快照存储。"""

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
                CREATE TABLE IF NOT EXISTS search_tasks (
                    query TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    total_pages INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(query, page)
                );
                CREATE TABLE IF NOT EXISTS companies (
                    orgnr TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    representative_role TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    proff_phone TEXT NOT NULL DEFAULT '',
                    gmap_phone TEXT NOT NULL DEFAULT '',
                    gmap_company_name TEXT NOT NULL DEFAULT '',
                    override_mode TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    source_query TEXT NOT NULL DEFAULT '',
                    source_page INTEGER NOT NULL DEFAULT 0,
                    source_url TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    gmap_status TEXT NOT NULL DEFAULT '',
                    firecrawl_status TEXT NOT NULL DEFAULT '',
                    firecrawl_retry_at TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gmap_queue (
                    orgnr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS firecrawl_queue (
                    orgnr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS final_companies (
                    orgnr TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    representative TEXT NOT NULL,
                    email TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(orgnr, representative, email)
                );
                CREATE INDEX IF NOT EXISTS idx_proff_search_claim
                ON search_tasks(status, next_run_at, updated_at, query, page);
                CREATE INDEX IF NOT EXISTS idx_proff_gmap_claim
                ON gmap_queue(status, next_run_at, updated_at, orgnr);
                CREATE INDEX IF NOT EXISTS idx_proff_firecrawl_claim
                ON firecrawl_queue(status, next_run_at, updated_at, orgnr);
                """
            )
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            for table in ("search_tasks", "gmap_queue", "firecrawl_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (now,),
                )
            self._conn.commit()

    def ensure_search_seed(self, queries: list[str]) -> None:
        now = _utc_now()
        with self._lock:
            for query in queries:
                self._conn.execute(
                    """
                    INSERT INTO search_tasks(query, page, total_pages, status, retries, next_run_at, last_error, updated_at)
                    VALUES(?, 1, 0, 'pending', 0, ?, '', ?)
                    ON CONFLICT(query, page) DO NOTHING
                    """,
                    (query, now, now),
                )
            self._conn.commit()

    def search_task_count(self) -> int:
        return self._scalar("SELECT COUNT(*) FROM search_tasks")

    def has_segmented_search_tasks(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM search_tasks WHERE query LIKE ? LIMIT 1",
                (f"%{TASK_KEY_FILTER_DELIMITER}%",),
            ).fetchone()
            if row is not None:
                return True
            row = self._conn.execute(
                "SELECT 1 FROM search_tasks WHERE query LIKE ? LIMIT 1",
                (f"%{TASK_KEY_INDUSTRY_DELIMITER}%",),
            ).fetchone()
            return row is not None

    def reseed_search_tasks(self, task_keys: list[str]) -> int:
        now = _utc_now()
        clean_keys = [str(item).strip() for item in task_keys if str(item).strip()]
        if not clean_keys:
            return 0
        with self._lock:
            self._conn.execute("DELETE FROM search_tasks")
            self._conn.executemany(
                """
                INSERT INTO search_tasks(query, page, total_pages, status, retries, next_run_at, last_error, updated_at)
                VALUES(?, 1, 0, 'pending', 0, ?, '', ?)
                """,
                [(task_key, now, now) for task_key in clean_keys],
            )
            self._conn.commit()
            return len(clean_keys)

    def claim_search_task(self) -> ProffSearchTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT query, page, retries FROM search_tasks
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY query ASC, page ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE search_tasks SET status = 'running', updated_at = ? WHERE query = ? AND page = ?",
                (now, str(row["query"]), int(row["page"])),
            )
            self._conn.commit()
            return ProffSearchTask(
                query=str(row["query"]),
                page=int(row["page"]),
                retries=int(row["retries"] or 0),
            )

    def mark_search_done(self, *, query: str, page: int, total_pages: int, max_pages_per_query: int) -> None:
        now = _utc_now()
        capped_total = min(max(int(total_pages), 0), max(int(max_pages_per_query), 1), 400)
        next_page = int(page) + 1
        with self._lock:
            self._conn.execute(
                """
                UPDATE search_tasks
                SET status = 'done', total_pages = ?, last_error = '', updated_at = ?
                WHERE query = ? AND page = ?
                """,
                (capped_total, now, query, page),
            )
            if capped_total > 0 and next_page <= capped_total:
                self._conn.execute(
                    """
                    INSERT INTO search_tasks(query, page, total_pages, status, retries, next_run_at, last_error, updated_at)
                    VALUES(?, ?, 0, 'pending', 0, ?, '', ?)
                    ON CONFLICT(query, page) DO NOTHING
                    """,
                    (query, next_page, now, now),
                )
            self._conn.commit()

    def defer_search_task(self, *, query: str, page: int, retries: int, delay_seconds: float, error_text: str) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    """
                    UPDATE search_tasks
                    SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ?
                    WHERE query = ? AND page = ?
                    """,
                    (retries, _utc_after(delay_seconds), str(error_text)[:500], _utc_now(), query, page),
                )
                self._conn.commit()
            run_with_sqlite_retry(self._conn, _op)

    def mark_search_failed(self, *, query: str, page: int, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE search_tasks
                SET status = 'failed', last_error = ?, updated_at = ?
                WHERE query = ? AND page = ?
                """,
                (str(error_text)[:500], _utc_now(), query, page),
            )
            self._conn.commit()

    def has_search_work(self) -> bool:
        return self._scalar(
            "SELECT COUNT(*) FROM search_tasks WHERE status IN ('pending', 'running')"
        ) > 0

    def claim_gmap_task(self) -> GMapTask | None:
        task = self._claim_queue_task("gmap_queue")
        if task is None:
            return None
        orgnr, retries = task
        company = self.get_company(orgnr) or {}
        return GMapTask(
            orgnr=orgnr,
            company_name=str(company.get("company_name", "")).strip(),
            address=str(company.get("address", "")).strip(),
            proff_phone=str(company.get("proff_phone", "")).strip() or str(company.get("phone", "")).strip(),
            retries=retries,
        )

    def claim_firecrawl_task(self) -> FirecrawlTask | None:
        task = self._claim_queue_task("firecrawl_queue")
        if task is None:
            return None
        orgnr, retries = task
        company = self.get_company(orgnr) or {}
        website = str(company.get("homepage", "")).strip()
        return FirecrawlTask(
            orgnr=orgnr,
            company_name=str(company.get("company_name", "")).strip(),
            website=website,
            domain=str(company.get("domain", "")).strip() or _extract_domain(website),
            override_mode=str(company.get("override_mode", "")).strip(),
            retries=retries,
        )

    def _claim_queue_task(self, table: str) -> tuple[str, int] | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT orgnr, retries FROM {table}
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY updated_at ASC, orgnr ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            orgnr = str(row["orgnr"])
            self._conn.execute(
                f"UPDATE {table} SET status = 'running', updated_at = ? WHERE orgnr = ?",
                (now, orgnr),
            )
            self._conn.commit()
            return orgnr, int(row["retries"] or 0)

    def upsert_company(self, company: ProffCompany) -> None:
        emails = company.emails()
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(company.orgnr)
            merged_emails = _parse_json_list(str(current.get("emails_json", "[]") if current else "[]"))
            for email in emails:
                if email and email not in merged_emails:
                    merged_emails.append(email)
            payload = (
                company.orgnr,
                company.company_name,
                company.representative,
                company.representative_role,
                company.address,
                company.homepage,
                _extract_domain(company.homepage),
                company.phone,
                company.phone,
                str(current.get("gmap_phone", "") if current else ""),
                str(current.get("gmap_company_name", "") if current else ""),
                str(current.get("override_mode", "") if current else ""),
                _dump_json_list(merged_emails),
                company.source_query,
                company.source_page,
                company.source_url,
                json.dumps(company.raw_payload, ensure_ascii=False),
                str(current.get("gmap_status", "") if current else ""),
                str(current.get("firecrawl_status", "") if current else ""),
                str(current.get("firecrawl_retry_at", "") if current else ""),
                str(current.get("last_error", "") if current else ""),
                now,
            )
            self._conn.execute(
                """
                INSERT INTO companies(
                    orgnr, company_name, representative, representative_role, address, homepage, domain,
                    phone, proff_phone, gmap_phone, gmap_company_name, override_mode, emails_json,
                    source_query, source_page, source_url, raw_json,
                    gmap_status, firecrawl_status, firecrawl_retry_at, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(orgnr) DO UPDATE SET
                    company_name = excluded.company_name,
                    representative = CASE WHEN excluded.representative != '' THEN excluded.representative ELSE companies.representative END,
                    representative_role = CASE WHEN excluded.representative_role != '' THEN excluded.representative_role ELSE companies.representative_role END,
                    address = CASE WHEN excluded.address != '' THEN excluded.address ELSE companies.address END,
                    homepage = CASE WHEN excluded.homepage != '' THEN excluded.homepage ELSE companies.homepage END,
                    domain = CASE WHEN excluded.domain != '' THEN excluded.domain ELSE companies.domain END,
                    phone = CASE WHEN excluded.phone != '' THEN excluded.phone ELSE companies.phone END,
                    proff_phone = CASE WHEN excluded.proff_phone != '' THEN excluded.proff_phone ELSE companies.proff_phone END,
                    gmap_phone = CASE WHEN excluded.gmap_phone != '' THEN excluded.gmap_phone ELSE companies.gmap_phone END,
                    gmap_company_name = CASE WHEN excluded.gmap_company_name != '' THEN excluded.gmap_company_name ELSE companies.gmap_company_name END,
                    override_mode = CASE WHEN excluded.override_mode != '' THEN excluded.override_mode ELSE companies.override_mode END,
                    emails_json = excluded.emails_json,
                    source_query = CASE WHEN excluded.source_query != '' THEN excluded.source_query ELSE companies.source_query END,
                    source_page = CASE WHEN excluded.source_page > 0 THEN excluded.source_page ELSE companies.source_page END,
                    source_url = CASE WHEN excluded.source_url != '' THEN excluded.source_url ELSE companies.source_url END,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                payload,
            )
            self._schedule_company_tasks_locked(company.orgnr)
            self._conn.commit()
        self.refresh_final_company(company.orgnr)

    def mark_gmap_done(self, *, orgnr: str, website: str, source: str, phone: str, company_name: str = "") -> None:
        _ = source
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(orgnr) or {}
            final_website = str(website or "").strip() or str(current.get("homepage", "")).strip()
            proff_phone = str(current.get("proff_phone", "")).strip() or str(current.get("phone", "")).strip()
            gmap_phone = str(phone or "").strip()
            final_phone = gmap_phone or str(current.get("phone", "")).strip()
            override_mode = str(current.get("override_mode", "")).strip()
            if proff_phone and gmap_phone and _normalize_phone(proff_phone) != _normalize_phone(gmap_phone):
                override_mode = "website_override"
            self._conn.execute(
                """
                UPDATE companies
                SET homepage = ?, domain = ?, phone = ?, gmap_phone = ?, gmap_company_name = ?, override_mode = ?,
                    gmap_status = 'done', last_error = '', updated_at = ?
                WHERE orgnr = ?
                """,
                (
                    final_website,
                    _extract_domain(final_website),
                    final_phone,
                    gmap_phone,
                    str(company_name or "").strip(),
                    override_mode,
                    now,
                    orgnr,
                ),
            )
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'done', last_error = '', updated_at = ? WHERE orgnr = ?",
                (now, orgnr),
            )
            self._schedule_company_tasks_locked(orgnr)
            self._conn.commit()
        self.refresh_final_company(orgnr)

    def mark_firecrawl_done(
        self,
        *,
        orgnr: str,
        emails: list[str],
        representative: str = "",
        company_name: str = "",
        retry_after_seconds: float = 0.0,
    ) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(orgnr) or {}
            merged = _parse_json_list(str(current.get("emails_json", "[]")))
            for email in emails:
                value = str(email or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            has_email = bool(merged)
            override_mode = str(current.get("override_mode", "")).strip()
            next_company_name = str(current.get("company_name", "")).strip()
            next_representative = str(current.get("representative", "")).strip()
            next_phone = str(current.get("phone", "")).strip()
            if override_mode == "website_override":
                next_company_name = str(company_name or "").strip() or str(current.get("gmap_company_name", "")).strip()
                next_representative = str(representative or "").strip()
                next_phone = str(current.get("gmap_phone", "")).strip() or next_phone
            elif not next_representative and str(representative or "").strip():
                next_representative = str(representative).strip()
            self._conn.execute(
                """
                UPDATE companies
                SET company_name = ?, representative = ?, phone = ?, emails_json = ?, firecrawl_status = ?,
                    firecrawl_retry_at = ?, last_error = '', updated_at = ?
                WHERE orgnr = ?
                """,
                (
                    next_company_name,
                    next_representative,
                    next_phone,
                    _dump_json_list(merged),
                    "done" if has_email else "zero",
                    "" if has_email or retry_after_seconds <= 0 else _utc_after(retry_after_seconds),
                    now,
                    orgnr,
                ),
            )
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'done', last_error = '', updated_at = ? WHERE orgnr = ?",
                (now, orgnr),
            )
            self._conn.commit()
        self.refresh_final_company(orgnr)

    def requeue_expired_firecrawl_tasks(self) -> int:
        now = _utc_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT orgnr FROM companies
                WHERE firecrawl_status = 'zero' AND firecrawl_retry_at != '' AND firecrawl_retry_at <= ?
                """,
                (now,),
            ).fetchall()
            if not rows:
                return 0
            for row in rows:
                orgnr = str(row["orgnr"])
                self._enqueue_queue_locked("firecrawl_queue", orgnr)
                self._conn.execute(
                    """
                    UPDATE companies
                    SET firecrawl_status = 'pending', firecrawl_retry_at = '', updated_at = ?
                    WHERE orgnr = ?
                    """,
                    (now, orgnr),
                )
            self._conn.commit()
            return len(rows)

    def defer_gmap_task(self, *, orgnr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_queue_task(
            "gmap_queue",
            status_field="gmap_status",
            orgnr=orgnr,
            retries=retries,
            delay_seconds=delay_seconds,
            error_text=error_text,
        )

    def defer_firecrawl_task(self, *, orgnr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_queue_task(
            "firecrawl_queue",
            status_field="firecrawl_status",
            orgnr=orgnr,
            retries=retries,
            delay_seconds=delay_seconds,
            error_text=error_text,
        )

    def mark_gmap_failed(self, *, orgnr: str, error_text: str) -> None:
        self._mark_queue_failed("gmap_queue", status_field="gmap_status", orgnr=orgnr, error_text=error_text)

    def mark_firecrawl_failed(self, *, orgnr: str, error_text: str) -> None:
        self._mark_queue_failed("firecrawl_queue", status_field="firecrawl_status", orgnr=orgnr, error_text=error_text)

    def _defer_queue_task(
        self,
        table: str,
        *,
        status_field: str,
        orgnr: str,
        retries: int,
        delay_seconds: float,
        error_text: str,
    ) -> None:
        with self._lock:
            def _op() -> None:
                now = _utc_now()
                self._conn.execute(
                    f"""
                    UPDATE {table}
                    SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ?
                    WHERE orgnr = ?
                    """,
                    (retries, _utc_after(delay_seconds), str(error_text)[:500], now, orgnr),
                )
                self._conn.execute(
                    f"UPDATE companies SET {status_field} = 'pending', last_error = ?, updated_at = ? WHERE orgnr = ?",
                    (str(error_text)[:500], now, orgnr),
                )
                self._conn.commit()
            run_with_sqlite_retry(self._conn, _op)

    def _mark_queue_failed(self, table: str, *, status_field: str, orgnr: str, error_text: str) -> None:
        with self._lock:
            now = _utc_now()
            self._conn.execute(
                f"UPDATE {table} SET status = 'failed', last_error = ?, updated_at = ? WHERE orgnr = ?",
                (str(error_text)[:500], now, orgnr),
            )
            self._conn.execute(
                f"UPDATE companies SET {status_field} = 'failed', last_error = ?, updated_at = ? WHERE orgnr = ?",
                (str(error_text)[:500], now, orgnr),
            )
            self._conn.commit()

    def get_company(self, orgnr: str) -> dict[str, object] | None:
        with self._lock:
            return self._fetch_company_locked(orgnr)

    def refresh_final_company(self, orgnr: str) -> None:
        with self._lock:
            record = self._fetch_company_locked(orgnr)
            if record is None:
                return
            emails = _parse_json_list(str(record.get("emails_json", "[]")))
            company_name = str(record.get("company_name", "")).strip()
            representative = str(record.get("representative", "")).strip()
            self._conn.execute("DELETE FROM final_companies WHERE orgnr = ?", (orgnr,))
            if company_name and representative and emails:
                now = _utc_now()
                for email in emails:
                    self._conn.execute(
                        """
                        INSERT INTO final_companies(orgnr, company_name, representative, email, source, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (
                            orgnr,
                            company_name,
                            representative,
                            email,
                            str(record.get("override_mode", "")).strip() or "proff",
                            now,
                        ),
                    )
            self._conn.commit()

    def company_count(self) -> int:
        return self._scalar("SELECT COUNT(*) FROM companies")

    def get_progress(self) -> ProffProgress:
        return ProffProgress(
            search_total=self._scalar("SELECT COUNT(*) FROM search_tasks"),
            search_done=self._scalar("SELECT COUNT(*) FROM search_tasks WHERE status = 'done'"),
            search_pending=self._scalar("SELECT COUNT(*) FROM search_tasks WHERE status = 'pending'"),
            search_running=self._scalar("SELECT COUNT(*) FROM search_tasks WHERE status = 'running'"),
            gmap_pending=self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'pending'"),
            gmap_running=self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'running'"),
            firecrawl_pending=self._scalar("SELECT COUNT(*) FROM firecrawl_queue WHERE status = 'pending'"),
            firecrawl_running=self._scalar("SELECT COUNT(*) FROM firecrawl_queue WHERE status = 'running'"),
            companies_total=self._scalar("SELECT COUNT(*) FROM companies"),
            final_total=self._scalar("SELECT COUNT(*) FROM final_companies"),
        )

    def requeue_stale_running_tasks(self, *, older_than_seconds: int) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(int(older_than_seconds), 1))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        total = 0
        with self._lock:
            search_rows = self._conn.execute(
                "SELECT query, page FROM search_tasks WHERE status = 'running' AND updated_at <= ?",
                (cutoff,),
            ).fetchall()
            if search_rows:
                self._conn.execute(
                    """
                    UPDATE search_tasks
                    SET status = 'pending', next_run_at = ?, updated_at = ?
                    WHERE status = 'running' AND updated_at <= ?
                    """,
                    (now, now, cutoff),
                )
                total += len(search_rows)
            for table in ("gmap_queue", "firecrawl_queue"):
                rows = self._conn.execute(
                    f"SELECT orgnr FROM {table} WHERE status = 'running' AND updated_at <= ?",
                    (cutoff,),
                ).fetchall()
                if not rows:
                    continue
                self._conn.execute(
                    f"""
                    UPDATE {table}
                    SET status = 'pending', next_run_at = ?, updated_at = ?
                    WHERE status = 'running' AND updated_at <= ?
                    """,
                    (now, now, cutoff),
                )
                total += len(rows)
            self._conn.commit()
        return total

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000;")
        try:
            rows = conn.execute("SELECT * FROM companies ORDER BY orgnr ASC").fetchall()
        finally:
            conn.close()
        companies_rows: list[dict[str, object]] = []
        enriched_rows: list[dict[str, object]] = []
        with_emails_rows: list[dict[str, object]] = []
        for row in rows:
            record = dict(row)
            emails = _parse_json_list(str(record.get("emails_json", "[]")))
            base = {
                "comp_id": str(record.get("orgnr", "")),
                "orgnr": str(record.get("orgnr", "")),
                "company_name": str(record.get("company_name", "")).strip(),
                "ceo": str(record.get("representative", "")).strip(),
                "homepage": "",
                "domain": "",
                "phone": str(record.get("phone", "")).strip(),
                "emails": [],
                "source_query": str(record.get("source_query", "")).strip(),
            }
            companies_rows.append(base)
            enriched = dict(base)
            enriched["homepage"] = str(record.get("homepage", "")).strip()
            enriched["domain"] = str(record.get("domain", "")).strip()
            enriched_rows.append(enriched)
            if not (base["company_name"] and base["ceo"] and emails):
                continue
            for email in emails:
                with_emails_rows.append(
                    {
                        "comp_id": base["comp_id"],
                        "orgnr": base["orgnr"],
                        "company_name": base["company_name"],
                        "representative": base["ceo"],
                        "email": email,
                        "homepage": str(record.get("homepage", "")).strip(),
                        "domain": str(record.get("domain", "")).strip(),
                        "phone": base["phone"],
                        "address": str(record.get("address", "")).strip(),
                        "source_query": base["source_query"],
                        "representative_role": str(record.get("representative_role", "")).strip(),
                        "override_mode": str(record.get("override_mode", "")).strip(),
                    }
                )
        _write_jsonl_atomic(output_dir / "companies.jsonl", companies_rows)
        _write_jsonl_atomic(output_dir / "companies_enriched.jsonl", enriched_rows)
        _write_jsonl_atomic(output_dir / "companies_with_emails.jsonl", with_emails_rows)
        _write_jsonl_atomic(output_dir / "final_companies.jsonl", with_emails_rows)

    def _schedule_company_tasks_locked(self, orgnr: str) -> None:
        record = self._fetch_company_locked(orgnr)
        if record is None:
            return
        emails = _parse_json_list(str(record.get("emails_json", "[]")))
        representative = str(record.get("representative", "")).strip()
        homepage = str(record.get("homepage", "")).strip()
        now = _utc_now()
        if representative and emails:
            self._mark_queue_done_locked("gmap_queue", orgnr, now)
            self._mark_queue_done_locked("firecrawl_queue", orgnr, now)
            return
        if homepage:
            self._mark_queue_done_locked("gmap_queue", orgnr, now)
            if str(record.get("firecrawl_status", "")).strip() in {"", "failed"}:
                self._enqueue_queue_locked("firecrawl_queue", orgnr)
                self._conn.execute(
                    "UPDATE companies SET firecrawl_status = 'pending', firecrawl_retry_at = '', updated_at = ? WHERE orgnr = ?",
                    (now, orgnr),
                )
            return
        if str(record.get("company_name", "")).strip() and str(record.get("gmap_status", "")).strip() in {"", "failed"}:
            self._enqueue_queue_locked("gmap_queue", orgnr)
            self._conn.execute(
                "UPDATE companies SET gmap_status = 'pending', updated_at = ? WHERE orgnr = ?",
                (now, orgnr),
            )
        self._mark_queue_done_locked("firecrawl_queue", orgnr, now)

    def _enqueue_queue_locked(self, table: str, orgnr: str) -> None:
        now = _utc_now()
        self._conn.execute(
            f"""
            INSERT INTO {table}(orgnr, status, retries, next_run_at, last_error, updated_at)
            VALUES(?, 'pending', 0, ?, '', ?)
            ON CONFLICT(orgnr) DO UPDATE SET
                status = 'pending',
                retries = 0,
                next_run_at = excluded.next_run_at,
                last_error = '',
                updated_at = excluded.updated_at
            """,
            (orgnr, now, now),
        )

    def _mark_queue_done_locked(self, table: str, orgnr: str, now: str) -> None:
        self._conn.execute(
            f"""
            INSERT INTO {table}(orgnr, status, retries, next_run_at, last_error, updated_at)
            VALUES(?, 'done', 0, ?, '', ?)
            ON CONFLICT(orgnr) DO UPDATE SET
                status = 'done',
                retries = 0,
                last_error = '',
                updated_at = excluded.updated_at
            """,
            (orgnr, now, now),
        )

    def _fetch_company_locked(self, orgnr: str) -> dict[str, object] | None:
        row = self._conn.execute("SELECT * FROM companies WHERE orgnr = ?", (orgnr,)).fetchone()
        return dict(row) if row is not None else None

    def _scalar(self, sql: str) -> int:
        with self._lock:
            row = self._conn.execute(sql).fetchone()
            return int(row[0]) if row is not None else 0
