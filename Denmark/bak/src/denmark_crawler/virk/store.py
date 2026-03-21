"""丹麦 Virk sqlite 存储与快照导出。"""

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

from denmark_crawler.dnb.runtime.sqlite_retry import run_with_sqlite_retry
from denmark_crawler.snov.client import extract_domain
from denmark_crawler.virk.models import VirkCompanyRecord
from denmark_crawler.virk.models import VirkSearchCompany


RETRYABLE_ERROR_HINTS = (
    "Just a moment",
    "HTTP 403",
    "HTTP 429",
    "Error 1015",
    "timed out",
    "timeout",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item or "").strip().lower()
        if text and text not in out:
            out.append(text)
    return out


def _dump_json_list(items: list[str]) -> str:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _merge_text(current: str, incoming: str) -> str:
    return str(incoming or "").strip() or str(current or "").strip()


def _tmp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl_atomic(path: Path, rows: list[dict[str, object]]) -> None:
    tmp_path = _tmp_path(path)
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    tmp_path.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp_path, path)


@dataclass(slots=True)
class DetailTask:
    cvr: str
    retries: int


@dataclass(slots=True)
class GMapTask:
    cvr: str
    company_name: str
    city: str
    postal_code: str
    phone: str
    retries: int


@dataclass(slots=True)
class FirecrawlTask:
    cvr: str
    company_name: str
    website: str
    domain: str
    retries: int


class VirkDenmarkStore:
    """Virk 断点、队列和快照存储。"""

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
                CREATE TABLE IF NOT EXISTS search_pages (
                    page_index INTEGER PRIMARY KEY,
                    total_hits INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS companies (
                    cvr TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    postal_code TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT 'Denmark',
                    status TEXT NOT NULL DEFAULT '',
                    company_form TEXT NOT NULL DEFAULT '',
                    main_industry TEXT NOT NULL DEFAULT '',
                    start_date TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    representative TEXT NOT NULL DEFAULT '',
                    representative_role TEXT NOT NULL DEFAULT '',
                    legal_owner TEXT NOT NULL DEFAULT '',
                    website TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    website_source TEXT NOT NULL DEFAULT '',
                    gmap_company_name TEXT NOT NULL DEFAULT '',
                    detail_done INTEGER NOT NULL DEFAULT 0,
                    gmap_status TEXT NOT NULL DEFAULT '',
                    firecrawl_status TEXT NOT NULL DEFAULT '',
                    firecrawl_retry_at TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS detail_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gmap_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS firecrawl_queue (
                    cvr TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS final_companies (
                    cvr TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    ceo TEXT NOT NULL,
                    homepage TEXT NOT NULL DEFAULT '',
                    contact_emails TEXT NOT NULL DEFAULT '[]',
                    domain TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_search_pages_claim ON search_pages(status, page_index);
                CREATE INDEX IF NOT EXISTS idx_detail_claim ON detail_queue(status, next_run_at, updated_at, cvr);
                CREATE INDEX IF NOT EXISTS idx_gmap_claim ON gmap_queue(status, next_run_at, updated_at, cvr);
                CREATE INDEX IF NOT EXISTS idx_firecrawl_claim ON firecrawl_queue(status, next_run_at, updated_at, cvr);
                """
            )
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            for table in ("search_pages", "detail_queue", "gmap_queue", "firecrawl_queue"):
                self._conn.execute(f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'", (now,))
            self._conn.commit()

    def ensure_search_seed(self) -> None:
        with self._lock:
            if self._scalar("SELECT COUNT(*) FROM search_pages") > 0:
                return
            self._conn.execute(
                "INSERT INTO search_pages(page_index, total_hits, status, updated_at) VALUES(0, 0, 'pending', ?)",
                (_utc_now(),),
            )
            self._conn.commit()

    def expand_search_pages_from_known_total(self, *, page_size: int, max_pages: int | None) -> int:
        known_total = self._scalar("SELECT MAX(total_hits) FROM search_pages")
        if known_total <= 0:
            return 0
        total_pages = ((known_total - 1) // max(int(page_size), 1)) + 1
        if max_pages is not None:
            total_pages = min(total_pages, max(int(max_pages), 1))
        now = _utc_now()
        inserted = 0
        with self._lock:
            existing_pages = {
                int(row["page_index"])
                for row in self._conn.execute("SELECT page_index FROM search_pages").fetchall()
            }
            missing_rows = [
                (page_index, 0, "pending", now)
                for page_index in range(total_pages)
                if page_index not in existing_pages
            ]
            if not missing_rows:
                return 0
            self._conn.executemany(
                """
                INSERT INTO search_pages(page_index, total_hits, status, updated_at)
                VALUES(?, ?, ?, ?)
                """,
                missing_rows,
            )
            self._conn.commit()
            inserted = len(missing_rows)
        return inserted

    def claim_search_page(self) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT page_index FROM search_pages WHERE status = 'pending' ORDER BY page_index ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            page_index = int(row["page_index"])
            self._conn.execute(
                "UPDATE search_pages SET status = 'running', updated_at = ? WHERE page_index = ?",
                (_utc_now(), page_index),
            )
            self._conn.commit()
            return page_index

    def mark_search_page_done(self, page_index: int, *, total_hits: int, page_size: int, max_pages: int | None) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE search_pages SET status = 'done', total_hits = ?, updated_at = ? WHERE page_index = ?",
                (max(int(total_hits), 0), now, page_index),
            )
            total_pages = ((max(int(total_hits), 0) - 1) // max(int(page_size), 1) + 1) if total_hits > 0 else 0
            next_page = int(page_index) + 1
            if total_pages > 0 and next_page < total_pages and (max_pages is None or next_page < max_pages):
                self._conn.execute(
                    """
                    INSERT INTO search_pages(page_index, total_hits, status, updated_at)
                    VALUES(?, 0, 'pending', ?)
                    ON CONFLICT(page_index) DO NOTHING
                    """,
                    (next_page, now),
                )
            self._conn.commit()

    def reset_search_page(self, page_index: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE search_pages SET status = 'pending', updated_at = ? WHERE page_index = ?",
                (_utc_now(), page_index),
            )
            self._conn.commit()

    def has_search_work(self) -> bool:
        return self._scalar("SELECT COUNT(*) FROM search_pages WHERE status IN ('pending', 'running')") > 0

    def upsert_search_company(self, row: VirkSearchCompany) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(row.cvr)
            emails = _parse_json_list(current.get("emails_json", "[]") if current else "[]")
            for item in row.emails:
                value = str(item or "").strip().lower()
                if value and value not in emails:
                    emails.append(value)
            payload = (
                row.cvr,
                _merge_text(current.get("company_name", "") if current else "", row.company_name),
                _merge_text(current.get("address", "") if current else "", row.address),
                _merge_text(current.get("city", "") if current else "", row.city),
                _merge_text(current.get("postal_code", "") if current else "", row.postal_code),
                "Denmark",
                _merge_text(current.get("status", "") if current else "", row.status),
                _merge_text(current.get("company_form", "") if current else "", row.company_form),
                _merge_text(current.get("main_industry", "") if current else "", row.main_industry),
                _merge_text(current.get("start_date", "") if current else "", row.start_date),
                _merge_text(current.get("phone", "") if current else "", row.phone),
                _dump_json_list(emails),
                current.get("representative", "") if current else "",
                current.get("representative_role", "") if current else "",
                current.get("legal_owner", "") if current else "",
                current.get("website", "") if current else "",
                current.get("domain", "") if current else "",
                current.get("website_source", "") if current else "",
                current.get("gmap_company_name", "") if current else "",
                int(current.get("detail_done", 0) if current else 0),
                current.get("gmap_status", "") if current else "",
                current.get("firecrawl_status", "") if current else "",
                current.get("firecrawl_retry_at", "") if current else "",
                current.get("last_error", "") if current else "",
                now,
            )
            self._conn.execute(
                """
                INSERT INTO companies(
                    cvr, company_name, address, city, postal_code, country, status, company_form, main_industry,
                    start_date, phone, emails_json, representative, representative_role, legal_owner, website,
                    domain, website_source, gmap_company_name, detail_done, gmap_status, firecrawl_status,
                    firecrawl_retry_at, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cvr) DO UPDATE SET
                    company_name = excluded.company_name,
                    address = excluded.address,
                    city = excluded.city,
                    postal_code = excluded.postal_code,
                    country = excluded.country,
                    status = excluded.status,
                    company_form = excluded.company_form,
                    main_industry = excluded.main_industry,
                    start_date = excluded.start_date,
                    phone = excluded.phone,
                    emails_json = excluded.emails_json,
                    updated_at = excluded.updated_at
                """,
                payload,
            )
            self._conn.commit()
        if not self.is_detail_done(row.cvr):
            self.enqueue_detail_task(row.cvr)
        self.refresh_final_company(row.cvr)

    def is_detail_done(self, cvr: str) -> bool:
        return self._scalar("SELECT COUNT(*) FROM companies WHERE cvr = ? AND detail_done = 1", (cvr,)) > 0

    def enqueue_detail_task(self, cvr: str) -> None:
        self._enqueue_task("detail_queue", cvr)

    def enqueue_gmap_task(self, cvr: str) -> None:
        self._enqueue_task("gmap_queue", cvr)

    def enqueue_firecrawl_task(self, cvr: str) -> None:
        self._enqueue_task("firecrawl_queue", cvr)

    def _enqueue_task(self, table: str, cvr: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                f"""
                INSERT INTO {table}(cvr, status, retries, next_run_at, last_error, updated_at)
                VALUES(?, 'pending', 0, ?, '', ?)
                ON CONFLICT(cvr) DO UPDATE SET
                    status = CASE WHEN {table}.status = 'done' THEN {table}.status ELSE 'pending' END,
                    next_run_at = CASE WHEN {table}.status = 'done' THEN {table}.next_run_at ELSE excluded.next_run_at END,
                    updated_at = excluded.updated_at
                """,
                (cvr, now, now),
            )
            self._conn.commit()

    def _claim_task(self, table: str) -> tuple[str, int] | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT cvr, retries FROM {table}
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY updated_at ASC, cvr ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            cvr = str(row["cvr"])
            retries = int(row["retries"] or 0)
            self._conn.execute(
                f"UPDATE {table} SET status = 'running', updated_at = ? WHERE cvr = ?",
                (now, cvr),
            )
            self._conn.commit()
            return cvr, retries

    def claim_detail_task(self) -> DetailTask | None:
        task = self._claim_task("detail_queue")
        return DetailTask(*task) if task else None

    def claim_gmap_task(self) -> GMapTask | None:
        task = self._claim_task("gmap_queue")
        if task is None:
            return None
        cvr, retries = task
        company = self.get_company(cvr) or {}
        return GMapTask(
            cvr=cvr,
            company_name=str(company.get("company_name", "")).strip(),
            city=str(company.get("city", "")).strip(),
            postal_code=str(company.get("postal_code", "")).strip(),
            phone=str(company.get("phone", "")).strip(),
            retries=retries,
        )

    def claim_firecrawl_task(self) -> FirecrawlTask | None:
        task = self._claim_task("firecrawl_queue")
        if task is None:
            return None
        cvr, retries = task
        company = self.get_company(cvr) or {}
        website = str(company.get("website", "")).strip()
        return FirecrawlTask(
            cvr=cvr,
            company_name=str(company.get("company_name", "")).strip(),
            website=website,
            domain=extract_domain(website),
            retries=retries,
        )

    def get_company(self, cvr: str) -> dict[str, object] | None:
        with self._lock:
            return self._fetch_company_locked(cvr)

    def upsert_detail_company(self, record: VirkCompanyRecord) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(record.cvr) or {}
            emails = _parse_json_list(str(current.get("emails_json", "[]")))
            for item in record.emails:
                value = str(item or "").strip().lower()
                if value and value not in emails:
                    emails.append(value)
            website = _merge_text(str(current.get("website", "")), record.website)
            self._conn.execute(
                """
                UPDATE companies
                SET company_name = ?, address = ?, city = ?, postal_code = ?, status = ?, company_form = ?,
                    main_industry = ?, start_date = ?, phone = ?, emails_json = ?, representative = ?,
                    representative_role = ?, legal_owner = ?, website = ?, domain = ?, website_source = ?,
                    gmap_company_name = ?, detail_done = 1, updated_at = ?
                WHERE cvr = ?
                """,
                (
                    _merge_text(str(current.get("company_name", "")), record.company_name),
                    _merge_text(str(current.get("address", "")), record.address),
                    _merge_text(str(current.get("city", "")), record.city),
                    _merge_text(str(current.get("postal_code", "")), record.postal_code),
                    _merge_text(str(current.get("status", "")), record.status),
                    _merge_text(str(current.get("company_form", "")), record.company_form),
                    _merge_text(str(current.get("main_industry", "")), record.main_industry),
                    _merge_text(str(current.get("start_date", "")), record.start_date),
                    _merge_text(str(current.get("phone", "")), record.phone),
                    _dump_json_list(emails),
                    _merge_text(str(current.get("representative", "")), record.representative),
                    _merge_text(str(current.get("representative_role", "")), record.representative_role),
                    _merge_text(str(current.get("legal_owner", "")), record.legal_owner),
                    website,
                    extract_domain(website) or str(current.get("domain", "")),
                    _merge_text(str(current.get("website_source", "")), record.website_source),
                    _merge_text(str(current.get("gmap_company_name", "")), record.gmap_company_name),
                    now,
                    record.cvr,
                ),
            )
            self._conn.execute(
                "UPDATE detail_queue SET status = 'done', last_error = '', updated_at = ? WHERE cvr = ?",
                (now, record.cvr),
            )
            self._conn.commit()
        self._schedule_after_detail(record.cvr)
        self.refresh_final_company(record.cvr)

    def _schedule_after_detail(self, cvr: str) -> None:
        company = self.get_company(cvr) or {}
        emails = _parse_json_list(str(company.get("emails_json", "[]")))
        representative = str(company.get("representative", "")).strip()
        website = str(company.get("website", "")).strip()
        if emails or not representative:
            return
        if website:
            self.enqueue_firecrawl_task(cvr)
            return
        self.enqueue_gmap_task(cvr)

    def mark_gmap_done(self, *, cvr: str, website: str, source: str, phone: str, company_name: str = "") -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(cvr) or {}
            final_website = _merge_text(str(current.get("website", "")), website)
            self._conn.execute(
                """
                UPDATE companies
                SET website = ?, domain = ?, website_source = ?, phone = ?, gmap_company_name = ?, gmap_status = 'done', updated_at = ?
                WHERE cvr = ?
                """,
                (
                    final_website,
                    extract_domain(final_website) or str(current.get("domain", "")),
                    _merge_text(str(current.get("website_source", "")), source),
                    _merge_text(str(current.get("phone", "")), phone),
                    _merge_text(str(current.get("gmap_company_name", "")), company_name),
                    now,
                    cvr,
                ),
            )
            self._conn.execute("UPDATE gmap_queue SET status = 'done', last_error = '', updated_at = ? WHERE cvr = ?", (now, cvr))
            self._conn.commit()
        company = self.get_company(cvr) or {}
        if not _parse_json_list(str(company.get("emails_json", "[]"))) and str(company.get("representative", "")).strip() and str(company.get("website", "")).strip():
            self.enqueue_firecrawl_task(cvr)
        self.refresh_final_company(cvr)

    def mark_firecrawl_done(self, *, cvr: str, emails: list[str], retry_after_seconds: float = 0.0) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(cvr) or {}
            merged = _parse_json_list(str(current.get("emails_json", "[]")))
            for item in emails:
                value = str(item or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            has_email = bool(merged)
            self._conn.execute(
                """
                UPDATE companies
                SET emails_json = ?, firecrawl_status = ?, firecrawl_retry_at = ?, updated_at = ?
                WHERE cvr = ?
                """,
                (
                    _dump_json_list(merged),
                    "done" if has_email else "zero",
                    "" if has_email or retry_after_seconds <= 0 else _utc_after(retry_after_seconds),
                    now,
                    cvr,
                ),
            )
            self._conn.execute("UPDATE firecrawl_queue SET status = 'done', last_error = '', updated_at = ? WHERE cvr = ?", (now, cvr))
            self._conn.commit()
        self.refresh_final_company(cvr)

    def requeue_expired_firecrawl_tasks(self) -> int:
        now = _utc_now()
        with self._lock:
            rows = self._conn.execute(
                "SELECT cvr FROM companies WHERE firecrawl_retry_at != '' AND firecrawl_retry_at <= ? AND firecrawl_status IN ('done', 'zero')",
                (now,),
            ).fetchall()
            if not rows:
                return 0
            for row in rows:
                cvr = str(row["cvr"])
                self._conn.execute(
                    "UPDATE firecrawl_queue SET status = 'pending', retries = 0, next_run_at = ?, last_error = '', updated_at = ? WHERE cvr = ?",
                    (now, now, cvr),
                )
                self._conn.execute(
                    "UPDATE companies SET firecrawl_status = 'pending', firecrawl_retry_at = '', updated_at = ? WHERE cvr = ?",
                    (now, cvr),
                )
            self._conn.commit()
            return len(rows)

    def requeue_retryable_failed_tasks(self) -> dict[str, int]:
        now = _utc_now()
        summary = {"detail_queue": 0, "gmap_queue": 0, "firecrawl_queue": 0}
        conditions = " OR ".join("last_error LIKE ?" for _ in RETRYABLE_ERROR_HINTS)
        params = tuple(f"%{hint}%" for hint in RETRYABLE_ERROR_HINTS)
        with self._lock:
            for table in summary:
                rows = self._conn.execute(
                    f"SELECT cvr FROM {table} WHERE status = 'failed' AND ({conditions})",
                    params,
                ).fetchall()
                if not rows:
                    continue
                self._conn.execute(
                    f"""
                    UPDATE {table}
                    SET status = 'pending', retries = 0, next_run_at = ?, last_error = '', updated_at = ?
                    WHERE status = 'failed' AND ({conditions})
                    """,
                    (now, now, *params),
                )
                self._conn.execute(
                    "UPDATE companies SET last_error = '', updated_at = ? WHERE cvr IN ({})".format(
                        ",".join("?" for _ in rows)
                    ),
                    (now, *(str(row["cvr"]) for row in rows)),
                )
                summary[table] = len(rows)
            self._conn.commit()
        return summary

    def _defer_task(self, table: str, *, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ? WHERE cvr = ?",
                    (max(retries, 0), _utc_after(delay_seconds), str(error_text)[:500], _utc_now(), cvr),
                )
                self._conn.commit()
            run_with_sqlite_retry(self._conn, _op)

    def defer_detail_task(self, *, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("detail_queue", cvr=cvr, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_gmap_task(self, *, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("gmap_queue", cvr=cvr, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_firecrawl_task(self, *, cvr: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("firecrawl_queue", cvr=cvr, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def _mark_failed(self, table: str, *, cvr: str, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'failed', last_error = ?, updated_at = ? WHERE cvr = ?",
                (str(error_text)[:500], _utc_now(), cvr),
            )
            self._conn.execute(
                "UPDATE companies SET last_error = ?, updated_at = ? WHERE cvr = ?",
                (str(error_text)[:500], _utc_now(), cvr),
            )
            self._conn.commit()

    def mark_detail_failed(self, *, cvr: str, error_text: str) -> None:
        self._mark_failed("detail_queue", cvr=cvr, error_text=error_text)

    def mark_gmap_failed(self, *, cvr: str, error_text: str) -> None:
        self._mark_failed("gmap_queue", cvr=cvr, error_text=error_text)

    def mark_firecrawl_failed(self, *, cvr: str, error_text: str) -> None:
        self._mark_failed("firecrawl_queue", cvr=cvr, error_text=error_text)

    def refresh_final_company(self, cvr: str) -> None:
        with self._lock:
            current = self._fetch_company_locked(cvr)
            if current is None:
                return
            emails = _parse_json_list(str(current.get("emails_json", "[]")))
            company_name = str(current.get("company_name", "")).strip()
            representative = str(current.get("representative", "")).strip()
            if company_name and representative and emails:
                self._conn.execute(
                    """
                    INSERT INTO final_companies(cvr, company_name, ceo, homepage, contact_emails, domain, phone, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cvr) DO UPDATE SET
                        company_name = excluded.company_name,
                        ceo = excluded.ceo,
                        homepage = excluded.homepage,
                        contact_emails = excluded.contact_emails,
                        domain = excluded.domain,
                        phone = excluded.phone,
                        updated_at = excluded.updated_at
                    """,
                    (
                        cvr,
                        company_name,
                        representative,
                        str(current.get("website", "")).strip(),
                        _dump_json_list(emails),
                        str(current.get("domain", "")).strip(),
                        str(current.get("phone", "")).strip(),
                        _utc_now(),
                    ),
                )
            else:
                self._conn.execute("DELETE FROM final_companies WHERE cvr = ?", (cvr,))
            self._conn.commit()

    def get_stats(self) -> dict[str, int]:
        return {
            "search_pages_total": self._scalar("SELECT COUNT(*) FROM search_pages"),
            "search_pages_done": self._scalar("SELECT COUNT(*) FROM search_pages WHERE status = 'done'"),
            "companies_total": self._scalar("SELECT COUNT(*) FROM companies"),
            "companies_detail_done": self._scalar("SELECT COUNT(*) FROM companies WHERE detail_done = 1"),
            "detail_pending": self._scalar("SELECT COUNT(*) FROM detail_queue WHERE status = 'pending'"),
            "detail_running": self._scalar("SELECT COUNT(*) FROM detail_queue WHERE status = 'running'"),
            "gmap_pending": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'pending'"),
            "gmap_running": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'running'"),
            "firecrawl_pending": self._scalar("SELECT COUNT(*) FROM firecrawl_queue WHERE status = 'pending'"),
            "firecrawl_running": self._scalar("SELECT COUNT(*) FROM firecrawl_queue WHERE status = 'running'"),
            "firecrawl_done": self._scalar("SELECT COUNT(*) FROM firecrawl_queue WHERE status = 'done'"),
            "final_total": self._scalar("SELECT COUNT(*) FROM final_companies"),
        }

    def requeue_stale_running_tasks(self, *, older_than_seconds: int) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(int(older_than_seconds), 1))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        total = 0
        with self._lock:
            page_rows = self._conn.execute("SELECT page_index FROM search_pages WHERE status = 'running' AND updated_at <= ?", (cutoff,)).fetchall()
            if page_rows:
                self._conn.execute(
                    "UPDATE search_pages SET status = 'pending', updated_at = ? WHERE status = 'running' AND updated_at <= ?",
                    (now, cutoff),
                )
                total += len(page_rows)
            for table in ("detail_queue", "gmap_queue", "firecrawl_queue"):
                rows = self._conn.execute(f"SELECT cvr FROM {table} WHERE status = 'running' AND updated_at <= ?", (cutoff,)).fetchall()
                if not rows:
                    continue
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running' AND updated_at <= ?",
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
            rows = conn.execute("SELECT * FROM companies ORDER BY cvr ASC").fetchall()
            companies_rows: list[dict[str, object]] = []
            enriched_rows: list[dict[str, object]] = []
            with_emails_rows: list[dict[str, object]] = []
            for row in rows:
                record = dict(row)
                emails = _parse_json_list(str(record.get("emails_json", "[]")))
                companies_rows.append(
                    {
                        "comp_id": str(record.get("cvr", "")),
                        "company_name": str(record.get("company_name", "")).strip(),
                        "ceo": str(record.get("representative", "")).strip(),
                        "homepage": "",
                        "emails": [],
                    }
                )
                enriched_rows.append(
                    {
                        "comp_id": str(record.get("cvr", "")),
                        "company_name": str(record.get("company_name", "")).strip(),
                        "ceo": str(record.get("representative", "")).strip(),
                        "homepage": str(record.get("website", "")).strip(),
                        "emails": [],
                    }
                )
                if not (str(record.get("company_name", "")).strip() and str(record.get("representative", "")).strip() and emails):
                    continue
                with_emails_rows.append(
                    {
                        "comp_id": str(record.get("cvr", "")),
                        "cvr": str(record.get("cvr", "")),
                        "company_name": str(record.get("company_name", "")).strip(),
                        "ceo": str(record.get("representative", "")).strip(),
                        "homepage": str(record.get("website", "")).strip(),
                        "domain": str(record.get("domain", "")).strip(),
                        "phone": str(record.get("phone", "")).strip(),
                        "emails": emails,
                        "legal_owner": str(record.get("legal_owner", "")).strip(),
                    }
                )
            _write_jsonl_atomic(output_dir / "companies.jsonl", companies_rows)
            _write_jsonl_atomic(output_dir / "companies_enriched.jsonl", enriched_rows)
            _write_jsonl_atomic(output_dir / "companies_with_emails.jsonl", with_emails_rows)
            _write_jsonl_atomic(output_dir / "final_companies.jsonl", with_emails_rows)
        finally:
            conn.close()

    def _fetch_company_locked(self, cvr: str) -> dict[str, object] | None:
        row = self._conn.execute("SELECT * FROM companies WHERE cvr = ?", (cvr,)).fetchone()
        return dict(row) if row is not None else None

    def _scalar(self, sql: str, params: tuple[object, ...] = ()) -> int:
        row = self._conn.execute(sql, params).fetchone()
        return int(row[0]) if row is not None else 0
