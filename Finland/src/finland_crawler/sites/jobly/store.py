"""Jobly SQLite 断点存储。

结构与 Duunitori store 相同，表名/前缀改为 jobly。
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_phone(value: str) -> str:
    return re.sub(r"[^\d+]+", "", str(value or "").strip())


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
    return [str(x or "").strip().lower() for x in payload if str(x or "").strip()]


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


def _tmp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl_atomic(path: Path, rows: list[dict[str, object]]) -> None:
    tmp = _tmp_path(path)
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    tmp.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp, path)


@dataclass(slots=True)
class JoblyDetailTask:
    job_id: str
    url: str
    retries: int = 0

@dataclass(slots=True)
class JoblyGMapTask:
    job_id: str
    company_name: str
    city: str
    retries: int = 0

@dataclass(slots=True)
class JoblyFirecrawlTask:
    job_id: str
    company_name: str
    website: str
    domain: str
    retries: int = 0

@dataclass(slots=True)
class JoblyProgress:
    search_done_pages: int
    search_total: int
    detail_total: int
    detail_done: int
    detail_pending: int
    detail_running: int
    gmap_pending: int
    gmap_running: int
    firecrawl_pending: int
    firecrawl_running: int
    jobs_total: int
    final_total: int


class JoblyStore:
    """Jobly 断点存储。"""

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
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS search_progress (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    total_count INTEGER NOT NULL DEFAULT 0,
                    pages_done INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    gmap_phone TEXT NOT NULL DEFAULT '',
                    gmap_company_name TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source_page INTEGER NOT NULL DEFAULT 0,
                    detail_status TEXT NOT NULL DEFAULT 'pending',
                    gmap_status TEXT NOT NULL DEFAULT '',
                    firecrawl_status TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS detail_queue (
                    job_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gmap_queue (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS firecrawl_queue (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS final_companies (
                    job_id TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    representative TEXT NOT NULL,
                    email TEXT NOT NULL,
                    phone TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, email)
                );
            """)
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            for table in ("detail_queue", "gmap_queue", "firecrawl_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (now,),
                )
            self._conn.commit()

    def get_search_progress(self) -> tuple[int, int]:
        with self._lock:
            row = self._conn.execute("SELECT pages_done, total_count FROM search_progress WHERE id = 1").fetchone()
            if not row:
                return 0, 0
            return row["pages_done"], row["total_count"]

    def update_search_progress(self, total_count: int, pages_done: int) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO search_progress (id, total_count, pages_done, updated_at)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    total_count = MAX(excluded.total_count, search_progress.total_count),
                    pages_done = MAX(excluded.pages_done, search_progress.pages_done),
                    updated_at = excluded.updated_at""",
                (total_count, pages_done, now),
            )
            self._conn.commit()

    def upsert_job(self, job: dict[str, str], page: int = 0) -> None:
        now = _utc_now()
        job_id = job.get("job_id", "")
        if not job_id:
            return
        url = job.get("url", "")
        with self._lock:
            self._conn.execute(
                """INSERT INTO jobs (job_id, url, title, company_name, city, source_page, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    title = COALESCE(NULLIF(excluded.title, ''), jobs.title),
                    company_name = COALESCE(NULLIF(excluded.company_name, ''), jobs.company_name),
                    updated_at = excluded.updated_at""",
                (job_id, url, job.get("title", ""), job.get("company_name", ""), job.get("city", ""), page, now),
            )
            self._conn.execute(
                """INSERT OR IGNORE INTO detail_queue (job_id, url, status, retries, next_run_at, last_error, updated_at)
                VALUES (?, ?, 'pending', 0, ?, '', ?)""",
                (job_id, url, now, now),
            )
            self._conn.commit()

    def upsert_job_detail(self, job_id: str, detail: dict[str, str]) -> None:
        now = _utc_now()
        email = str(detail.get("email", "")).strip().lower()
        representative = str(detail.get("representative", "")).strip()
        phone = _normalize_phone(detail.get("phone", ""))
        with self._lock:
            row = self._conn.execute("SELECT company_name FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            company_name = row["company_name"] if row else ""
            deliverable = bool(email and representative and company_name)
            self._conn.execute(
                """UPDATE jobs SET email = COALESCE(NULLIF(?, ''), email),
                    phone = COALESCE(NULLIF(?, ''), phone),
                    representative = COALESCE(NULLIF(?, ''), representative),
                    description = ?, detail_status = 'done', updated_at = ?
                WHERE job_id = ?""",
                (email, phone, representative, detail.get("description", "")[:2000], now, job_id),
            )
            if deliverable:
                self._insert_final(job_id, company_name, representative, email, phone, "", "", "jobly-direct")
            else:
                self._conn.execute(
                    """INSERT OR IGNORE INTO gmap_queue (job_id, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (job_id, now, now),
                )
                self._conn.execute(
                    "UPDATE jobs SET gmap_status = 'pending', updated_at = ? WHERE job_id = ?", (now, job_id),
                )
            self._conn.commit()

    def _insert_final(self, job_id: str, name: str, rep: str, email: str,
                      phone: str, homepage: str, evidence: str, source: str) -> None:
        now = _utc_now()
        self._conn.execute(
            """INSERT OR IGNORE INTO final_companies
            (job_id, company_name, representative, email, phone, homepage, evidence_url, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, name, rep, email, phone, homepage, evidence, source, now),
        )

    def claim_detail_task(self) -> JoblyDetailTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT job_id, url, retries FROM detail_queue
                WHERE status = 'pending' AND next_run_at <= ? ORDER BY updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE job_id = ?",
                (now, row["job_id"]),
            )
            self._conn.commit()
            return JoblyDetailTask(job_id=row["job_id"], url=row["url"], retries=row["retries"])

    def mark_detail_done(self, job_id: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute("UPDATE detail_queue SET status = 'done', updated_at = ? WHERE job_id = ?", (now, job_id))
            self._conn.commit()

    def mark_detail_failed(self, job_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE job_id = ?",
                (error_text[:500], now, job_id),
            )
            self._conn.commit()

    def defer_detail_task(self, job_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE detail_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ? WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    def claim_gmap_task(self) -> JoblyGMapTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT g.job_id, g.retries, j.company_name, j.city
                FROM gmap_queue g JOIN jobs j ON g.job_id = j.job_id
                WHERE g.status = 'pending' AND g.next_run_at <= ?
                ORDER BY g.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'running', updated_at = ? WHERE job_id = ?",
                (now, row["job_id"]),
            )
            self._conn.commit()
            return JoblyGMapTask(
                job_id=row["job_id"], company_name=row["company_name"],
                city=row["city"], retries=row["retries"],
            )

    def mark_gmap_done(self, *, job_id: str, website: str, phone: str, company_name: str) -> None:
        now = _utc_now()
        domain = _extract_domain(website)
        with self._lock:
            self._conn.execute("UPDATE gmap_queue SET status = 'done', updated_at = ? WHERE job_id = ?", (now, job_id))
            self._conn.execute(
                """UPDATE jobs SET homepage = COALESCE(NULLIF(?, ''), homepage),
                    domain = COALESCE(NULLIF(?, ''), domain),
                    gmap_phone = ?, gmap_company_name = ?, gmap_status = 'done', updated_at = ?
                WHERE job_id = ?""",
                (website, domain, _normalize_phone(phone), company_name, now, job_id),
            )
            if domain:
                self._conn.execute(
                    """INSERT OR IGNORE INTO firecrawl_queue (job_id, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (job_id, now, now),
                )
                self._conn.execute(
                    "UPDATE jobs SET firecrawl_status = 'pending', updated_at = ? WHERE job_id = ?", (now, job_id),
                )
            self._conn.commit()

    def mark_gmap_failed(self, *, job_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE job_id = ?",
                (error_text[:500], now, job_id),
            )
            self._conn.execute("UPDATE jobs SET gmap_status = 'failed', updated_at = ? WHERE job_id = ?", (now, job_id))
            self._conn.commit()

    def defer_gmap_task(self, *, job_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE gmap_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ? WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    def claim_firecrawl_task(self) -> JoblyFirecrawlTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT f.job_id, f.retries, j.company_name, j.homepage, j.domain
                FROM firecrawl_queue f JOIN jobs j ON f.job_id = j.job_id
                WHERE f.status = 'pending' AND f.next_run_at <= ?
                ORDER BY f.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'running', updated_at = ? WHERE job_id = ?",
                (now, row["job_id"]),
            )
            self._conn.commit()
            return JoblyFirecrawlTask(
                job_id=row["job_id"], company_name=row["company_name"],
                website=row["homepage"], domain=row["domain"], retries=row["retries"],
            )

    def mark_firecrawl_done(self, *, job_id: str, emails: list[str],
                            representative: str = "", company_name: str = "", evidence_url: str = "") -> None:
        now = _utc_now()
        with self._lock:
            current = self._conn.execute(
                "SELECT company_name, representative, phone, homepage, gmap_company_name, emails_json FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if not current:
                self._conn.execute("UPDATE firecrawl_queue SET status = 'done', updated_at = ? WHERE job_id = ?", (now, job_id))
                self._conn.commit()
                return
            merged = _parse_json_list(str(current["emails_json"] or "[]"))
            for email in emails:
                value = str(email or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            has_email = bool(merged)
            next_name = str(current["company_name"] or "").strip()
            if str(company_name or "").strip():
                next_name = str(company_name).strip()
            elif str(current["gmap_company_name"] or "").strip():
                next_name = str(current["gmap_company_name"]).strip()
            next_rep = str(current["representative"] or "").strip()
            if str(representative or "").strip():
                next_rep = str(representative).strip()
            self._conn.execute(
                """UPDATE jobs SET company_name = ?, representative = ?,
                    emails_json = ?, evidence_url = ?,
                    firecrawl_status = ?, last_error = '', updated_at = ?
                WHERE job_id = ?""",
                (next_name, next_rep, _dump_json_list(merged),
                 str(evidence_url or "").strip(), "done" if has_email else "zero", now, job_id),
            )
            self._conn.execute("UPDATE firecrawl_queue SET status = 'done', updated_at = ? WHERE job_id = ?", (now, job_id))
            if has_email:
                for em in merged:
                    self._insert_final(job_id, next_name, next_rep, em,
                                       str(current["phone"] or ""), str(current["homepage"] or ""),
                                       str(evidence_url or ""), "jobly-firecrawl")
            self._conn.commit()

    def mark_firecrawl_failed(self, *, job_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE job_id = ?",
                (error_text[:500], now, job_id),
            )
            self._conn.execute("UPDATE jobs SET firecrawl_status = 'failed', updated_at = ? WHERE job_id = ?", (now, job_id))
            self._conn.commit()

    def defer_firecrawl_task(self, *, job_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE firecrawl_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ? WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    def get_progress(self) -> JoblyProgress:
        with self._lock:
            def _count(table, status):
                r = self._conn.execute(f"SELECT COUNT(*) as c FROM {table} WHERE status = ?", (status,)).fetchone()
                return r["c"] if r else 0
            sp = self.get_search_progress()
            dt = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue").fetchone()
            dd = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue WHERE status='done'").fetchone()
            jt = self._conn.execute("SELECT COUNT(*) as c FROM jobs").fetchone()
            ft = self._conn.execute("SELECT COUNT(*) as c FROM final_companies").fetchone()
            return JoblyProgress(
                search_done_pages=sp[0], search_total=sp[1],
                detail_total=dt["c"] if dt else 0, detail_done=dd["c"] if dd else 0,
                detail_pending=_count("detail_queue", "pending"), detail_running=_count("detail_queue", "running"),
                gmap_pending=_count("gmap_queue", "pending"), gmap_running=_count("gmap_queue", "running"),
                firecrawl_pending=_count("firecrawl_queue", "pending"), firecrawl_running=_count("firecrawl_queue", "running"),
                jobs_total=jt["c"] if jt else 0, final_total=ft["c"] if ft else 0,
            )

    def requeue_stale_running_tasks(self, *, older_than_seconds: float = 300.0) -> dict[str, int]:
        cutoff = _utc_after(-older_than_seconds)
        now = _utc_now()
        recovered = {}
        with self._lock:
            for table in ("detail_queue", "gmap_queue", "firecrawl_queue"):
                cur = self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running' AND updated_at < ?",
                    (now, cutoff),
                )
                if cur.rowcount > 0:
                    recovered[table] = cur.rowcount
            self._conn.commit()
        return recovered

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY job_id").fetchall()
            _write_jsonl_atomic(output_dir / "jobly_jobs.jsonl", [dict(r) for r in rows])
            rows = self._conn.execute("SELECT * FROM final_companies ORDER BY job_id").fetchall()
            _write_jsonl_atomic(output_dir / "jobly_final.jsonl", [dict(r) for r in rows])
