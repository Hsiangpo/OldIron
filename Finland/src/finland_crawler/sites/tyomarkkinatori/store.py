"""Työmarkkinatori SQLite 断点存储。"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
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
    text = re.sub(r"[^\d+]+", "", str(value or "").strip())
    if text.startswith("+358"):
        text = text[4:]
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


# ---- 任务数据类 ----

@dataclass(slots=True)
class TmtDetailTask:
    """详情拉取任务。"""
    job_id: str
    retries: int = 0


@dataclass(slots=True)
class TmtGMapTask:
    """GMap 补全任务。"""
    job_id: str
    company_name: str
    address: str
    city: str
    retries: int = 0


@dataclass(slots=True)
class TmtFirecrawlTask:
    """Protocol+LLM 邮箱补全任务。"""
    job_id: str
    company_name: str
    website: str
    domain: str
    retries: int = 0


@dataclass(slots=True)
class TmtProgress:
    """运行进度。"""
    search_done_pages: int
    search_total_jobs: int
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


class TmtStore:
    """Työmarkkinatori 断点与快照存储。"""

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
                    title TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    business_id TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    postcode TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    work_time TEXT NOT NULL DEFAULT '',
                    employment_type TEXT NOT NULL DEFAULT '',
                    duration TEXT NOT NULL DEFAULT '',
                    salary_info TEXT NOT NULL DEFAULT '',
                    industry_code TEXT NOT NULL DEFAULT '',
                    publish_date TEXT NOT NULL DEFAULT '',
                    end_date TEXT NOT NULL DEFAULT '',
                    application_url TEXT NOT NULL DEFAULT '',
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
                    business_id TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, email)
                );

                CREATE INDEX IF NOT EXISTS idx_tmt_detail_claim
                ON detail_queue(status, next_run_at, updated_at, job_id);
                CREATE INDEX IF NOT EXISTS idx_tmt_gmap_claim
                ON gmap_queue(status, next_run_at, updated_at, job_id);
                CREATE INDEX IF NOT EXISTS idx_tmt_firecrawl_claim
                ON firecrawl_queue(status, next_run_at, updated_at, job_id);
            """)
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        """启动时修复运行状态。"""
        now = _utc_now()
        with self._lock:
            for table in ("detail_queue", "gmap_queue", "firecrawl_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (now,),
                )
                self._conn.execute(
                    f"UPDATE {table} SET next_run_at = ?, updated_at = ? WHERE status = 'pending' AND next_run_at > ?",
                    (now, now, now),
                )
            self._conn.commit()

    # ---- 搜索进度 ----

    def get_search_progress(self) -> tuple[int, int]:
        """返回 (已完成页数, 总职位数)。"""
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

    # ---- 职位 upsert ----

    def upsert_job(self, posting) -> None:
        """插入或更新职位记录（搜索阶段）。"""
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO jobs
                (job_id, title, company_name, business_id, city, source_page, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    title = COALESCE(NULLIF(excluded.title, ''), jobs.title),
                    company_name = COALESCE(NULLIF(excluded.company_name, ''), jobs.company_name),
                    business_id = COALESCE(NULLIF(excluded.business_id, ''), jobs.business_id),
                    updated_at = excluded.updated_at
                """,
                (posting.job_id, posting.title, posting.company_name,
                 posting.business_id, posting.city, posting.source_page, now),
            )
            # 创建详情任务
            self._conn.execute(
                """INSERT OR IGNORE INTO detail_queue
                (job_id, status, retries, next_run_at, last_error, updated_at)
                VALUES (?, 'pending', 0, ?, '', ?)""",
                (posting.job_id, now, now),
            )
            self._conn.commit()

    def upsert_job_detail(self, posting) -> None:
        """更新职位详情数据（详情阶段）。"""
        now = _utc_now()
        phone = _normalize_phone(posting.phone)
        email = str(posting.email or "").strip().lower()
        domain = _extract_domain(posting.homepage)
        # pipeline 1: 邮箱+代表人+公司名齐全且代表人不含公司后缀才直接落盘
        _corp_re = re.compile(
            r"\b(ApS|A/S|I/S|K/S|P/S|IVS|GmbH|AG|Ltd\.?|LLC|Inc\.?|PLC|LP|LLP|AB|SA|BV|NV|Oy|AS)\b",
            re.IGNORECASE,
        )
        rep_str = str(posting.representative or "").strip()
        deliverable = bool(
            email and rep_str and posting.company_name
            and not _corp_re.search(rep_str)
        )
        with self._lock:
            self._conn.execute(
                """UPDATE jobs SET
                    company_name = COALESCE(NULLIF(?, ''), company_name),
                    business_id = COALESCE(NULLIF(?, ''), business_id),
                    address = ?, postcode = ?, city = COALESCE(NULLIF(?, ''), city),
                    region = ?, email = COALESCE(NULLIF(?, ''), email),
                    phone = COALESCE(NULLIF(?, ''), phone),
                    representative = ?, homepage = COALESCE(NULLIF(?, ''), homepage),
                    domain = COALESCE(NULLIF(?, ''), domain),
                    work_time = ?, employment_type = ?, duration = ?,
                    salary_info = ?, industry_code = ?,
                    publish_date = ?, end_date = ?,
                    application_url = ?, description = ?,
                    detail_status = 'done', updated_at = ?
                WHERE job_id = ?""",
                (
                    posting.company_name, posting.business_id,
                    posting.address, posting.postcode, posting.city,
                    posting.region, email, phone,
                    posting.representative, posting.homepage, domain,
                    posting.work_time, posting.employment_type, posting.duration,
                    posting.salary_info, posting.industry_code,
                    posting.publish_date, posting.end_date,
                    posting.application_url, posting.description,
                    now, posting.job_id,
                ),
            )
            if deliverable:
                self._insert_final(
                    posting.job_id, posting.company_name,
                    posting.representative, email, phone,
                    posting.homepage, posting.business_id, "", "tmt-direct",
                )
            else:
                # 缺数据 → 派生 GMap 任务
                self._conn.execute(
                    """INSERT OR IGNORE INTO gmap_queue
                    (job_id, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (posting.job_id, now, now),
                )
                self._conn.execute(
                    "UPDATE jobs SET gmap_status = 'pending', updated_at = ? WHERE job_id = ?",
                    (now, posting.job_id),
                )
            self._conn.commit()

    def _insert_final(
        self, job_id: str, name: str, rep: str, email: str,
        phone: str, homepage: str, biz_id: str, evidence: str, source: str,
    ) -> None:
        now = _utc_now()
        self._conn.execute(
            """INSERT OR IGNORE INTO final_companies
            (job_id, company_name, representative, email, phone, homepage,
             business_id, evidence_url, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, name, rep, email, phone, homepage, biz_id, evidence, source, now),
        )

    # ---- 详情任务 ----

    def claim_detail_task(self) -> TmtDetailTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT job_id, retries FROM detail_queue
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE detail_queue SET status = 'running', updated_at = ? WHERE job_id = ?",
                (now, row["job_id"]),
            )
            self._conn.commit()
            return TmtDetailTask(job_id=row["job_id"], retries=row["retries"])

    def mark_detail_done(self, job_id: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE detail_queue SET status = 'done', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
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
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    # ---- GMap 任务 ----

    def claim_gmap_task(self) -> TmtGMapTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT g.job_id, g.retries, j.company_name, j.address, j.city
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
            return TmtGMapTask(
                job_id=row["job_id"], company_name=row["company_name"],
                address=row["address"], city=row["city"],
                retries=row["retries"],
            )

    def mark_gmap_done(self, *, job_id: str, website: str, source: str,
                       phone: str, company_name: str) -> None:
        now = _utc_now()
        domain = _extract_domain(website)
        with self._lock:
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'done', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            self._conn.execute(
                """UPDATE jobs SET
                    homepage = COALESCE(NULLIF(?, ''), homepage),
                    domain = COALESCE(NULLIF(?, ''), domain),
                    gmap_phone = ?, gmap_company_name = ?,
                    gmap_status = 'done', updated_at = ?
                WHERE job_id = ?""",
                (website, domain, _normalize_phone(phone), company_name, now, job_id),
            )
            if domain:
                self._conn.execute(
                    """INSERT OR IGNORE INTO firecrawl_queue
                    (job_id, status, retries, next_run_at, last_error, updated_at)
                    VALUES (?, 'pending', 0, ?, '', ?)""",
                    (job_id, now, now),
                )
                self._conn.execute(
                    "UPDATE jobs SET firecrawl_status = 'pending', updated_at = ? WHERE job_id = ?",
                    (now, job_id),
                )
            self._conn.commit()

    def mark_gmap_failed(self, *, job_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE job_id = ?",
                (error_text[:500], now, job_id),
            )
            self._conn.execute(
                "UPDATE jobs SET gmap_status = 'failed', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            self._conn.commit()

    def defer_gmap_task(self, *, job_id: str, retries: int, delay_seconds: float, error_text: str) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE gmap_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    # ---- Firecrawl 任务 ----

    def claim_firecrawl_task(self) -> TmtFirecrawlTask | None:
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
            return TmtFirecrawlTask(
                job_id=row["job_id"], company_name=row["company_name"],
                website=row["homepage"], domain=row["domain"],
                retries=row["retries"],
            )

    def mark_firecrawl_done(
        self, *, job_id: str, emails: list[str],
        representative: str = "", company_name: str = "",
        evidence_url: str = "", retry_after_seconds: float = 0.0,
    ) -> None:
        """标记 Firecrawl 完成，LLM 覆盖逻辑同丹麦。"""
        now = _utc_now()
        with self._lock:
            current = self._conn.execute(
                """SELECT company_name, representative, business_id, phone, homepage,
                   gmap_company_name, emails_json FROM jobs WHERE job_id = ?""",
                (job_id,),
            ).fetchone()
            if not current:
                self._conn.execute(
                    "UPDATE firecrawl_queue SET status = 'done', updated_at = ? WHERE job_id = ?",
                    (now, job_id),
                )
                self._conn.commit()
                return
            # 合并邮箱
            merged = _parse_json_list(str(current["emails_json"] or "[]"))
            for email in emails:
                value = str(email or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            has_email = bool(merged)
            # 官网信息覆盖
            next_name = str(current["company_name"] or "").strip()
            website_name = str(company_name or "").strip()
            if website_name:
                next_name = website_name
            elif str(current["gmap_company_name"] or "").strip():
                next_name = str(current["gmap_company_name"]).strip()
            next_rep = str(current["representative"] or "").strip()
            website_rep = str(representative or "").strip()
            if website_rep:
                next_rep = website_rep
            self._conn.execute(
                """UPDATE jobs SET
                    company_name = ?, representative = ?,
                    emails_json = ?, evidence_url = ?,
                    firecrawl_status = ?, last_error = '', updated_at = ?
                WHERE job_id = ?""",
                (next_name, next_rep, _dump_json_list(merged),
                 str(evidence_url or "").strip(),
                 "done" if has_email else "zero", now, job_id),
            )
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'done', last_error = '', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            # 有邮箱 + 代表人齐全才落盘 final（三项齐全门禁）
            if has_email and next_rep:
                for em in merged:
                    self._insert_final(
                        job_id, next_name, next_rep, em,
                        str(current["phone"] or ""), str(current["homepage"] or ""),
                        str(current["business_id"] or ""), str(evidence_url or ""),
                        "tmt-firecrawl",
                    )
            self._conn.commit()

    def mark_firecrawl_failed(self, *, job_id: str, error_text: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE firecrawl_queue SET status = 'failed', last_error = ?, updated_at = ? WHERE job_id = ?",
                (error_text[:500], now, job_id),
            )
            self._conn.execute(
                "UPDATE jobs SET firecrawl_status = 'failed', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            self._conn.commit()

    def defer_firecrawl_task(
        self, *, job_id: str, retries: int, delay_seconds: float, error_text: str,
    ) -> None:
        now = _utc_now()
        run_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE firecrawl_queue SET status = 'pending', retries = ?,
                next_run_at = ?, last_error = ?, updated_at = ?
                WHERE job_id = ?""",
                (retries, run_at, error_text[:500], now, job_id),
            )
            self._conn.commit()

    # ---- 进度 ----

    def get_progress(self) -> TmtProgress:
        with self._lock:
            def _count(table, status):
                r = self._conn.execute(
                    f"SELECT COUNT(*) as c FROM {table} WHERE status = ?", (status,)
                ).fetchone()
                return r["c"] if r else 0

            sp = self.get_search_progress()
            dt = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue").fetchone()
            dd = self._conn.execute("SELECT COUNT(*) as c FROM detail_queue WHERE status='done'").fetchone()
            jt = self._conn.execute("SELECT COUNT(*) as c FROM jobs").fetchone()
            ft = self._conn.execute("SELECT COUNT(*) as c FROM final_companies").fetchone()

            return TmtProgress(
                search_done_pages=sp[0],
                search_total_jobs=sp[1],
                detail_total=dt["c"] if dt else 0,
                detail_done=dd["c"] if dd else 0,
                detail_pending=_count("detail_queue", "pending"),
                detail_running=_count("detail_queue", "running"),
                gmap_pending=_count("gmap_queue", "pending"),
                gmap_running=_count("gmap_queue", "running"),
                firecrawl_pending=_count("firecrawl_queue", "pending"),
                firecrawl_running=_count("firecrawl_queue", "running"),
                jobs_total=jt["c"] if jt else 0,
                final_total=ft["c"] if ft else 0,
            )

    # ---- 陈旧任务回收 ----

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

    # ---- JSONL 快照导出 ----

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY job_id").fetchall()
            jobs = [dict(r) for r in rows]
            _write_jsonl_atomic(output_dir / "tmt_jobs.jsonl", jobs)

            rows = self._conn.execute("SELECT * FROM final_companies ORDER BY job_id").fetchall()
            finals = [dict(r) for r in rows]
            _write_jsonl_atomic(output_dir / "tmt_final.jsonl", finals)
