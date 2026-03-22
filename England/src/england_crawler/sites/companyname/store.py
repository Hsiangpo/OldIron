"""CompanyName SQLite 断点存储（无搜索阶段，直灌公司名）。"""

from __future__ import annotations

import hashlib
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


def run_with_sqlite_retry(conn, operation, *, attempts=6, base_delay=0.05, cap_delay=0.5):
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
    # 域名合法性：必须含 "."、仅 ASCII、不含空格
    if "." not in value or not value.isascii() or " " in value:
        return ""
    return value


def _normalize_phone(value: str) -> str:
    text = re.sub(r"[^\d+]+", "", str(value or "").strip())
    if text.startswith("+44"):
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
    tmp = _tmp_path(path)
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    tmp.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp, path)


# ── 数据模型 ──

@dataclass(slots=True)
class GMapTask:
    """GMap 查询任务。"""
    orgnr: str
    company_name: str
    address: str
    proff_phone: str
    retries: int


@dataclass(slots=True)
class FirecrawlTask:
    """官网爬虫/邮箱补充任务。"""
    orgnr: str
    company_name: str
    website: str
    domain: str
    override_mode: str
    retries: int


@dataclass(slots=True)
class Progress:
    """运行进度。"""
    gmap_pending: int
    gmap_running: int
    firecrawl_pending: int
    firecrawl_running: int
    companies_total: int
    final_total: int


# ── 主存储 ──

class CompanyNameStore:
    """CompanyName 断点与快照存储（无搜索表）。"""

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
                CREATE TABLE IF NOT EXISTS companies (
                    orgnr TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    homepage TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    gmap_phone TEXT NOT NULL DEFAULT '',
                    gmap_company_name TEXT NOT NULL DEFAULT '',
                    override_mode TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    evidence_url TEXT NOT NULL DEFAULT '',
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
                    evidence_url TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(orgnr, email)
                );
                CREATE INDEX IF NOT EXISTS idx_gmap_claim
                ON gmap_queue(status, next_run_at, updated_at, orgnr);
                CREATE INDEX IF NOT EXISTS idx_firecrawl_claim
                ON firecrawl_queue(status, next_run_at, updated_at, orgnr);
            """)
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            for table in ("gmap_queue", "firecrawl_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', updated_at = ? WHERE status = 'running'",
                    (now,),
                )
            self._conn.commit()

    # ── 公司灌入 ──

    def seed_companies(self, names: list[str]) -> int:
        """从公司名列表批量灌入，跳过已有的。返回新增数量。"""
        now = _utc_now()
        added = 0
        with self._lock:
            for name in names:
                name = str(name or "").strip()
                if not name:
                    continue
                # 用确定性 hash 做 orgnr（hashlib 不受 PYTHONHASHSEED 影响）
                orgnr = hashlib.md5(name.lower().encode("utf-8")).hexdigest()[:16]
                try:
                    self._conn.execute(
                        """INSERT INTO companies(orgnr, company_name, gmap_status, firecrawl_status, updated_at)
                           VALUES(?, ?, 'pending', '', ?)""",
                        (orgnr, name, now),
                    )
                    self._conn.execute(
                        """INSERT INTO gmap_queue(orgnr, status, retries, next_run_at, last_error, updated_at)
                           VALUES(?, 'pending', 0, ?, '', ?)""",
                        (orgnr, now, now),
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    pass  # 已存在
                if added % 5000 == 0 and added > 0:
                    self._conn.commit()
            self._conn.commit()
        return added

    # ── GMap 队列 ──

    def claim_gmap_task(self) -> GMapTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT q.orgnr, c.company_name, c.address, c.phone, q.retries
                   FROM gmap_queue q JOIN companies c ON q.orgnr = c.orgnr
                   WHERE q.status = 'pending' AND q.next_run_at <= ?
                   ORDER BY q.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'running', updated_at = ? WHERE orgnr = ?",
                (now, row["orgnr"]),
            )
            self._conn.commit()
            return GMapTask(
                orgnr=row["orgnr"],
                company_name=row["company_name"],
                address=row["address"] or "",
                proff_phone=row["phone"] or "",
                retries=row["retries"],
            )

    def complete_gmap_task(self, orgnr: str, homepage: str, phone: str,
                           gmap_name: str, evidence_url: str = "") -> None:
        now = _utc_now()
        domain = _extract_domain(homepage)
        with self._lock:
            self._conn.execute(
                """UPDATE companies SET homepage=?, domain=?, gmap_phone=?,
                   gmap_company_name=?, gmap_status='done', evidence_url=?, updated_at=?
                   WHERE orgnr=?""",
                (homepage, domain, phone, gmap_name, evidence_url, now, orgnr),
            )
            self._conn.execute(
                "UPDATE gmap_queue SET status='done', updated_at=? WHERE orgnr=?",
                (now, orgnr),
            )
            # 有官网就入 firecrawl 队列
            if domain:
                try:
                    self._conn.execute(
                        """INSERT INTO firecrawl_queue(orgnr, status, retries, next_run_at, last_error, updated_at)
                           VALUES(?, 'pending', 0, ?, '', ?)""",
                        (orgnr, now, now),
                    )
                except sqlite3.IntegrityError:
                    pass
                self._conn.execute(
                    "UPDATE companies SET firecrawl_status='pending' WHERE orgnr=?",
                    (orgnr,),
                )
            else:
                self._conn.execute(
                    "UPDATE companies SET firecrawl_status='skip' WHERE orgnr=?",
                    (orgnr,),
                )
            self._conn.commit()

    def defer_gmap_task(self, orgnr: str, delay_seconds: float, error: str = "") -> None:
        now = _utc_now()
        next_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE gmap_queue SET status='pending', retries=retries+1,
                   next_run_at=?, last_error=?, updated_at=? WHERE orgnr=?""",
                (next_at, error[:500], now, orgnr),
            )
            self._conn.commit()

    # ── Firecrawl 队列 ──

    def claim_firecrawl_task(self) -> FirecrawlTask | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                """SELECT q.orgnr, c.company_name, c.homepage, c.domain, c.override_mode, q.retries
                   FROM firecrawl_queue q JOIN companies c ON q.orgnr = c.orgnr
                   WHERE q.status = 'pending' AND q.next_run_at <= ?
                   ORDER BY q.updated_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                "UPDATE firecrawl_queue SET status='running', updated_at=? WHERE orgnr=?",
                (now, row["orgnr"]),
            )
            self._conn.commit()
            return FirecrawlTask(
                orgnr=row["orgnr"],
                company_name=row["company_name"],
                website=row["homepage"] or "",
                domain=row["domain"] or "",
                override_mode=row["override_mode"] or "",
                retries=row["retries"],
            )

    def complete_firecrawl_task(self, orgnr: str, emails: list[str],
                                 evidence_url: str = "", representative: str = "",
                                 website_company_name: str = "") -> None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT company_name, representative, homepage, phone, emails_json, evidence_url FROM companies WHERE orgnr=?",
                (orgnr,),
            ).fetchone()
            if not row:
                return
            old_emails = _parse_json_list(row["emails_json"])
            merged = list(old_emails)
            for em in emails:
                em_lower = em.strip().lower()
                if em_lower and em_lower not in merged:
                    merged.append(em_lower)
            ev = evidence_url or row["evidence_url"] or ""
            # 代表人：优先用 LLM 提取到的，其次用数据库里已有的
            rep = representative.strip() if representative else ""
            if not rep:
                rep = row["representative"] or ""
            # 公司名：优先用官网提取到的真实公司名，其次用原始搜索名
            final_name = website_company_name.strip() if website_company_name else ""
            if not final_name:
                final_name = row["company_name"]
            self._conn.execute(
                """UPDATE companies SET company_name=?, emails_json=?, firecrawl_status='done',
                   representative=?, evidence_url=?, updated_at=? WHERE orgnr=?""",
                (final_name, _dump_json_list(merged), rep, ev, now, orgnr),
            )
            self._conn.execute(
                "UPDATE firecrawl_queue SET status='done', updated_at=? WHERE orgnr=?",
                (now, orgnr),
            )
            # 写入 final_companies
            for em in merged:
                try:
                    self._conn.execute(
                        """INSERT OR REPLACE INTO final_companies(orgnr, company_name, representative, email, evidence_url, source, updated_at)
                           VALUES(?, ?, ?, ?, ?, 'companyname', ?)""",
                        (orgnr, final_name, rep, em, ev, now),
                    )
                except sqlite3.IntegrityError:
                    pass
            self._conn.commit()

    def defer_firecrawl_task(self, orgnr: str, delay_seconds: float, error: str = "") -> None:
        now = _utc_now()
        next_at = _utc_after(delay_seconds)
        with self._lock:
            self._conn.execute(
                """UPDATE firecrawl_queue SET status='pending', retries=retries+1,
                   next_run_at=?, last_error=?, updated_at=? WHERE orgnr=?""",
                (next_at, error[:500], now, orgnr),
            )
            self._conn.commit()

    def skip_firecrawl_task(self, orgnr: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "UPDATE firecrawl_queue SET status='done', updated_at=? WHERE orgnr=?",
                (now, orgnr),
            )
            self._conn.execute(
                "UPDATE companies SET firecrawl_status='skip', updated_at=? WHERE orgnr=?",
                (now, orgnr),
            )
            self._conn.commit()

    # ── 陈旧任务回收 ──

    def requeue_stale_running_tasks(self, older_than_seconds: float = 300) -> dict[str, int]:
        now = _utc_now()
        cutoff = _utc_after(-older_than_seconds)
        result: dict[str, int] = {}
        with self._lock:
            for table in ("gmap_queue", "firecrawl_queue"):
                cur = self._conn.execute(
                    f"UPDATE {table} SET status='pending', updated_at=? WHERE status='running' AND updated_at < ?",
                    (now, cutoff),
                )
                if cur.rowcount:
                    result[table] = cur.rowcount
            self._conn.commit()
        return result

    def batch_resolve_cached_firecrawl(self, cached_domains: dict[str, list[str]]) -> int:
        """启动时批量处理：域名已在缓存里的 pending/running firecrawl 任务直接标 done。"""
        if not cached_domains:
            return 0
        now = _utc_now()
        resolved = 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT fq.orgnr FROM firecrawl_queue fq "
                "JOIN companies c ON c.orgnr = fq.orgnr "
                "WHERE fq.status IN ('pending', 'running')"
            ).fetchall()
            for row in rows:
                orgnr = row[0]
                company = self._conn.execute(
                    "SELECT domain, emails_json FROM companies WHERE orgnr=?", (orgnr,)
                ).fetchone()
                if not company:
                    continue
                domain = (company["domain"] or "").strip().lower()
                if domain not in cached_domains:
                    continue
                emails = cached_domains[domain]
                old = _parse_json_list(company["emails_json"])
                merged = list(old)
                for em in emails:
                    em_lower = em.strip().lower()
                    if em_lower and em_lower not in merged:
                        merged.append(em_lower)
                self._conn.execute(
                    "UPDATE companies SET emails_json=?, firecrawl_status='done', updated_at=? WHERE orgnr=?",
                    (_dump_json_list(merged), now, orgnr),
                )
                self._conn.execute(
                    "UPDATE firecrawl_queue SET status='done', updated_at=? WHERE orgnr=?",
                    (now, orgnr),
                )
                # 写入 final_companies
                cname = self._conn.execute(
                    "SELECT company_name, representative, evidence_url FROM companies WHERE orgnr=?", (orgnr,)
                ).fetchone()
                for em in merged:
                    try:
                        self._conn.execute(
                            """INSERT OR REPLACE INTO final_companies(orgnr, company_name, representative, email, evidence_url, source, updated_at)
                               VALUES(?, ?, ?, ?, ?, 'companyname', ?)""",
                            (orgnr, cname["company_name"], cname["representative"] or "", em, cname["evidence_url"] or "", now),
                        )
                    except sqlite3.IntegrityError:
                        pass
                resolved += 1
            self._conn.commit()
        return resolved

    # ── 进度 ──

    def company_count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

    def progress(self) -> Progress:
        with self._lock:
            def _count(table, status):
                return self._conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE status=?", (status,)
                ).fetchone()[0]
            return Progress(
                gmap_pending=_count("gmap_queue", "pending"),
                gmap_running=_count("gmap_queue", "running"),
                firecrawl_pending=_count("firecrawl_queue", "pending"),
                firecrawl_running=_count("firecrawl_queue", "running"),
                companies_total=self._conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
                final_total=self._conn.execute("SELECT COUNT(DISTINCT orgnr) FROM final_companies").fetchone()[0],
            )

    # ── JSONL 导出 ──

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            rows = self._conn.execute(
                """SELECT orgnr, company_name, representative, email, evidence_url
                   FROM final_companies ORDER BY company_name, email"""
            ).fetchall()
        records = []
        for row in rows:
            records.append({
                "orgnr": row["orgnr"],
                "company_name": row["company_name"],
                "representative": row["representative"],
                "email": row["email"],
                "homepage": "",
                "phone": "",
                "evidence_url": row["evidence_url"] or "",
            })
        # 补充 homepage/phone
        with self._lock:
            company_map = {}
            for r in self._conn.execute(
                "SELECT orgnr, homepage, COALESCE(NULLIF(phone,''), gmap_phone, '') AS phone FROM companies"
            ).fetchall():
                company_map[r["orgnr"]] = (r["homepage"], r["phone"])
        for rec in records:
            hp, ph = company_map.get(rec["orgnr"], ("", ""))
            rec["homepage"] = hp
            rec["phone"] = ph
        _write_jsonl_atomic(output_dir / "final_companies.jsonl", records)
