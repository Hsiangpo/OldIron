"""Mynavi 站点 SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from shared.oldiron_core.fc_email.normalization import join_emails

_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.jp",
    "or.jp",
    "ne.jp",
    "go.jp",
    "ac.jp",
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
}
_REP_ONLY_QUEUE_MIGRATION_VERSION = 20260408


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def build_company_key(company_name: str, website: str, address: str) -> str:
    """构建公司去重 key，优先使用官网域名。"""
    name = str(company_name or "").strip().lower()
    host = _registrable_domain(website)
    if host:
        return f"{name}|{host}"
    return f"{name}|{str(address or '').strip().lower()}"


def _registrable_domain(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    host = str(parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2


class MynaviStore:
    """线程安全的 Mynavi 数据存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._conn_lock = threading.Lock()
        self._connections: list[sqlite3.Connection] = []
        self._max_write_retries = 6
        self._init_tables()
        self._repair_legacy_company_rows()
        self._migrate_rep_only_queue()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=60000")
            self._local.conn = conn
            with self._conn_lock:
                self._connections.append(conn)
        return conn

    def close(self) -> None:
        with self._conn_lock:
            connections = list(self._connections)
            self._connections.clear()
        for conn in connections:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        if hasattr(self._local, "conn"):
            delattr(self._local, "conn")

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _run_write(self, action) -> Any:
        for attempt in range(self._max_write_retries):
            try:
                conn = self._conn()
                result = action(conn)
                conn.commit()
                return result
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower():
                    raise
                if attempt == self._max_write_retries - 1:
                    raise
                time.sleep(0.2 * (attempt + 1))
        raise RuntimeError("SQLite 写入重试失败")

    def _init_tables(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companies (
                company_id TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                representative TEXT DEFAULT '',
                website TEXT DEFAULT '',
                address TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                detail_url TEXT DEFAULT '',
                source_job_url TEXT DEFAULT '',
                emails TEXT DEFAULT '',
                gmap_status TEXT DEFAULT 'pending',
                email_status TEXT DEFAULT 'pending',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                scope TEXT PRIMARY KEY,
                last_page INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending'
            );
            """
        )
        self._ensure_company_columns(conn)
        conn.commit()

    def _ensure_company_columns(self, conn: sqlite3.Connection) -> None:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
        if "source_job_url" not in existing:
            conn.execute("ALTER TABLE companies ADD COLUMN source_job_url TEXT DEFAULT ''")

    def _repair_legacy_company_rows(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT company_id, company_name, representative, website, address, industry,
                           detail_url, source_job_url, emails, gmap_status, email_status, updated_at
                    FROM companies
                    ORDER BY updated_at, company_id
                    """
                ).fetchall()
            ]
            if not rows:
                return
            grouped: dict[str, list[dict[str, str]]] = {}
            needs_repair = False
            for row in rows:
                canonical_id = _canonical_company_id(row)
                grouped.setdefault(canonical_id, []).append(row)
                if canonical_id != str(row.get("company_id", "") or ""):
                    needs_repair = True
            if not needs_repair and all(len(items) == 1 for items in grouped.values()):
                return
            merged_rows = [_merge_company_group(company_id, items) for company_id, items in grouped.items()]
            conn.execute("DELETE FROM companies")
            for row in merged_rows:
                conn.execute(
                    """
                    INSERT INTO companies (
                        company_id, company_name, representative, website, address, industry,
                        detail_url, source_job_url, emails, gmap_status, email_status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["company_id"],
                        row["company_name"],
                        row["representative"],
                        row["website"],
                        row["address"],
                        row["industry"],
                        row["detail_url"],
                        row["source_job_url"],
                        row["emails"],
                        row["gmap_status"],
                        row["email_status"],
                        row["updated_at"],
                    ),
                )

        self._run_write(_action)

    def _migrate_rep_only_queue(self) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            current_version = int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)
            if current_version >= _REP_ONLY_QUEUE_MIGRATION_VERSION:
                return
            conn.execute(
                """
                UPDATE companies
                SET email_status = 'rep_pending'
                WHERE website != ''
                  AND website IS NOT NULL
                  AND trim(coalesce(representative, '')) = ''
                  AND email_status = 'done'
                """
            )
            conn.execute(f"PRAGMA user_version = {_REP_ONLY_QUEUE_MIGRATION_VERSION}")

        self._run_write(_action)

    def upsert_companies(self, companies: list[dict[str, str]]) -> int:
        def _action(conn: sqlite3.Connection) -> int:
            inserted = 0
            for company in companies:
                company_name = str(company.get("company_name", "") or "").strip()
                raw_company_id = str(company.get("company_id", "") or "").strip()
                company_id = ""
                if company_name and (str(company.get("website", "") or "").strip() or str(company.get("address", "") or "").strip()):
                    company_id = build_company_key(
                        company_name,
                        str(company.get("website", "") or ""),
                        str(company.get("address", "") or ""),
                    )
                if not company_id:
                    company_id = raw_company_id
                if not company_id or not company_name:
                    continue
                existed = conn.execute(
                    "SELECT 1 FROM companies WHERE company_id = ? LIMIT 1",
                    (company_id,),
                ).fetchone() is not None
                conn.execute(
                    """
                    INSERT INTO companies (
                        company_id, company_name, representative, website,
                        address, industry, detail_url, source_job_url, emails, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(company_id) DO UPDATE SET
                        company_name = excluded.company_name,
                        representative = CASE
                            WHEN excluded.representative NOT IN ('', '-')
                            THEN excluded.representative
                            ELSE companies.representative
                        END,
                        website = CASE WHEN excluded.website != '' THEN excluded.website ELSE companies.website END,
                        address = CASE WHEN excluded.address != '' THEN excluded.address ELSE companies.address END,
                        industry = CASE WHEN excluded.industry != '' THEN excluded.industry ELSE companies.industry END,
                        detail_url = CASE WHEN excluded.detail_url != '' THEN excluded.detail_url ELSE companies.detail_url END,
                        source_job_url = CASE WHEN excluded.source_job_url != '' THEN excluded.source_job_url ELSE companies.source_job_url END,
                        emails = CASE WHEN excluded.emails != '' THEN excluded.emails ELSE companies.emails END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        company_id,
                        company_name,
                        company.get("representative", ""),
                        company.get("website", ""),
                        company.get("address", ""),
                        company.get("industry", ""),
                        company.get("detail_url", ""),
                        company.get("source_job_url", ""),
                        company.get("emails", ""),
                        _now_text(),
                    ),
                )
                inserted += int(not existed)
            return inserted

        return int(self._run_write(_action) or 0)

    def get_company_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM companies").fetchone()
        return int(row["cnt"] if row else 0)

    def get_checkpoint(self, scope: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute(
            "SELECT last_page, total_pages, status FROM checkpoints WHERE scope = ?",
            (scope,),
        ).fetchone()
        return dict(row) if row else None

    def update_checkpoint(self, scope: str, last_page: int, total_pages: int, status: str = "running") -> None:
        self._run_write(
            lambda conn: conn.execute(
                """
                INSERT INTO checkpoints (scope, last_page, total_pages, status)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    last_page = excluded.last_page,
                    total_pages = excluded.total_pages,
                    status = excluded.status
                """,
                (scope, last_page, total_pages, status),
            )
        )

    def get_gmap_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_id, company_name, address
            FROM companies
            WHERE (website = '' OR website IS NULL)
              AND (gmap_status = 'pending' OR gmap_status IS NULL)
            ORDER BY company_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def update_website(self, company_id: str, website: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET website = ?, gmap_status = 'done', updated_at = ? WHERE company_id = ?",
                (website, _now_text(), company_id),
            )
        )

    def mark_gmap_done(self, company_id: str) -> None:
        self._run_write(
            lambda conn: conn.execute(
                "UPDATE companies SET gmap_status = 'done', updated_at = ? WHERE company_id = ?",
                (_now_text(), company_id),
            )
        )

    def get_email_pending(self, limit: int = 0) -> list[dict[str, str]]:
        conn = self._conn()
        sql = """
            SELECT company_id, company_name, address, website, representative
            FROM companies
            WHERE website != '' AND website IS NOT NULL
              AND (email_status IN ('pending', 'rep_pending') OR email_status IS NULL)
            ORDER BY company_id
        """
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def save_email_result(self, company_id: str, emails: list[str], representative: str = "") -> None:
        email_str = join_emails(emails)

        def _action(conn: sqlite3.Connection) -> None:
            if representative:
                conn.execute(
                    """
                    UPDATE companies
                    SET emails = ?, email_status = 'done', representative = ?, updated_at = ?
                    WHERE company_id = ?
                    """,
                    (email_str, representative, _now_text(), company_id),
                )
                return
            conn.execute(
                "UPDATE companies SET emails = ?, email_status = 'done', updated_at = ? WHERE company_id = ?",
                (email_str, _now_text(), company_id),
            )

        self._run_write(_action)

    def export_all_companies(self) -> list[dict[str, str]]:
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT company_name, representative, website, address, industry, detail_url, source_job_url, emails
            FROM companies
            ORDER BY company_id
            """
        ).fetchall()
        return [dict(row) for row in rows]


def _canonical_company_id(row: dict[str, str]) -> str:
    company_name = str(row.get("company_name", "") or "").strip()
    website = str(row.get("website", "") or "").strip()
    address = str(row.get("address", "") or "").strip()
    if company_name and (website or address):
        return build_company_key(company_name, website, address)
    return str(row.get("company_id", "") or "").strip()


def _merge_company_group(company_id: str, rows: list[dict[str, str]]) -> dict[str, str]:
    best = max(rows, key=_company_row_score)
    merged = dict(best)
    merged["company_id"] = company_id
    merged["website"] = _pick_best_website(rows)
    merged["address"] = _pick_best_text(rows, "address")
    merged["industry"] = _pick_best_text(rows, "industry")
    merged["detail_url"] = _pick_best_text(rows, "detail_url")
    merged["source_job_url"] = _pick_best_text(rows, "source_job_url")
    merged["representative"] = _pick_best_representative(rows)
    merged["emails"] = join_emails(_collect_text_values(rows, "emails"))
    merged["gmap_status"] = "done" if merged["website"] else "pending"
    merged["email_status"] = _merge_email_status(rows, merged["website"], merged["emails"])
    merged["updated_at"] = max(str(row.get("updated_at", "") or "") for row in rows)
    return merged


def _company_row_score(row: dict[str, str]) -> tuple[int, int, int, int]:
    website = str(row.get("website", "") or "").strip()
    emails = join_emails(str(row.get("emails", "") or ""))
    representative = str(row.get("representative", "") or "").strip()
    return (
        1 if website else 0,
        len(emails.split("; ")) if emails else 0,
        _representative_score(representative),
        1 if str(row.get("detail_url", "") or "").strip() else 0,
    )


def _pick_best_text(rows: list[dict[str, str]], field: str) -> str:
    values = [str(row.get(field, "") or "").strip() for row in rows if str(row.get(field, "") or "").strip()]
    if not values:
        return ""
    return max(values, key=lambda item: (len(item), item))


def _pick_best_website(rows: list[dict[str, str]]) -> str:
    values = [str(row.get("website", "") or "").strip() for row in rows if str(row.get("website", "") or "").strip()]
    if not values:
        return ""
    return min(values, key=lambda item: (_website_depth(item), len(item), item))


def _website_depth(value: str) -> int:
    parsed = urlparse(str(value or "").strip())
    parts = [part for part in str(parsed.path or "").split("/") if part]
    return len(parts)


def _pick_best_representative(rows: list[dict[str, str]]) -> str:
    values = [str(row.get("representative", "") or "").strip() for row in rows if str(row.get("representative", "") or "").strip() not in {"", "-"}]
    if not values:
        return ""
    return max(values, key=lambda item: (_representative_score(item), -len(item), item))


def _representative_score(value: str) -> int:
    text = str(value or "").strip()
    if not text or text == "-":
        return -100
    score = 0
    for keyword in ("代表取締役社長", "代表取締役", "代表執行役社長", "代表執行役", "理事長", "学長", "社長", "会長", "CEO"):
        if keyword.lower() in text.lower():
            score += 100
    if any(token in text for token in ("／", "/", "、", ";", ",")):
        score -= 20
    if len(text) <= 12:
        score += 10
    return score


def _collect_text_values(rows: list[dict[str, str]], field: str) -> list[str]:
    values: list[str] = []
    for row in rows:
        text = str(row.get(field, "") or "").strip()
        if text:
            values.append(text)
    return values


def _merge_email_status(rows: list[dict[str, str]], website: str, emails: str) -> str:
    if emails:
        return "done"
    if not website:
        return "done"
    if any(str(row.get("email_status", "") or "").strip() == "pending" for row in rows):
        return "pending"
    return "done"

