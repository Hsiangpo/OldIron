"""Firecrawl 域名级共享缓存。"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path


RUNNING_RECHECK_SECONDS = 15.0
LOGGER = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_before(seconds: float) -> str:
    target = datetime.now(timezone.utc) - timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _seconds_until(timestamp: str, *, fallback: float) -> float:
    raw = str(timestamp or "").strip()
    if not raw:
        return max(float(fallback), 1.0)
    try:
        target = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return max(float(fallback), 1.0)
    remaining = (target - datetime.now(timezone.utc)).total_seconds()
    return max(remaining, 1.0)


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def _dump_json_list(items: list[str]) -> str:
    values: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in values:
            values.append(text)
    return json.dumps(values, ensure_ascii=False)


def _run_with_retry(conn: sqlite3.Connection, operation, attempts: int = 6):
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
            time.sleep(min(0.05 * (2**attempt), 0.5))
    raise RuntimeError("sqlite retry unreachable")


@dataclass(slots=True)
class FirecrawlDomainDecision:
    status: str
    emails: list[str]
    wait_seconds: float = 0.0
    retry_after_seconds: float = 0.0


class FirecrawlDomainCache:
    """跨站点共享的域名查询去重缓存。"""

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
        self._repair_stale_entries()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS firecrawl_domain_cache (
                    domain TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    next_retry_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self._conn.commit()

    def _repair_stale_entries(self) -> None:
        """启动时清理：删除所有 running/pending 且超过 5 分钟的域名，让它们可被重新 claim。"""
        cutoff = _utc_before(300.0)
        with self._lock:
            count = self._conn.execute(
                """
                DELETE FROM firecrawl_domain_cache
                WHERE status IN ('running', 'pending') AND updated_at < ?
                """,
                (cutoff,),
            ).rowcount
            self._conn.commit()
            if count:
                LOGGER.info("域名缓存启动清理：删除 %d 个超时 running/pending 条目", count)

    def get_all_done_domains(self) -> dict[str, list[str]]:
        """返回所有已完成域名及其邮箱，供启动时批量预处理。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT domain, emails_json FROM firecrawl_domain_cache WHERE status = 'done'"
            ).fetchall()
        return {str(r["domain"]): _parse_json_list(r["emails_json"]) for r in rows}

    def prepare_lookup(self, domain: str, *, stale_running_seconds: float = 900.0) -> FirecrawlDomainDecision:
        clean_domain = str(domain or "").strip().lower()
        if not clean_domain:
            return FirecrawlDomainDecision(status="wait", emails=[], wait_seconds=60.0)

        with self._lock:
            def _op() -> FirecrawlDomainDecision:
                now = _utc_now()
                cutoff = _utc_before(stale_running_seconds)
                row = self._conn.execute(
                    "SELECT * FROM firecrawl_domain_cache WHERE domain = ?",
                    (clean_domain,),
                ).fetchone()
                if row is None:
                    self._conn.execute("BEGIN IMMEDIATE")
                    try:
                        self._conn.execute(
                            """
                            INSERT INTO firecrawl_domain_cache(domain, status, emails_json, next_retry_at, updated_at, last_error)
                            VALUES(?, 'running', '[]', '', ?, '')
                            """,
                            (clean_domain, now),
                        )
                        self._conn.commit()
                    except sqlite3.IntegrityError:
                        self._conn.rollback()
                        return FirecrawlDomainDecision(status="wait", emails=[], wait_seconds=2.0)
                    return FirecrawlDomainDecision(status="claimed", emails=[])

                status = str(row["status"])
                if status == "done":
                    emails = _parse_json_list(row["emails_json"])
                    next_retry_at = str(row["next_retry_at"] or "").strip()
                    if emails:
                        return FirecrawlDomainDecision(status="done", emails=emails)
                    if next_retry_at and next_retry_at <= now:
                        self._conn.execute("BEGIN IMMEDIATE")
                        self._conn.execute(
                            """
                            UPDATE firecrawl_domain_cache
                            SET status = 'running', updated_at = ?, next_retry_at = '', last_error = ''
                            WHERE domain = ?
                            """,
                            (now, clean_domain),
                        )
                        self._conn.commit()
                        return FirecrawlDomainDecision(status="claimed", emails=[])
                    retry_after_seconds = (
                        _seconds_until(next_retry_at, fallback=RUNNING_RECHECK_SECONDS)
                        if next_retry_at
                        else 0.0
                    )
                    return FirecrawlDomainDecision(
                        status="done",
                        emails=[],
                        retry_after_seconds=retry_after_seconds,
                    )
                if status == "pending" and str(row["next_retry_at"] or "") > now:
                    return FirecrawlDomainDecision(
                        status="wait",
                        emails=[],
                        wait_seconds=_seconds_until(
                            str(row["next_retry_at"] or ""),
                            fallback=RUNNING_RECHECK_SECONDS,
                        ),
                    )
                if status == "running" and str(row["updated_at"] or "") > cutoff:
                    return FirecrawlDomainDecision(
                        status="wait",
                        emails=[],
                        wait_seconds=RUNNING_RECHECK_SECONDS,
                    )

                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    """
                    UPDATE firecrawl_domain_cache
                    SET status = 'running', updated_at = ?, next_retry_at = '', last_error = ''
                    WHERE domain = ?
                    """,
                    (now, clean_domain),
                )
                self._conn.commit()
                return FirecrawlDomainDecision(status="claimed", emails=[])

            return _run_with_retry(self._conn, _op)

    def mark_done(self, domain: str, emails: list[str], *, retry_after_seconds: float = 0.0) -> None:
        clean_domain = str(domain or "").strip().lower()
        with self._lock:
            def _op() -> None:
                has_emails = bool([item for item in emails if str(item or "").strip()])
                next_retry_at = "" if has_emails or retry_after_seconds <= 0 else _utc_after(retry_after_seconds)
                self._conn.execute(
                    """
                    INSERT INTO firecrawl_domain_cache(domain, status, emails_json, next_retry_at, updated_at, last_error)
                    VALUES(?, 'done', ?, ?, ?, '')
                    ON CONFLICT(domain) DO UPDATE SET
                        status = 'done',
                        emails_json = excluded.emails_json,
                        next_retry_at = excluded.next_retry_at,
                        updated_at = excluded.updated_at,
                        last_error = ''
                    """,
                    (clean_domain, _dump_json_list(emails), next_retry_at, _utc_now()),
                )
                self._conn.commit()
            _run_with_retry(self._conn, _op)

    def seed_done(self, pairs: list[tuple[str, list[str]]]) -> None:
        if not pairs:
            return
        now = _utc_now()
        with self._lock:
            def _op() -> None:
                self._conn.executemany(
                    """
                    INSERT INTO firecrawl_domain_cache(domain, status, emails_json, next_retry_at, updated_at, last_error)
                    VALUES(?, 'done', ?, '', ?, '')
                    ON CONFLICT(domain) DO UPDATE SET
                        status = CASE WHEN firecrawl_domain_cache.status = 'done' THEN firecrawl_domain_cache.status ELSE 'done' END,
                        emails_json = CASE
                            WHEN firecrawl_domain_cache.status = 'done' AND firecrawl_domain_cache.emails_json != '[]'
                                THEN firecrawl_domain_cache.emails_json
                            ELSE excluded.emails_json
                        END,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            str(domain).strip().lower(),
                            _dump_json_list(emails),
                            now,
                        )
                        for domain, emails in pairs
                        if str(domain).strip()
                    ],
                )
                self._conn.commit()
            _run_with_retry(self._conn, _op)

    def defer(self, domain: str, *, delay_seconds: float, error_text: str) -> None:
        clean_domain = str(domain or "").strip().lower()
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    """
                    INSERT INTO firecrawl_domain_cache(domain, status, emails_json, next_retry_at, updated_at, last_error)
                    VALUES(?, 'pending', '[]', ?, ?, ?)
                    ON CONFLICT(domain) DO UPDATE SET
                        status = 'pending',
                        next_retry_at = excluded.next_retry_at,
                        updated_at = excluded.updated_at,
                        last_error = excluded.last_error
                    """,
                    (clean_domain, _utc_after(delay_seconds), _utc_now(), error_text[:500]),
                )
                self._conn.commit()
            _run_with_retry(self._conn, _op)

