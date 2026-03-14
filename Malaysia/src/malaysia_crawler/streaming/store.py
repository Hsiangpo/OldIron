"""流式主流程持久化存储。"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from malaysia_crawler.common.io_utils import ensure_dir
from malaysia_crawler.streaming.store_manager import ManagerQueueMixin


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    delay = max(float(seconds), 0.0)
    target = datetime.now(timezone.utc) + timedelta(seconds=delay)
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class SnovTask:
    normalized_name: str
    company_name: str
    domain: str
    company_manager: str
    contact_email: str
    contact_phone: str
    company_id: int
    retries: int


class PipelineStore(ManagerQueueMixin):
    """维护去重、断点与任务队列。"""

    def __init__(self, db_path: str | Path) -> None:
        target = Path(db_path)
        ensure_dir(target.parent)
        self._db_path = target
        self._lock = threading.Lock()
        self._conn = self._new_connection()
        self._init_schema()
        self._apply_runtime_repairs()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def reconnect(self) -> None:
        # 中文注释：遇到 sqlite I/O 异常时允许热重连，避免整条流水线被一次瞬时故障打断。
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = self._new_connection()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ctos_pool (
                    normalized_name TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    registration_no TEXT NOT NULL DEFAULT '',
                    prefix TEXT NOT NULL DEFAULT '',
                    page INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS businesslist_scan (
                    company_id INTEGER PRIMARY KEY,
                    normalized_name TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    company_manager TEXT NOT NULL DEFAULT '',
                    contact_email TEXT NOT NULL DEFAULT '',
                    contact_phone TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snov_queue (
                    normalized_name TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    company_manager TEXT NOT NULL,
                    contact_email TEXT NOT NULL DEFAULT '',
                    contact_phone TEXT NOT NULL DEFAULT '',
                    company_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS manager_enrich_queue (
                    normalized_name TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    contact_email TEXT NOT NULL DEFAULT '',
                    contact_phone TEXT NOT NULL DEFAULT '',
                    company_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    round_index INTEGER NOT NULL DEFAULT 0,
                    candidate_pool TEXT NOT NULL DEFAULT '[]',
                    tried_urls TEXT NOT NULL DEFAULT '[]',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS final_companies (
                    normalized_name TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    company_manager TEXT NOT NULL,
                    contact_eamils TEXT NOT NULL,
                    phone TEXT NOT NULL DEFAULT '',
                    company_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._conn.commit()

    def _ensure_text_column(self, table_name: str, column_name: str, default_value: str = "") -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        escaped_default = default_value.replace("'", "''")
        self._conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} TEXT NOT NULL DEFAULT '{escaped_default}'"
        )

    def _apply_runtime_repairs(self) -> None:
        with self._lock:
            self._ensure_text_column("businesslist_scan", "contact_phone", "")
            self._ensure_text_column("snov_queue", "contact_phone", "")
            self._ensure_text_column("final_companies", "phone", "")
            # 中文注释：历史版本会把 404 页面误识别为公司详情，这里在启动时做一次自动修复。
            fixed_time = _utc_now()
            self._conn.execute(
                """
                UPDATE businesslist_scan
                SET
                    normalized_name = '',
                    company_name = '',
                    domain = '',
                    company_manager = '',
                    contact_email = '',
                    contact_phone = '',
                    status = 'miss',
                    updated_at = ?
                WHERE lower(company_name) LIKE '%404 error%page not found%'
                """,
                (fixed_time,),
            )
            # 中文注释：同步清理误入队列的 404 伪公司，避免污染后续 Snov 队列。
            self._conn.execute(
                """
                DELETE FROM snov_queue
                WHERE
                    lower(company_name) LIKE '%404 error%page not found%'
                    OR lower(normalized_name) = '404errorpagenotfound'
                """
            )
            # 中文注释：修复历史脏状态，若管理人队列已结束，businesslist_scan 不应继续停留在 queued_manager_enrich。
            self._conn.execute(
                """
                UPDATE businesslist_scan
                SET status = 'no_manager', updated_at = ?
                WHERE status = 'queued_manager_enrich'
                  AND normalized_name IN (
                      SELECT normalized_name
                      FROM manager_enrich_queue
                      WHERE status = 'failed'
                  )
                """,
                (fixed_time,),
            )
            self._conn.execute(
                """
                UPDATE businesslist_scan
                SET status = 'queued', updated_at = ?
                WHERE status = 'queued_manager_enrich'
                  AND normalized_name IN (
                      SELECT normalized_name
                      FROM manager_enrich_queue
                      WHERE status = 'done'
                  )
                """,
                (fixed_time,),
            )
            # 中文注释：成品口径收紧：final_companies 仅保留“公司名+管理人+邮箱”齐全的数据。
            self._conn.execute(
                """
                DELETE FROM final_companies
                WHERE
                    trim(company_name) = ''
                    OR trim(company_manager) = ''
                    OR trim(contact_eamils) IN ('', '[]')
                """
            )
            self._conn.commit()

    def _get_meta(self, key: str, default: str) -> str:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def _set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    def _ctos_meta_key(self, key: str, cursor_key: str) -> str:
        token = cursor_key.strip()
        if not token:
            return key
        return f"{key}:{token}"

    def load_ctos_cursor(self, prefixes: str, cursor_key: str = "") -> tuple[int, int]:
        with self._lock:
            key_prefixes = self._ctos_meta_key("ctos_prefixes", cursor_key)
            key_index = self._ctos_meta_key("ctos_prefix_index", cursor_key)
            key_page = self._ctos_meta_key("ctos_next_page", cursor_key)
            saved_prefixes = self._get_meta(key_prefixes, "")
            if saved_prefixes != prefixes:
                self._set_meta(key_prefixes, prefixes)
                self._set_meta(key_index, "0")
                self._set_meta(key_page, "1")
                self._conn.commit()
                return 0, 1
            prefix_index = int(self._get_meta(key_index, "0"))
            next_page = int(self._get_meta(key_page, "1"))
            return max(prefix_index, 0), max(next_page, 1)

    def save_ctos_cursor(self, prefix_index: int, next_page: int, cursor_key: str = "") -> None:
        with self._lock:
            key_index = self._ctos_meta_key("ctos_prefix_index", cursor_key)
            key_page = self._ctos_meta_key("ctos_next_page", cursor_key)
            self._set_meta(key_index, str(max(prefix_index, 0)))
            self._set_meta(key_page, str(max(next_page, 1)))
            self._conn.commit()

    def upsert_ctos_company(
        self,
        *,
        normalized_name: str,
        company_name: str,
        registration_no: str,
        prefix: str,
        page: int,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO ctos_pool(
                    normalized_name, company_name, registration_no, prefix, page, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(normalized_name) DO UPDATE SET
                    company_name = excluded.company_name,
                    registration_no = excluded.registration_no,
                    prefix = excluded.prefix,
                    page = excluded.page,
                    updated_at = excluded.updated_at
                """,
                (normalized_name, company_name, registration_no, prefix, page, _utc_now()),
            )
            self._conn.commit()

    def has_ctos_name(self, normalized_name: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM ctos_pool WHERE normalized_name = ? LIMIT 1",
                (normalized_name,),
            ).fetchone()
            return row is not None

    def has_ctos_registration(self, normalized_registration: str) -> bool:
        target = re.sub(r"[^a-z0-9]+", "", normalized_registration.lower())
        if not target:
            return False
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1
                FROM ctos_pool
                WHERE lower(replace(replace(registration_no, '-', ''), ' ', '')) = ?
                LIMIT 1
                """,
                (target,),
            ).fetchone()
            return row is not None

    def get_next_businesslist_id(self, default_value: int) -> int:
        with self._lock:
            raw = self._get_meta("businesslist_next_id", str(default_value))
            value = int(raw)
            # 中文注释：游标不得低于本次配置的起始 ID，避免历史低位断点导致长期空跑。
            return max(value, max(default_value, 1))

    def set_next_businesslist_id(self, next_id: int) -> None:
        with self._lock:
            self._set_meta("businesslist_next_id", str(max(next_id, 1)))
            self._conn.commit()

    def claim_next_businesslist_id(self, *, start_id: int, end_id: int) -> int | None:
        with self._lock:
            current = int(self._get_meta("businesslist_next_id", str(max(start_id, 1))))
            current = max(current, max(start_id, 1))
            if current > end_id:
                return None
            self._set_meta("businesslist_next_id", str(current + 1))
            self._conn.commit()
            return current

    def is_businesslist_scanned(self, company_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM businesslist_scan WHERE company_id = ? LIMIT 1",
                (company_id,),
            ).fetchone()
            if row is None:
                return False
            status = str(row["status"])
            # 中文注释：错误态允许重扫，避免旧错误记录导致该 ID 永久跳过。
            if status.startswith("error:"):
                return False
            return True

    def mark_businesslist_scan(
        self,
        *,
        company_id: int,
        normalized_name: str,
        company_name: str,
        domain: str,
        company_manager: str,
        contact_email: str,
        status: str,
        contact_phone: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO businesslist_scan(
                    company_id, normalized_name, company_name, domain, company_manager, contact_email, contact_phone, status, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_id) DO UPDATE SET
                    normalized_name = excluded.normalized_name,
                    company_name = excluded.company_name,
                    domain = excluded.domain,
                    company_manager = excluded.company_manager,
                    contact_email = excluded.contact_email,
                    contact_phone = excluded.contact_phone,
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                (
                    company_id,
                    normalized_name,
                    company_name,
                    domain,
                    company_manager,
                    contact_email,
                    contact_phone,
                    status,
                    _utc_now(),
                ),
            )
            self._conn.commit()

    def enqueue_snov_task(
        self,
        *,
        normalized_name: str,
        company_name: str,
        domain: str,
        company_manager: str,
        contact_email: str,
        company_id: int,
        contact_phone: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO snov_queue(
                    normalized_name, company_name, domain, company_manager, contact_email, contact_phone,
                    company_id, status, retries, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?)
                ON CONFLICT(normalized_name) DO UPDATE SET
                    company_name = excluded.company_name,
                    domain = excluded.domain,
                    company_manager = excluded.company_manager,
                    contact_email = excluded.contact_email,
                    contact_phone = excluded.contact_phone,
                    company_id = excluded.company_id,
                    status = 'pending',
                    updated_at = excluded.updated_at
                """,
                (
                    normalized_name,
                    company_name,
                    domain,
                    company_manager,
                    contact_email,
                    contact_phone,
                    company_id,
                    _utc_now(),
                ),
            )
            self._conn.commit()

    def enqueue_from_businesslist_if_ready(self, normalized_name: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT company_id, company_name, domain, company_manager, contact_email, contact_phone
                FROM businesslist_scan
                WHERE normalized_name = ? AND domain <> '' AND company_manager <> ''
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (normalized_name,),
            ).fetchone()
            if row is None:
                return False
            self._conn.execute(
                """
                INSERT INTO snov_queue(
                    normalized_name, company_name, domain, company_manager, contact_email, contact_phone,
                    company_id, status, retries, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?)
                ON CONFLICT(normalized_name) DO UPDATE SET
                    company_name = excluded.company_name,
                    domain = excluded.domain,
                    company_manager = excluded.company_manager,
                    contact_email = excluded.contact_email,
                    contact_phone = excluded.contact_phone,
                    company_id = excluded.company_id,
                    status = 'pending',
                    updated_at = excluded.updated_at
                """,
                (
                    normalized_name,
                    str(row["company_name"]),
                    str(row["domain"]),
                    str(row["company_manager"]),
                    str(row["contact_email"]),
                    str(row["contact_phone"]),
                    int(row["company_id"]),
                    _utc_now(),
                ),
            )
            self._conn.execute(
                """
                UPDATE businesslist_scan
                SET status = 'queued_late', updated_at = ?
                WHERE company_id = ?
                """,
                (_utc_now(), int(row["company_id"])),
            )
            self._conn.commit()
            return True

    def backfill_unmatched_businesslist_to_queue(self, *, batch_size: int = 1000) -> int:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT company_id, normalized_name, company_name, domain, company_manager, contact_email, contact_phone
                FROM businesslist_scan
                WHERE status = 'not_in_ctos' AND domain <> '' AND company_manager <> ''
                ORDER BY company_id ASC
                LIMIT ?
                """,
                (max(batch_size, 1),),
            ).fetchall()
            if not rows:
                return 0
            now = _utc_now()
            for row in rows:
                self._conn.execute(
                    """
                    INSERT INTO snov_queue(
                        normalized_name, company_name, domain, company_manager, contact_email, contact_phone,
                        company_id, status, retries, last_error, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?)
                    ON CONFLICT(normalized_name) DO UPDATE SET
                        company_name = excluded.company_name,
                        domain = excluded.domain,
                        company_manager = excluded.company_manager,
                        contact_email = excluded.contact_email,
                        contact_phone = excluded.contact_phone,
                        company_id = excluded.company_id,
                        status = 'pending',
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(row["normalized_name"]),
                        str(row["company_name"]),
                        str(row["domain"]),
                        str(row["company_manager"]),
                        str(row["contact_email"]),
                        str(row["contact_phone"]),
                        int(row["company_id"]),
                        now,
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE businesslist_scan
                    SET status = 'queued_without_ctos', updated_at = ?
                    WHERE company_id = ?
                    """,
                    (now, int(row["company_id"])),
                )
            self._conn.commit()
            return len(rows)

    def backfill_no_manager_to_queue(self, *, batch_size: int = 1000) -> int:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT company_id, normalized_name, company_name, domain, contact_email, contact_phone
                FROM businesslist_scan
                WHERE status = 'no_manager' AND domain <> ''
                ORDER BY company_id ASC
                LIMIT ?
                """,
                (max(batch_size, 1),),
            ).fetchall()
            if not rows:
                return 0
            now = _utc_now()
            for row in rows:
                self._conn.execute(
                    """
                    INSERT INTO snov_queue(
                        normalized_name, company_name, domain, company_manager, contact_email, contact_phone,
                        company_id, status, retries, last_error, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?)
                    ON CONFLICT(normalized_name) DO UPDATE SET
                        company_name = excluded.company_name,
                        domain = excluded.domain,
                        company_manager = excluded.company_manager,
                        contact_email = excluded.contact_email,
                        contact_phone = excluded.contact_phone,
                        company_id = excluded.company_id,
                        status = 'pending',
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(row["normalized_name"]),
                        str(row["company_name"]),
                        str(row["domain"]),
                        "",
                        str(row["contact_email"]),
                        str(row["contact_phone"]),
                        int(row["company_id"]),
                        now,
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE businesslist_scan
                    SET status = 'queued_no_manager', updated_at = ?
                    WHERE company_id = ?
                    """,
                    (now, int(row["company_id"])),
                )
            self._conn.commit()
            return len(rows)

    def claim_snov_task(self) -> SnovTask | None:
        with self._lock:
            now = _utc_now()
            row = self._conn.execute(
                """
                SELECT normalized_name, company_name, domain, company_manager, contact_email, contact_phone, company_id, retries
                FROM snov_queue
                WHERE status = 'pending' AND updated_at <= ?
                ORDER BY updated_at ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE snov_queue SET status = 'running', updated_at = ? WHERE normalized_name = ?",
                (_utc_now(), row["normalized_name"]),
            )
            self._conn.commit()
            return SnovTask(
                normalized_name=str(row["normalized_name"]),
                company_name=str(row["company_name"]),
                domain=str(row["domain"]),
                company_manager=str(row["company_manager"]),
                contact_email=str(row["contact_email"]),
                contact_phone=str(row["contact_phone"]),
                company_id=int(row["company_id"]),
                retries=int(row["retries"]),
            )

    def requeue_stale_running_tasks(self, *, older_than_seconds: int = 600) -> int:
        threshold = max(int(older_than_seconds), 1)
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=threshold)
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT normalized_name
                FROM snov_queue
                WHERE status = 'running' AND updated_at <= ?
                """,
                (cutoff,),
            ).fetchall()
            if not rows:
                return 0
            self._conn.execute(
                """
                UPDATE snov_queue
                SET status = 'pending', updated_at = ?
                WHERE status = 'running' AND updated_at <= ?
                """,
                (now, cutoff),
            )
            self._conn.commit()
            return len(rows)

    def mark_snov_done(
        self,
        *,
        normalized_name: str,
        final_status: str,
        contact_eamils: list[str],
        company_name: str,
        domain: str,
        company_manager: str,
        company_id: int,
        phone: str = "",
    ) -> bool:
        if final_status not in {"done", "no_email"}:
            raise ValueError("final_status 非法。")
        inserted = False
        with self._lock:
            manager_ready = bool(company_manager.strip())
            name_ready = bool(company_name.strip())
            email_ready = any(str(item).strip() for item in contact_eamils)
            # 中文注释：成品必须满足“公司名 + 管理人 + 邮箱”齐全，且仅 done 状态可入成品。
            if name_ready and manager_ready and email_ready and final_status == "done":
                self._conn.execute(
                    """
                    INSERT INTO final_companies(
                        normalized_name, company_name, domain, company_manager, contact_eamils, phone, company_id, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(normalized_name) DO UPDATE SET
                        company_name = excluded.company_name,
                        domain = excluded.domain,
                        company_manager = excluded.company_manager,
                        contact_eamils = excluded.contact_eamils,
                        phone = CASE
                            WHEN excluded.phone <> '' THEN excluded.phone
                            ELSE final_companies.phone
                        END,
                        company_id = excluded.company_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        normalized_name,
                        company_name,
                        domain,
                        company_manager,
                        json.dumps(contact_eamils, ensure_ascii=False),
                        phone,
                        company_id,
                        _utc_now(),
                    ),
                )
                inserted = True
            self._conn.execute(
                """
                UPDATE snov_queue
                SET status = ?, last_error = '', updated_at = ?
                WHERE normalized_name = ?
                """,
                (final_status, _utc_now(), normalized_name),
            )
            self._conn.commit()
        return inserted

    def mark_snov_failed(self, *, normalized_name: str, error_text: str, max_retries: int) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT retries FROM snov_queue WHERE normalized_name = ?",
                (normalized_name,),
            ).fetchone()
            retries = 0 if row is None else int(row["retries"])
            retries += 1
            next_status = "pending" if retries < max_retries else "failed"
            self._conn.execute(
                """
                UPDATE snov_queue
                SET retries = ?, status = ?, last_error = ?, updated_at = ?
                WHERE normalized_name = ?
                """,
                (retries, next_status, error_text[:500], _utc_now(), normalized_name),
            )
            self._conn.commit()

    def defer_snov_task(
        self,
        *,
        normalized_name: str,
        delay_seconds: float,
        error_text: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE snov_queue
                SET status = 'pending', last_error = ?, updated_at = ?
                WHERE normalized_name = ?
                """,
                (error_text[:500], _utc_after(delay_seconds), normalized_name),
            )
            self._conn.commit()

    def requeue_rate_limited_failed_tasks(self) -> int:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT normalized_name
                FROM snov_queue
                WHERE status = 'failed' AND last_error LIKE '%429%'
                """
            ).fetchall()
            if not rows:
                return 0
            now = _utc_now()
            self._conn.execute(
                """
                UPDATE snov_queue
                SET status = 'pending', retries = 0, updated_at = ?
                WHERE status = 'failed' AND last_error LIKE '%429%'
                """,
                (now,),
            )
            self._conn.commit()
            return len(rows)

    def get_stats(self) -> dict[str, int | str]:
        with self._lock:
            ctos_count = int(self._conn.execute("SELECT COUNT(*) FROM ctos_pool").fetchone()[0])
            businesslist_count = int(self._conn.execute("SELECT COUNT(*) FROM businesslist_scan").fetchone()[0])
            queue_pending = int(
                self._conn.execute("SELECT COUNT(*) FROM snov_queue WHERE status = 'pending'").fetchone()[0]
            )
            queue_running = int(
                self._conn.execute("SELECT COUNT(*) FROM snov_queue WHERE status = 'running'").fetchone()[0]
            )
            queue_failed = int(
                self._conn.execute("SELECT COUNT(*) FROM snov_queue WHERE status = 'failed'").fetchone()[0]
            )
            queue_done = int(self._conn.execute("SELECT COUNT(*) FROM snov_queue WHERE status = 'done'").fetchone()[0])
            queue_no_email = int(
                self._conn.execute("SELECT COUNT(*) FROM snov_queue WHERE status = 'no_email'").fetchone()[0]
            )
            final_count = int(self._conn.execute("SELECT COUNT(*) FROM final_companies").fetchone()[0])
            next_id = int(self._get_meta("businesslist_next_id", "1"))
            businesslist_status = {str(row["status"]): int(row["cnt"]) for row in self._conn.execute(
                "SELECT status, COUNT(*) AS cnt FROM businesslist_scan GROUP BY status"
            ).fetchall()}
            manager_status = {
                str(row["status"]): int(row["cnt"])
                for row in self._conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM manager_enrich_queue GROUP BY status"
                ).fetchall()
            }
            businesslist_miss = businesslist_status.get("miss", 0)
            businesslist_error = sum(
                count for status, count in businesslist_status.items() if status.startswith("error:")
            )
            businesslist_hit = max(businesslist_count - businesslist_miss - businesslist_error, 0)
            recent_rows = self._conn.execute(
                """
                SELECT domain, company_manager, contact_email
                FROM businesslist_scan
                ORDER BY company_id DESC
                LIMIT 200
                """
            ).fetchall()
            recent_with_domain = sum(1 for row in recent_rows if str(row["domain"]).strip())
            recent_with_manager = sum(1 for row in recent_rows if str(row["company_manager"]).strip())
            recent_with_email = sum(1 for row in recent_rows if str(row["contact_email"]).strip())
            last_success = self._conn.execute(
                """
                SELECT company_id, company_name, domain, contact_eamils, updated_at
                FROM final_companies
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            last_success_company_id = 0
            last_success_company_name = ""
            last_success_domain = ""
            last_success_updated_at = ""
            last_success_email_count = 0
            if last_success is not None:
                last_success_company_id = int(last_success["company_id"])
                last_success_company_name = str(last_success["company_name"])
                last_success_domain = str(last_success["domain"])
                last_success_updated_at = str(last_success["updated_at"])
                try:
                    payload = json.loads(str(last_success["contact_eamils"]))
                    if isinstance(payload, list):
                        last_success_email_count = len(payload)
                except json.JSONDecodeError:
                    last_success_email_count = 0
            return {
                "ctos_pool": ctos_count,
                "businesslist_scanned": businesslist_count,
                "businesslist_hit": businesslist_hit,
                "businesslist_miss": businesslist_miss,
                "businesslist_error": businesslist_error,
                "businesslist_queued": businesslist_status.get("queued", 0),
                "businesslist_queued_no_manager": businesslist_status.get("queued_no_manager", 0),
                "businesslist_queued_without_ctos": businesslist_status.get("queued_without_ctos", 0),
                "businesslist_queued_late": businesslist_status.get("queued_late", 0),
                "businesslist_queued_manager_enrich": businesslist_status.get("queued_manager_enrich", 0),
                "businesslist_no_domain": businesslist_status.get("no_domain", 0),
                "businesslist_no_manager": businesslist_status.get("no_manager", 0),
                "businesslist_not_in_ctos": businesslist_status.get("not_in_ctos", 0),
                "businesslist_invalid_name": businesslist_status.get("invalid_name", 0),
                "manager_queue_pending": manager_status.get("pending", 0),
                "manager_queue_running": manager_status.get("running", 0),
                "manager_queue_done": manager_status.get("done", 0),
                "manager_queue_failed": manager_status.get("failed", 0),
                "recent_window_size": len(recent_rows),
                "recent_with_domain": recent_with_domain,
                "recent_with_manager": recent_with_manager,
                "recent_with_email": recent_with_email,
                "queue_pending": queue_pending,
                "queue_running": queue_running,
                "queue_failed": queue_failed,
                "queue_done": queue_done,
                "queue_no_email": queue_no_email,
                "final_companies": final_count,
                "businesslist_next_id": next_id,
                "last_success_company_id": last_success_company_id,
                "last_success_company_name": last_success_company_name,
                "last_success_domain": last_success_domain,
                "last_success_updated_at": last_success_updated_at,
                "last_success_email_count": last_success_email_count,
            }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
