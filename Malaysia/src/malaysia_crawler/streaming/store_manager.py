"""管理人补全队列存储混入。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    delay = max(float(seconds), 0.0)
    target = datetime.now(timezone.utc) + timedelta(seconds=delay)
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class ManagerTask:
    normalized_name: str
    company_name: str
    domain: str
    contact_email: str
    contact_phone: str
    company_id: int
    retries: int
    round_index: int
    candidate_pool: list[str]
    tried_urls: list[str]


class ManagerQueueMixin:
    """为 PipelineStore 提供管理人队列方法。"""

    def _to_json_list(self, raw: str) -> list[str]:
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            if not isinstance(item, str):
                continue
            text = item.strip()
            if text:
                result.append(text)
        return result

    def _dump_json_list(self, items: list[str]) -> str:
        cleaned = [str(item).strip() for item in items if str(item).strip()]
        return json.dumps(list(dict.fromkeys(cleaned)), ensure_ascii=False)

    def enqueue_manager_task(
        self,
        *,
        normalized_name: str,
        company_name: str,
        domain: str,
        contact_email: str,
        company_id: int,
        contact_phone: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO manager_enrich_queue(
                    normalized_name, company_name, domain, contact_email, contact_phone,
                    company_id, status, retries, round_index, candidate_pool, tried_urls, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', 0, 0, '[]', '[]', '', ?)
                ON CONFLICT(normalized_name) DO UPDATE SET
                    company_name = excluded.company_name,
                    domain = excluded.domain,
                    contact_email = excluded.contact_email,
                    contact_phone = excluded.contact_phone,
                    company_id = excluded.company_id,
                    status = 'pending',
                    retries = 0,
                    round_index = 0,
                    candidate_pool = '[]',
                    tried_urls = '[]',
                    last_error = '',
                    updated_at = excluded.updated_at
                """,
                (
                    normalized_name,
                    company_name,
                    domain,
                    contact_email,
                    contact_phone,
                    company_id,
                    _utc_now(),
                ),
            )
            self._conn.commit()

    def claim_manager_task(self) -> ManagerTask | None:
        with self._lock:
            now = _utc_now()
            row = self._conn.execute(
                """
                SELECT normalized_name, company_name, domain, contact_email, contact_phone, company_id,
                       retries, round_index, candidate_pool, tried_urls
                FROM manager_enrich_queue
                WHERE status = 'pending' AND updated_at <= ?
                ORDER BY updated_at ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                """
                UPDATE manager_enrich_queue
                SET status = 'running', updated_at = ?
                WHERE normalized_name = ?
                """,
                (_utc_now(), str(row["normalized_name"])),
            )
            self._conn.commit()
            return ManagerTask(
                normalized_name=str(row["normalized_name"]),
                company_name=str(row["company_name"]),
                domain=str(row["domain"]),
                contact_email=str(row["contact_email"]),
                contact_phone=str(row["contact_phone"]),
                company_id=int(row["company_id"]),
                retries=int(row["retries"]),
                round_index=int(row["round_index"]),
                candidate_pool=self._to_json_list(str(row["candidate_pool"])),
                tried_urls=self._to_json_list(str(row["tried_urls"])),
            )

    def defer_manager_task(
        self,
        *,
        normalized_name: str,
        delay_seconds: float,
        retries: int,
        round_index: int,
        candidate_pool: list[str],
        tried_urls: list[str],
        error_text: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE manager_enrich_queue
                SET status = 'pending',
                    retries = ?,
                    round_index = ?,
                    candidate_pool = ?,
                    tried_urls = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE normalized_name = ?
                """,
                (
                    max(retries, 0),
                    max(round_index, 0),
                    self._dump_json_list(candidate_pool),
                    self._dump_json_list(tried_urls),
                    error_text[:500],
                    _utc_after(delay_seconds),
                    normalized_name,
                ),
            )
            self._conn.commit()

    def mark_manager_done(
        self,
        *,
        normalized_name: str,
        retries: int,
        round_index: int,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE manager_enrich_queue
                SET status = 'done',
                    retries = ?,
                    round_index = ?,
                    candidate_pool = ?,
                    tried_urls = ?,
                    last_error = '',
                    updated_at = ?
                WHERE normalized_name = ?
                """,
                (
                    max(retries, 0),
                    max(round_index, 0),
                    self._dump_json_list(candidate_pool),
                    self._dump_json_list(tried_urls),
                    _utc_now(),
                    normalized_name,
                ),
            )
            self._conn.commit()

    def mark_manager_failed(
        self,
        *,
        normalized_name: str,
        retries: int,
        round_index: int,
        candidate_pool: list[str],
        tried_urls: list[str],
        error_text: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE manager_enrich_queue
                SET status = 'failed',
                    retries = ?,
                    round_index = ?,
                    candidate_pool = ?,
                    tried_urls = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE normalized_name = ?
                """,
                (
                    max(retries, 0),
                    max(round_index, 0),
                    self._dump_json_list(candidate_pool),
                    self._dump_json_list(tried_urls),
                    error_text[:500],
                    _utc_now(),
                    normalized_name,
                ),
            )
            self._conn.commit()

    def requeue_stale_running_manager_tasks(self, *, older_than_seconds: int = 600) -> int:
        threshold = max(int(older_than_seconds), 1)
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=threshold)
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT normalized_name
                FROM manager_enrich_queue
                WHERE status = 'running' AND updated_at <= ?
                """,
                (cutoff,),
            ).fetchall()
            if not rows:
                return 0
            self._conn.execute(
                """
                UPDATE manager_enrich_queue
                SET status = 'pending', updated_at = ?
                WHERE status = 'running' AND updated_at <= ?
                """,
                (now, cutoff),
            )
            self._conn.commit()
            return len(rows)

