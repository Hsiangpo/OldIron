"""England 集群运行时仓储。"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

from england_crawler.cluster.config import ClusterConfig
from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.task_ops import ClusterTaskOpsMixin
from england_crawler.companies_house.client import normalize_company_name
from england_crawler.companies_house.input_xlsx import iter_company_names_from_xlsx
from england_crawler.dnb.catalog import build_industry_seed_segments
from england_crawler.snov.client import extract_domain


DNB_PIPELINE = "england_dnb"
CH_PIPELINE = "england_companies_house"
FIRECRAWL_WAIT_SECONDS = 15.0
logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_after(seconds: float) -> datetime:
    return _utc_now() + timedelta(seconds=max(float(seconds), 0.0))


def _build_task_id(pipeline: str, task_type: str, entity_id: str) -> str:
    return f"{pipeline}:{task_type}:{entity_id}"


def _clip_text(value: object, limit: int = 500) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _parse_json_list(raw: object) -> list[str]:
    if isinstance(raw, list):
        source = raw
    else:
        try:
            source = json.loads(str(raw or "[]"))
        except json.JSONDecodeError:
            source = []
    if not isinstance(source, list):
        return []
    values: list[str] = []
    for item in source:
        text = str(item or "").strip().lower()
        if text and text not in values:
            values.append(text)
    return values


def _dump_json_list(items: list[str]) -> Jsonb:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text and text not in cleaned:
            cleaned.append(text)
    return Jsonb(cleaned)


def _merge_text(current: object, incoming: object) -> str:
    fresh = str(incoming or "").strip()
    return fresh or str(current or "").strip()


def _build_comp_id(company_name: str) -> str:
    normalized = normalize_company_name(company_name)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def _source_fingerprint(path: Path) -> str:
    stat = path.resolve().stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _source_scope(max_companies: int) -> str:
    return f"limit:{max_companies}" if max(int(max_companies), 0) > 0 else "full"


def _source_key(path: Path, scope: str) -> str:
    return f"{path.resolve()}|{scope.strip() or 'full'}"


@dataclass(slots=True)
class ClaimedTask:
    task_id: str
    pipeline: str
    task_type: str
    entity_id: str
    retries: int
    payload: dict[str, object]


@dataclass(slots=True)
class FirecrawlKeyLease:
    key_hash: str
    key_value: str


class ClusterRepository(ClusterTaskOpsMixin):
    """England 集群核心仓储。"""

    _utc_now = staticmethod(_utc_now)
    _utc_after = staticmethod(_utc_after)
    _build_task_id = staticmethod(_build_task_id)
    _clip_text = staticmethod(_clip_text)
    _parse_json_list = staticmethod(_parse_json_list)
    _dump_json_list = staticmethod(_dump_json_list)
    _merge_text = staticmethod(_merge_text)

    def __init__(self, db: ClusterDb, config: ClusterConfig) -> None:
        self._db = db
        self._config = config

    def register_worker(
        self,
        *,
        worker_id: str,
        host_name: str,
        platform: str,
        capabilities: list[str],
        git_commit: str,
        python_version: str,
    ) -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cluster_workers(
                        worker_id, host_name, platform, capabilities_json, git_commit, python_version,
                        status, last_heartbeat_at, created_at, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, 'online', %s, %s, %s)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        host_name = excluded.host_name,
                        platform = excluded.platform,
                        capabilities_json = excluded.capabilities_json,
                        git_commit = excluded.git_commit,
                        python_version = excluded.python_version,
                        status = 'online',
                        last_heartbeat_at = excluded.last_heartbeat_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        worker_id,
                        host_name.strip(),
                        platform.strip(),
                        Jsonb(capabilities),
                        git_commit.strip(),
                        python_version.strip(),
                        now,
                        now,
                        now,
                    ),
                )

    def heartbeat(self, worker_id: str) -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cluster_workers
                    SET status = 'online', last_heartbeat_at = %s, updated_at = %s
                    WHERE worker_id = %s
                    """,
                    (now, now, worker_id),
                )

    def requeue_expired_tasks(self) -> int:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE england_cluster_tasks
                    SET status = 'pending', lease_owner = '', lease_expires_at = NULL, next_run_at = %s, updated_at = %s
                    WHERE status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= %s
                    """,
                    (now, now, now),
                )
                task_rows = cur.rowcount
                cur.execute(
                    """
                    UPDATE england_firecrawl_domain_cache
                    SET status = 'pending', lease_owner = '', lease_expires_at = NULL, next_retry_at = %s, updated_at = %s
                    WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= %s
                    """,
                    (now, now, now),
                )
                cur.execute(
                    """
                    UPDATE england_firecrawl_keys
                    SET in_flight = 0, lease_owner = '', lease_expires_at = NULL, updated_at = %s
                    WHERE lease_expires_at IS NOT NULL AND lease_expires_at <= %s
                    """,
                    (now, now),
                )
        return task_rows

    def claim_task(self, worker_id: str, capabilities: list[str]) -> ClaimedTask | None:
        supported = {item.strip() for item in capabilities if str(item).strip()}
        self.requeue_expired_tasks()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                for _ in range(64):
                    row = self._select_claimable_task(cur)
                    if row is None:
                        return None
                    task = ClaimedTask(
                        task_id=str(row["task_id"]),
                        pipeline=str(row["pipeline"]),
                        task_type=str(row["task_type"]),
                        entity_id=str(row["entity_id"]),
                        retries=int(row["retries"]),
                        payload=dict(row["payload_json"] or {}),
                    )
                    if self._task_already_done_locked(cur, task):
                        self._mark_task_done_locked(cur, task.task_id)
                        continue
                    if supported and task.task_type not in supported and task.pipeline not in supported:
                        cur.execute(
                            "UPDATE england_cluster_tasks SET next_run_at = %s, updated_at = %s WHERE task_id = %s",
                            (_utc_after(5.0), _utc_now(), task.task_id),
                        )
                        continue
                    if task.task_type in {"dnb_firecrawl", "ch_firecrawl"}:
                        action = self._prepare_firecrawl_domain_for_task(cur, task, worker_id)
                        if action != "claim":
                            continue
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'leased', lease_owner = %s, lease_expires_at = %s, updated_at = %s
                        WHERE task_id = %s
                        """,
                        (worker_id, _utc_after(self._config.task_lease_seconds), _utc_now(), task.task_id),
                    )
                    return task
        return None

    def complete_task(self, *, task_id: str, worker_id: str, result: dict[str, object]) -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                row = self._load_leased_task(cur, task_id, worker_id)
                if row is None:
                    if self._is_stale_task_callback_locked(cur, task_id, worker_id):
                        return
                    raise RuntimeError("任务不存在或不属于当前 worker。")
                task = ClaimedTask(
                    task_id=str(row["task_id"]),
                    pipeline=str(row["pipeline"]),
                    task_type=str(row["task_type"]),
                    entity_id=str(row["entity_id"]),
                    retries=int(row["retries"]),
                    payload=dict(row["payload_json"] or {}),
                )
                task_state = self._apply_task_completion(cur, task, result)
                cur.execute(
                    """
                    INSERT INTO england_cluster_task_attempts(task_id, worker_id, result_status, error_text, started_at, finished_at)
                    VALUES(%s, %s, %s, '', %s, %s)
                    """,
                    (task.task_id, worker_id, task_state, now, now),
                )
                if task_state == "done":
                    self._mark_task_done_locked(cur, task.task_id)

    def renew_task_lease(self, *, task_id: str, worker_id: str) -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                row = self._load_leased_task(cur, task_id, worker_id)
                if row is None:
                    if self._is_stale_task_callback_locked(cur, task_id, worker_id):
                        return
                    raise RuntimeError("任务不存在或不属于当前 worker。")
                lease_expires_at = _utc_after(self._config.task_lease_seconds)
                cur.execute(
                    """
                    UPDATE england_cluster_tasks
                    SET lease_expires_at = %s, updated_at = %s
                    WHERE task_id = %s
                    """,
                    (lease_expires_at, now, task_id),
                )
                task_type = str(row["task_type"])
                payload = dict(row["payload_json"] or {})
                if task_type in {"dnb_firecrawl", "ch_firecrawl"}:
                    domain = str(payload.get("domain", "")).strip().lower() or extract_domain(str(payload.get("homepage", "")).strip())
                    if domain:
                        cur.execute(
                            """
                            UPDATE england_firecrawl_domain_cache
                            SET lease_expires_at = %s, updated_at = %s
                            WHERE domain = %s AND status = 'running' AND lease_owner = %s
                            """,
                            (lease_expires_at, now, domain, worker_id),
                        )

    def fail_task(self, *, task_id: str, worker_id: str, error_text: str, retry_delay_seconds: float, fatal: bool) -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                row = self._load_leased_task(cur, task_id, worker_id)
                if row is None:
                    if self._is_stale_task_callback_locked(cur, task_id, worker_id):
                        return
                    raise RuntimeError("任务不存在或不属于当前 worker。")
                task = ClaimedTask(
                    task_id=str(row["task_id"]),
                    pipeline=str(row["pipeline"]),
                    task_type=str(row["task_type"]),
                    entity_id=str(row["entity_id"]),
                    retries=int(row["retries"]),
                    payload=dict(row["payload_json"] or {}),
                )
                attempt = task.retries + 1
                terminal = fatal or attempt >= self._task_retry_limit(task.task_type)
                self._apply_task_failure_side_effect(cur, task, error_text, retry_delay_seconds, terminal)
                cur.execute(
                    """
                    INSERT INTO england_cluster_task_attempts(task_id, worker_id, result_status, error_text, started_at, finished_at)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    """,
                    (task.task_id, worker_id, "failed" if terminal else "retry", _clip_text(error_text), now, now),
                )
                if terminal:
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'failed', retries = %s, lease_owner = '', lease_expires_at = NULL, last_error = %s, updated_at = %s
                        WHERE task_id = %s
                        """,
                        (attempt, _clip_text(error_text), _utc_now(), task.task_id),
                    )
                else:
                    self._reschedule_task_locked(cur, task.task_id, retries=attempt, delay_seconds=retry_delay_seconds, error_text=error_text, payload=task.payload)

    def acquire_firecrawl_key(self, worker_id: str) -> FirecrawlKeyLease:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE england_firecrawl_keys
                    SET state = 'active', cooldown_until = NULL, updated_at = %s
                    WHERE state = 'cooldown' AND cooldown_until IS NOT NULL AND cooldown_until <= %s
                    """,
                    (now, now),
                )
                cur.execute(
                    """
                    SELECT key_hash, key_value
                    FROM england_firecrawl_keys
                    WHERE state != 'disabled' AND (cooldown_until IS NULL OR cooldown_until <= %s) AND in_flight < %s
                    ORDER BY in_flight ASC, COALESCE(last_used_at, '1970-01-01'::timestamptz) ASC, key_hash ASC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (now, self._config.firecrawl_key_per_limit),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("没有可用 firecrawl key。")
                cur.execute(
                    """
                    UPDATE england_firecrawl_keys
                    SET in_flight = in_flight + 1, lease_owner = %s, lease_expires_at = %s, last_used_at = %s, updated_at = %s
                    WHERE key_hash = %s
                    """,
                    (worker_id, _utc_after(self._config.task_lease_seconds), now, now, str(row["key_hash"])),
                )
                return FirecrawlKeyLease(key_hash=str(row["key_hash"]), key_value=str(row["key_value"]))

    def release_firecrawl_key(self, *, key_hash: str, outcome: str, retry_after_seconds: float = 0.0, reason: str = "") -> None:
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                row = cur.execute("SELECT failure_count FROM england_firecrawl_keys WHERE key_hash = %s", (key_hash,)).fetchone()
                if row is None:
                    return
                failure_count = int(row["failure_count"] or 0)
                in_flight_sql = "GREATEST(in_flight - 1, 0)"
                if outcome == "success":
                    cur.execute(
                        f"UPDATE england_firecrawl_keys SET in_flight = {in_flight_sql}, state = 'active', failure_count = 0, cooldown_until = NULL, lease_owner = '', lease_expires_at = NULL, disabled_reason = '', updated_at = %s WHERE key_hash = %s",
                        (now, key_hash),
                    )
                elif outcome == "rate_limited":
                    cur.execute(
                        f"UPDATE england_firecrawl_keys SET in_flight = {in_flight_sql}, state = 'cooldown', failure_count = failure_count + 1, cooldown_until = %s, lease_owner = '', lease_expires_at = NULL, updated_at = %s WHERE key_hash = %s",
                        (_utc_after(retry_after_seconds or self._config.firecrawl_key_cooldown_seconds), now, key_hash),
                    )
                elif outcome == "disable":
                    cur.execute(
                        f"UPDATE england_firecrawl_keys SET in_flight = {in_flight_sql}, state = 'disabled', cooldown_until = NULL, lease_owner = '', lease_expires_at = NULL, disabled_reason = %s, updated_at = %s WHERE key_hash = %s",
                        (_clip_text(reason, 200), now, key_hash),
                    )
                else:
                    next_failure = failure_count + 1
                    if next_failure >= self._config.firecrawl_key_failure_threshold:
                        cur.execute(
                            f"UPDATE england_firecrawl_keys SET in_flight = {in_flight_sql}, state = 'cooldown', failure_count = %s, cooldown_until = %s, lease_owner = '', lease_expires_at = NULL, updated_at = %s WHERE key_hash = %s",
                            (next_failure, _utc_after(self._config.firecrawl_key_cooldown_seconds), now, key_hash),
                        )
                    else:
                        cur.execute(
                            f"UPDATE england_firecrawl_keys SET in_flight = {in_flight_sql}, failure_count = %s, lease_owner = '', lease_expires_at = NULL, updated_at = %s WHERE key_hash = %s",
                            (next_failure, now, key_hash),
                        )

    def submit_dnb_seed_tasks(self) -> int:
        count = 0
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                for segment in build_industry_seed_segments("gb"):
                    cur.execute(
                        """
                        INSERT INTO england_dnb_discovery_nodes(
                            segment_id, industry_path, country_iso_two_code, region_name, city_name,
                            expected_count, task_status, task_retries, updated_at
                        ) VALUES(%s, %s, %s, %s, %s, %s, 'pending', 0, %s)
                        ON CONFLICT(segment_id) DO NOTHING
                        """,
                        (segment.segment_id, segment.industry_path, segment.country_iso_two_code, segment.region_name, segment.city_name, segment.expected_count, _utc_now()),
                    )
                    cur.execute(
                        "SELECT task_status FROM england_dnb_discovery_nodes WHERE segment_id = %s",
                        (segment.segment_id,),
                    )
                    row = cur.fetchone()
                    if row is not None and str(row["task_status"] or "").strip() == "done":
                        continue
                    count += self._upsert_task_locked(
                        cur,
                        pipeline=DNB_PIPELINE,
                        task_type="dnb_discovery",
                        entity_id=segment.segment_id,
                        payload=segment.to_dict(),
                        force_pending=True,
                    )
        return count

    def get_dnb_source_state(self) -> str:
        counts = self._get_pipeline_task_counts(DNB_PIPELINE)
        if counts["pending"] > 0 or counts["leased"] > 0:
            return "in_progress"
        if counts["failed"] > 0:
            return "failed_only"
        if self._has_dnb_history():
            return "done"
        return "uninitialized"

    def get_companies_house_source_state(self, input_xlsx: Path, max_companies: int = 0) -> str:
        counts = self._get_pipeline_task_counts(CH_PIPELINE)
        if counts["pending"] > 0 or counts["leased"] > 0:
            return "in_progress"
        if not self._is_companies_house_source_loaded(input_xlsx, max_companies=max_companies):
            return "uninitialized"
        if counts["failed"] > 0:
            return "failed_only"
        return "done"

    def requeue_failed_tasks_for_pipeline(self, pipeline: str) -> int:
        normalized = str(pipeline or "").strip()
        if normalized not in {DNB_PIPELINE, CH_PIPELINE}:
            return 0
        count = 0
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT task_id, task_type, entity_id
                    FROM england_cluster_tasks
                    WHERE pipeline = %s AND status = 'failed'
                    ORDER BY task_id
                    FOR UPDATE
                    """,
                    (normalized,),
                )
                for row in cur.fetchall():
                    task_id = str(row["task_id"])
                    task_type = str(row["task_type"])
                    entity_id = str(row["entity_id"])
                    if normalized == DNB_PIPELINE:
                        self._requeue_failed_dnb_task_locked(cur, task_type=task_type, entity_id=entity_id)
                    else:
                        self._requeue_failed_ch_task_locked(cur, task_type=task_type, entity_id=entity_id)
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'pending', retries = 0, next_run_at = %s, lease_owner = '',
                            lease_expires_at = NULL, last_error = '', updated_at = %s
                        WHERE task_id = %s
                        """,
                        (now, now, task_id),
                    )
                    count += 1
        return count

    def submit_companies_house_input(self, input_xlsx: Path, max_companies: int = 0) -> int:
        source_path = Path(input_xlsx).resolve()
        scope = _source_scope(max_companies)
        fingerprint = _source_fingerprint(source_path)
        inserted = 0
        total_rows = 0
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                if self._source_is_loaded_locked(
                    cur,
                    source_path=source_path,
                    fingerprint=fingerprint,
                    scope=scope,
                ):
                    logger.info("Companies House 输入未变化，跳过重复补种：%s | 范围=%s", source_path, scope)
                    return 0
                for company_name in iter_company_names_from_xlsx(source_path, limit=max_companies):
                    normalized_name = normalize_company_name(company_name)
                    if not normalized_name:
                        continue
                    total_rows += 1
                    comp_id = _build_comp_id(company_name)
                    cur.execute(
                        """
                        INSERT INTO england_ch_companies(comp_id, company_name, normalized_name, updated_at)
                        VALUES(%s, %s, %s, %s)
                        ON CONFLICT(comp_id) DO UPDATE SET
                            company_name = CASE WHEN england_ch_companies.company_name = '' THEN EXCLUDED.company_name ELSE england_ch_companies.company_name END,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (comp_id, company_name.strip(), normalized_name, _utc_now()),
                    )
                    cur.execute(
                        """
                        SELECT ch_task_status, gmap_task_status
                        FROM england_ch_companies
                        WHERE comp_id = %s
                        """,
                        (comp_id,),
                    )
                    current = cur.fetchone()
                    payload = {
                        "comp_id": comp_id,
                        "company_name": company_name.strip(),
                        "company_number": "",
                        "homepage": "",
                        "domain": "",
                    }
                    if current is None or str(current["ch_task_status"] or "").strip() != "done":
                        inserted += self._upsert_task_locked(
                            cur,
                            pipeline=CH_PIPELINE,
                            task_type="ch_lookup",
                            entity_id=comp_id,
                            payload=payload,
                            force_pending=True,
                        )
                    if current is None or str(current["gmap_task_status"] or "").strip() != "done":
                        self._upsert_task_locked(
                            cur,
                            pipeline=CH_PIPELINE,
                            task_type="ch_gmap",
                            entity_id=comp_id,
                            payload=payload,
                            force_pending=True,
                        )
                self._mark_source_loaded_locked(
                    cur,
                    source_path=source_path,
                    fingerprint=fingerprint,
                    total_rows=total_rows,
                    scope=scope,
                )
        return inserted

    def reconcile_company_backed_task_states(self) -> int:
        affected = 0
        now = _utc_now()
        with self._db.transaction() as conn:
            with conn.cursor() as cur:
                for task_type, column in (
                    ("ch_lookup", "ch_task_status"),
                    ("ch_gmap", "gmap_task_status"),
                    ("ch_firecrawl", "firecrawl_task_status"),
                ):
                    cur.execute(
                        f"""
                        UPDATE england_cluster_tasks AS t
                        SET status = 'done', lease_owner = '', lease_expires_at = NULL, last_error = '', updated_at = %s
                        FROM england_ch_companies AS c
                        WHERE t.pipeline = %s
                          AND t.task_type = %s
                          AND t.entity_id = c.comp_id
                          AND c.{column} = 'done'
                          AND t.status <> 'done'
                        """,
                        (now, CH_PIPELINE, task_type),
                    )
                    affected += cur.rowcount
                cur.execute(
                    """
                    UPDATE england_cluster_tasks AS t
                    SET status = 'done', lease_owner = '', lease_expires_at = NULL, last_error = '', updated_at = %s
                    FROM england_dnb_discovery_nodes AS d
                    WHERE t.pipeline = %s
                      AND t.task_type = 'dnb_discovery'
                      AND t.entity_id = d.segment_id
                      AND d.task_status = 'done'
                      AND t.status <> 'done'
                    """,
                    (now, DNB_PIPELINE),
                )
                affected += cur.rowcount
                cur.execute(
                    """
                    UPDATE england_cluster_tasks AS t
                    SET status = 'done', lease_owner = '', lease_expires_at = NULL, last_error = '', updated_at = %s
                    FROM england_dnb_segments AS d
                    WHERE t.pipeline = %s
                      AND t.task_type = 'dnb_list_segment'
                      AND t.entity_id = d.segment_id
                      AND d.task_status = 'done'
                      AND t.status <> 'done'
                    """,
                    (now, DNB_PIPELINE),
                )
                affected += cur.rowcount
                cur.execute(
                    """
                    UPDATE england_cluster_tasks AS t
                    SET status = 'done', lease_owner = '', lease_expires_at = NULL, last_error = '', updated_at = %s
                    FROM england_dnb_companies AS d
                    WHERE t.pipeline = %s
                      AND t.task_type = 'dnb_detail'
                      AND t.entity_id = d.duns
                      AND d.detail_done = TRUE
                      AND t.status <> 'done'
                    """,
                    (now, DNB_PIPELINE),
                )
                affected += cur.rowcount
                for task_type, column in (
                    ("dnb_gmap", "gmap_task_status"),
                    ("dnb_firecrawl", "firecrawl_task_status"),
                ):
                    cur.execute(
                        f"""
                        UPDATE england_cluster_tasks AS t
                        SET status = 'done', lease_owner = '', lease_expires_at = NULL, last_error = '', updated_at = %s
                        FROM england_dnb_companies AS d
                        WHERE t.pipeline = %s
                          AND t.task_type = %s
                          AND t.entity_id = d.duns
                          AND d.{column} = 'done'
                          AND t.status <> 'done'
                        """,
                        (now, DNB_PIPELINE, task_type),
                    )
                    affected += cur.rowcount
        return affected

    def _get_pipeline_task_counts(self, pipeline: str) -> dict[str, int]:
        counts = {"pending": 0, "leased": 0, "failed": 0, "done": 0}
        with self._db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM england_cluster_tasks
                    WHERE pipeline = %s
                    GROUP BY status
                    """,
                    (pipeline,),
                )
                for row in cur.fetchall():
                    status = str(row["status"] or "").strip().lower()
                    if status in counts:
                        counts[status] = int(row["count"] or 0)
        return counts

    def _has_dnb_history(self) -> bool:
        with self._db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM england_dnb_discovery_nodes
                        UNION ALL
                        SELECT 1 FROM england_dnb_segments
                        UNION ALL
                        SELECT 1 FROM england_dnb_companies
                    ) AS has_rows
                    """
                )
                row = cur.fetchone()
        return bool(row and row["has_rows"])

    def _is_companies_house_source_loaded(self, input_xlsx: Path, *, max_companies: int = 0) -> bool:
        source_path = Path(input_xlsx).resolve()
        scope = _source_scope(max_companies)
        fingerprint = _source_fingerprint(source_path)
        with self._db.connect() as conn:
            with conn.cursor() as cur:
                return self._source_is_loaded_locked(
                    cur,
                    source_path=source_path,
                    fingerprint=fingerprint,
                    scope=scope,
                )

    def _source_is_loaded_locked(
        self,
        cur,
        *,
        source_path: Path,
        fingerprint: str,
        scope: str,
    ) -> bool:
        cur.execute(
            "SELECT fingerprint FROM england_ch_source_files WHERE source_path = %s",
            (_source_key(source_path, scope),),
        )
        row = cur.fetchone()
        return row is not None and str(row["fingerprint"]) == fingerprint

    def _requeue_failed_dnb_task_locked(self, cur, *, task_type: str, entity_id: str) -> None:
        if task_type == "dnb_discovery":
            cur.execute(
                """
                UPDATE england_dnb_discovery_nodes
                SET task_status = 'pending', task_retries = 0, updated_at = %s
                WHERE segment_id = %s
                """,
                (_utc_now(), entity_id),
            )
            return
        if task_type == "dnb_list_segment":
            cur.execute(
                """
                UPDATE england_dnb_segments
                SET task_status = 'pending', task_retries = 0, updated_at = %s
                WHERE segment_id = %s
                """,
                (_utc_now(), entity_id),
            )
            return
        self._update_dnb_task_state_locked(
            cur,
            duns=entity_id,
            task_type=task_type,
            status="pending",
            retries=0,
        )

    def _requeue_failed_ch_task_locked(self, cur, *, task_type: str, entity_id: str) -> None:
        self._update_ch_task_state_locked(
            cur,
            comp_id=entity_id,
            task_type=task_type,
            status="pending",
            retries=0,
        )

    def _mark_source_loaded_locked(
        self,
        cur,
        *,
        source_path: Path,
        fingerprint: str,
        total_rows: int,
        scope: str,
    ) -> None:
        cur.execute(
            """
            INSERT INTO england_ch_source_files(source_path, fingerprint, total_rows, updated_at)
            VALUES(%s, %s, %s, %s)
            ON CONFLICT(source_path) DO UPDATE SET
                fingerprint = EXCLUDED.fingerprint,
                total_rows = EXCLUDED.total_rows,
                updated_at = EXCLUDED.updated_at
            """,
            (
                _source_key(source_path, scope),
                fingerprint,
                max(int(total_rows), 0),
                _utc_now(),
            ),
        )

    def _select_claimable_task(self, cur) -> dict[str, Any] | None:
        cur.execute(
            """
            SELECT task_id, pipeline, task_type, entity_id, retries, payload_json
            FROM england_cluster_tasks
            WHERE status = 'pending' AND next_run_at <= %s
            ORDER BY next_run_at ASC, updated_at ASC, task_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """,
            (_utc_now(),),
        )
        return cur.fetchone()

    def _load_leased_task(self, cur, task_id: str, worker_id: str) -> dict[str, Any] | None:
        cur.execute(
            """
            SELECT task_id, pipeline, task_type, entity_id, retries, payload_json
            FROM england_cluster_tasks
            WHERE task_id = %s AND status = 'leased' AND lease_owner = %s
            FOR UPDATE
            """,
            (task_id, worker_id),
        )
        return cur.fetchone()

    def _is_stale_task_callback_locked(self, cur, task_id: str, worker_id: str) -> bool:
        """允许重复回写在任务已结束或已回到待处理时静默返回。"""
        cur.execute(
            """
            SELECT status, lease_owner
            FROM england_cluster_tasks
            WHERE task_id = %s
            FOR UPDATE
            """,
            (task_id,),
        )
        row = cur.fetchone()
        if row is None:
            return True
        status = str(row["status"] or "").strip().lower()
        if status != "leased":
            return True
        return False

    def _task_retry_limit(self, task_type: str) -> int:
        policy = self._config.retry_policy
        return {
            "dnb_discovery": 6,
            "dnb_list_segment": 6,
            "dnb_detail": policy.dnb_detail_max_retries,
            "dnb_gmap": policy.dnb_gmap_max_retries,
            "dnb_firecrawl": policy.dnb_firecrawl_max_retries,
            "ch_lookup": policy.ch_lookup_max_retries,
            "ch_gmap": policy.ch_gmap_max_retries,
            "ch_firecrawl": policy.ch_firecrawl_max_retries,
        }.get(task_type, 5)

    def _task_already_done_locked(self, cur, task: ClaimedTask) -> bool:
        if task.task_type == "dnb_discovery":
            cur.execute("SELECT task_status FROM england_dnb_discovery_nodes WHERE segment_id = %s", (task.entity_id,))
            row = cur.fetchone()
            return row is not None and str(row["task_status"] or "").strip() == "done"
        if task.task_type == "dnb_list_segment":
            cur.execute("SELECT task_status FROM england_dnb_segments WHERE segment_id = %s", (task.entity_id,))
            row = cur.fetchone()
            return row is not None and str(row["task_status"] or "").strip() == "done"
        if task.task_type == "dnb_detail":
            cur.execute("SELECT detail_done FROM england_dnb_companies WHERE duns = %s", (task.entity_id,))
            row = cur.fetchone()
            return row is not None and bool(row["detail_done"])
        if task.task_type in {"dnb_gmap", "dnb_firecrawl"}:
            column = "gmap_task_status" if task.task_type == "dnb_gmap" else "firecrawl_task_status"
            cur.execute(f"SELECT {column} FROM england_dnb_companies WHERE duns = %s", (task.entity_id,))
            row = cur.fetchone()
            return row is not None and str(row[column] or "").strip() == "done"
        if task.task_type in {"ch_lookup", "ch_gmap", "ch_firecrawl"}:
            column = {
                "ch_lookup": "ch_task_status",
                "ch_gmap": "gmap_task_status",
                "ch_firecrawl": "firecrawl_task_status",
            }[task.task_type]
            cur.execute(f"SELECT {column} FROM england_ch_companies WHERE comp_id = %s", (task.entity_id,))
            row = cur.fetchone()
            return row is not None and str(row[column] or "").strip() == "done"
        return False

    def _prepare_firecrawl_domain_for_task(self, cur, task: ClaimedTask, worker_id: str) -> str:
        now = _utc_now()
        domain = str(task.payload.get("domain", "")).strip().lower() or extract_domain(str(task.payload.get("homepage", "")).strip())
        if not domain:
            self._mark_entity_firecrawl_failed_locked(cur, task, "缺少域名")
            self._mark_task_done_locked(cur, task.task_id)
            return "done"
        task.payload["domain"] = domain
        cur.execute("SELECT * FROM england_firecrawl_domain_cache WHERE domain = %s FOR UPDATE", (domain,))
        row = cur.fetchone()
        if row is not None and str(row["status"]) == "done":
            self._apply_firecrawl_done_locked(cur, task, _parse_json_list(row["emails_json"]))
            self._mark_task_done_locked(cur, task.task_id)
            return "done"
        if row is not None and str(row["status"]) == "running":
            if row["lease_expires_at"] is None or row["lease_expires_at"] > now:
                self._reschedule_task_locked(cur, task.task_id, retries=task.retries, delay_seconds=FIRECRAWL_WAIT_SECONDS, error_text="等待同域名查询完成", payload=task.payload)
                return "wait"
        if row is not None and str(row["status"]) == "pending":
            next_retry_at = row["next_retry_at"]
            if next_retry_at is not None and next_retry_at > now:
                wait_seconds = max((next_retry_at - now).total_seconds(), FIRECRAWL_WAIT_SECONDS)
                self._reschedule_task_locked(cur, task.task_id, retries=task.retries, delay_seconds=wait_seconds, error_text="等待域名重试窗口", payload=task.payload)
                return "wait"
        cur.execute(
            """
            INSERT INTO england_firecrawl_domain_cache(domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at)
            VALUES(%s, 'running', '[]'::jsonb, NULL, %s, %s, '', %s)
            ON CONFLICT(domain) DO UPDATE SET
                status = 'running',
                lease_owner = EXCLUDED.lease_owner,
                lease_expires_at = EXCLUDED.lease_expires_at,
                next_retry_at = NULL,
                updated_at = EXCLUDED.updated_at,
                last_error = ''
            """,
            (domain, worker_id, _utc_after(self._config.task_lease_seconds), now),
        )
        cur.execute("UPDATE england_cluster_tasks SET payload_json = %s WHERE task_id = %s", (Jsonb(task.payload), task.task_id))
        return "claim"

    def _apply_task_completion(self, cur, task: ClaimedTask, result: dict[str, object]) -> str:
        handler = {
            "dnb_discovery": self._complete_dnb_discovery_locked,
            "dnb_list_segment": self._complete_dnb_list_segment_locked,
            "dnb_detail": self._complete_dnb_detail_locked,
            "dnb_gmap": self._complete_dnb_gmap_locked,
            "dnb_firecrawl": self._complete_dnb_firecrawl_locked,
            "ch_lookup": self._complete_ch_lookup_locked,
            "ch_gmap": self._complete_ch_gmap_locked,
            "ch_firecrawl": self._complete_ch_firecrawl_locked,
        }.get(task.task_type)
        if handler is None:
            raise RuntimeError(f"不支持的任务类型：{task.task_type}")
        return handler(cur, task, result)
