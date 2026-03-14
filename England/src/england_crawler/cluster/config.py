"""England 集群模式配置。"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, "").strip() or default


def _default_worker_id() -> str:
    host = socket.gethostname().strip().lower() or "worker"
    suffix = uuid.uuid4().hex[:8]
    return f"{host}-{suffix}"


@dataclass(slots=True)
class ClusterRetryPolicy:
    dnb_detail_max_retries: int = 8
    dnb_gmap_max_retries: int = 3
    dnb_firecrawl_max_retries: int = 5
    ch_lookup_max_retries: int = 4
    ch_gmap_max_retries: int = 3
    ch_firecrawl_max_retries: int = 5
    retry_backoff_cap_seconds: float = 180.0


@dataclass(slots=True)
class ClusterConfig:
    project_root: Path
    output_root: Path
    postgres_dsn: str
    coordinator_host: str
    coordinator_port: int
    coordinator_base_url: str
    cluster_token: str
    worker_id: str
    worker_poll_seconds: float
    worker_heartbeat_seconds: float
    task_lease_seconds: int
    snapshot_export_interval_seconds: float
    firecrawl_key_per_limit: int
    firecrawl_key_cooldown_seconds: int
    firecrawl_key_wait_seconds: int
    firecrawl_key_failure_threshold: int
    firecrawl_timeout_seconds: float
    firecrawl_max_retries: int
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_reasoning_effort: str
    llm_timeout_seconds: float
    firecrawl_prefilter_limit: int
    firecrawl_llm_pick_count: int
    firecrawl_extract_max_urls: int
    retry_policy: ClusterRetryPolicy

    @classmethod
    def from_env(cls, project_root: Path) -> "ClusterConfig":
        project_root = project_root.resolve()
        output_root = project_root / "output"
        host = _env_str("ENGLAND_CLUSTER_HOST", "0.0.0.0")
        port = _env_int("ENGLAND_CLUSTER_PORT", 8787)
        base_url = _env_str("ENGLAND_CLUSTER_BASE_URL", f"http://127.0.0.1:{port}")
        return cls(
            project_root=project_root,
            output_root=output_root,
            postgres_dsn=_env_str(
                "ENGLAND_CLUSTER_POSTGRES_DSN",
                "postgresql://postgres:postgres@127.0.0.1:6543/oldiron_england",
            ),
            coordinator_host=host,
            coordinator_port=port,
            coordinator_base_url=base_url.rstrip("/"),
            cluster_token=_env_str("ENGLAND_CLUSTER_TOKEN"),
            worker_id=_env_str("ENGLAND_CLUSTER_WORKER_ID", _default_worker_id()),
            worker_poll_seconds=_env_float("ENGLAND_CLUSTER_WORKER_POLL_SECONDS", 2.0),
            worker_heartbeat_seconds=_env_float("ENGLAND_CLUSTER_HEARTBEAT_SECONDS", 10.0),
            task_lease_seconds=_env_int("ENGLAND_CLUSTER_TASK_LEASE_SECONDS", 90),
            snapshot_export_interval_seconds=_env_float(
                "ENGLAND_CLUSTER_EXPORT_INTERVAL_SECONDS",
                30.0,
            ),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 2),
            firecrawl_key_cooldown_seconds=_env_int("FIRECRAWL_KEY_COOLDOWN_SECONDS", 90),
            firecrawl_key_wait_seconds=_env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 45.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 2),
            llm_api_key=_env_str("LLM_API_KEY"),
            llm_base_url=_env_str("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
            llm_model=_env_str("LLM_MODEL", "gpt-5.1-codex-mini"),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", "medium"),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
            firecrawl_prefilter_limit=_env_int("FIRECRAWL_PREFILTER_LIMIT", 24),
            firecrawl_llm_pick_count=_env_int("FIRECRAWL_LLM_PICK_COUNT", 12),
            firecrawl_extract_max_urls=_env_int("FIRECRAWL_EXTRACT_MAX_URLS", 10),
            retry_policy=ClusterRetryPolicy(
                dnb_detail_max_retries=_env_int("DNB_ENGLAND_DETAIL_TASK_MAX_RETRIES", 8),
                dnb_gmap_max_retries=_env_int("DNB_ENGLAND_GMAP_MAX_RETRIES", 3),
                dnb_firecrawl_max_retries=_env_int("DNB_ENGLAND_FIRECRAWL_TASK_MAX_RETRIES", 5),
                ch_lookup_max_retries=_env_int("COMPANIES_HOUSE_MAX_RETRIES", 4),
                ch_gmap_max_retries=_env_int("COMPANIES_HOUSE_GMAP_MAX_RETRIES", 3),
                ch_firecrawl_max_retries=_env_int("COMPANIES_HOUSE_FIRECRAWL_TASK_MAX_RETRIES", 5),
                retry_backoff_cap_seconds=_env_float(
                    "ENGLAND_CLUSTER_RETRY_BACKOFF_CAP_SECONDS",
                    180.0,
                ),
            ),
        )

    def validate(self) -> None:
        if not self.postgres_dsn:
            raise RuntimeError("缺少 ENGLAND_CLUSTER_POSTGRES_DSN。")
        if not self.coordinator_base_url:
            raise RuntimeError("缺少 ENGLAND_CLUSTER_BASE_URL。")

    def validate_worker_runtime(self) -> None:
        self.validate()
        if not self.llm_api_key:
            raise RuntimeError("集群 worker 缺少 LLM_API_KEY。")
