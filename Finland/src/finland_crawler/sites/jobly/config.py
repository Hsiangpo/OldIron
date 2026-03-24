"""Jobly 配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(int(raw), 1)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(float(raw), 0.1)
    except ValueError:
        return default


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, "").strip() or default


@dataclass
class JoblyConfig:
    """Jobly 爬虫配置。"""

    project_root: Path
    output_dir: Path
    store_db_path: Path

    proxy_url: str = ""
    timeout_seconds: float = 30.0

    detail_workers: int = 8
    gmap_workers: int = 64
    firecrawl_workers: int = 8

    max_list_pages: int = 500
    request_delay: float = 1.0

    max_task_retries: int = 3
    retry_backoff_cap_seconds: float = 30.0
    stale_running_requeue_seconds: float = 300.0
    queue_poll_interval: float = 1.0

    gmap_base_url: str = ""
    gmap_max_retries: int = 3

    firecrawl_keys_file: str = ""
    firecrawl_base_url: str = ""
    firecrawl_pool_db: str = ""
    firecrawl_timeout_seconds: float = 60.0
    firecrawl_max_retries: int = 3
    firecrawl_key_per_limit: int = 500
    firecrawl_key_wait_seconds: float = 3.0
    firecrawl_key_cooldown_seconds: float = 3600.0
    firecrawl_key_failure_threshold: int = 5
    firecrawl_prefilter_limit: int = 30
    firecrawl_llm_pick_count: int = 5
    firecrawl_extract_max_urls: int = 5
    firecrawl_zero_retry_seconds: float = 0.0
    firecrawl_contact_form_retry_seconds: float = 0.0
    firecrawl_task_max_retries: int = 5

    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_model: str = ""
    llm_reasoning_effort: str = ""
    llm_timeout_seconds: float = 120.0

    crawl_backend: str = "firecrawl"

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        detail_workers: int = 8,
        gmap_workers: int = 64,
        firecrawl_workers: int = 8,
    ) -> JoblyConfig:
        store_db = output_dir / "jobly_store.db"
        proxy = _env_str("JOBLY_PROXY_URL", _env_str("PROXY_URL", "http://127.0.0.1:7897"))
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            store_db_path=store_db,
            proxy_url=proxy,
            timeout_seconds=_env_float("JOBLY_TIMEOUT", 30.0),
            detail_workers=_env_int("JOBLY_DETAIL_WORKERS", detail_workers),
            gmap_workers=_env_int("JOBLY_GMAP_WORKERS", gmap_workers),
            firecrawl_workers=_env_int("JOBLY_FIRECRAWL_WORKERS", firecrawl_workers),
            gmap_base_url=_env_str("GMAP_SERVICE_ADDR", "http://127.0.0.1:8082"),
            firecrawl_keys_file=_env_str("FIRECRAWL_KEYS_FILE", ""),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", ""),
            firecrawl_pool_db=_env_str("FIRECRAWL_POOL_DB", ""),
            llm_api_key=_env_str("LLM_API_KEY", ""),
            llm_base_url=_env_str("LLM_BASE_URL", ""),
            llm_model=_env_str("LLM_MODEL", ""),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", ""),
            crawl_backend=_env_str("CRAWL_BACKEND", "firecrawl"),
            firecrawl_task_max_retries=_env_int("JOBLY_FIRECRAWL_TASK_MAX_RETRIES", 5),
        )

    def validate(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
