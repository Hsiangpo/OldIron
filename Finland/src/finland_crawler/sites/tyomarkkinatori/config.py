"""Työmarkkinatori 配置。"""

from __future__ import annotations

import os
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


@dataclass
class TmtConfig:
    """Työmarkkinatori 爬虫配置。"""

    project_root: Path
    output_dir: Path
    store_db_path: Path

    # 网络
    proxy_url: str = ""
    timeout_seconds: float = 30.0

    # 并发
    search_workers: int = 2
    detail_workers: int = 8
    gmap_workers: int = 64
    firecrawl_workers: int = 8

    # 搜索分页（API 最大 pageSize=90）
    search_page_size: int = 90
    search_max_pages: int = 200   # 100 × 200 = 最多 20000 条

    # 重试
    max_task_retries: int = 3
    retry_backoff_cap_seconds: float = 30.0
    stale_running_requeue_seconds: float = 300.0

    # 队列
    queue_poll_interval: float = 1.0

    # GMap
    gmap_base_url: str = ""
    gmap_max_retries: int = 3

    # Firecrawl / Protocol+LLM
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

    # LLM
    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_model: str = ""
    llm_reasoning_effort: str = ""
    llm_timeout_seconds: float = 120.0

    # 爬虫后端
    crawl_backend: str = "firecrawl"

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        search_workers: int = 2,
        detail_workers: int = 8,
        gmap_workers: int = 64,
        firecrawl_workers: int = 8,
    ) -> TmtConfig:
        """从环境变量创建配置。"""
        store_db = output_dir / "tmt_store.db"
        proxy = _env_str("TMT_PROXY_URL", _env_str("PROXY_URL", "http://127.0.0.1:7897"))
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            store_db_path=store_db,
            proxy_url=proxy,
            timeout_seconds=_env_float("TMT_TIMEOUT", 30.0),
            search_workers=_env_int("TMT_SEARCH_WORKERS", search_workers),
            detail_workers=_env_int("TMT_DETAIL_WORKERS", detail_workers),
            gmap_workers=_env_int("TMT_GMAP_WORKERS", gmap_workers),
            firecrawl_workers=_env_int("TMT_FIRECRAWL_WORKERS", firecrawl_workers),
            search_page_size=_env_int("TMT_PAGE_SIZE", 90),
            search_max_pages=_env_int("TMT_MAX_PAGES", 200),
            gmap_base_url=_env_str("GMAP_SERVICE_ADDR", "http://127.0.0.1:8082"),
            firecrawl_keys_file=_env_str("FIRECRAWL_KEYS_FILE", ""),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", ""),
            firecrawl_pool_db=_env_str("FIRECRAWL_POOL_DB", ""),
            llm_api_key=_env_str("LLM_API_KEY", ""),
            llm_base_url=_env_str("LLM_BASE_URL", ""),
            llm_model=_env_str("LLM_MODEL", ""),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", ""),
            crawl_backend=_env_str("CRAWL_BACKEND", "firecrawl"),
            firecrawl_task_max_retries=_env_int("TMT_FIRECRAWL_TASK_MAX_RETRIES", 5),
        )

    def validate(self) -> None:
        """校验必要配置。"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
