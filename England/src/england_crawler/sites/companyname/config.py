"""CompanyName 配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_API_STYLE
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_BASE_URL
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_MODEL
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_REASONING_EFFORT


def _env_str(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key, "").strip()
    return int(raw) if raw else default


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key, "").strip()
    return float(raw) if raw else default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes")


@dataclass
class CompanyNameConfig:
    """CompanyName 管线配置。"""

    project_root: Path = field(default_factory=lambda: Path.cwd())
    output_dir: Path = field(default_factory=lambda: Path.cwd() / "output" / "companyname")
    store_db_path: Path = field(default_factory=lambda: Path.cwd() / "output" / "companyname" / "store.db")
    excel_files: list[Path] = field(default_factory=list)

    # GMap
    gmap_workers: int = 128
    gmap_proxy: str = ""

    # 官网邮箱只走规则提取；内部字段名保留 firecrawl_ 前缀以兼容数据库
    firecrawl_workers: int = 128
    firecrawl_keys_inline: list[str] = field(default_factory=list)
    firecrawl_keys_file: str = ""
    firecrawl_pool_db: str = ""
    firecrawl_base_url: str = "https://api.firecrawl.dev"
    firecrawl_timeout_seconds: float = 60.0
    firecrawl_max_retries: int = 3
    firecrawl_key_per_limit: int = 500
    firecrawl_key_wait_seconds: float = 60.0
    firecrawl_key_cooldown_seconds: float = 86400.0
    firecrawl_key_failure_threshold: int = 5
    firecrawl_prefilter_limit: int = 40
    firecrawl_llm_pick_count: int = 10
    firecrawl_extract_max_urls: int = 5
    firecrawl_zero_retry_seconds: float = 43200.0
    firecrawl_contact_form_retry_seconds: float = 259200.0

    # LLM
    llm_api_key: str = ""
    llm_base_url: str = DEFAULT_LLM_BASE_URL
    llm_model: str = DEFAULT_LLM_MODEL
    llm_reasoning_effort: str = DEFAULT_LLM_REASONING_EFFORT
    llm_api_style: str = DEFAULT_LLM_API_STYLE
    llm_timeout_seconds: float = 120.0

    # 协议爬虫
    crawl_backend: str = "protocol"

    # 运行时
    queue_poll_interval: float = 0.5
    stale_running_requeue_seconds: float = 300.0
    log_interval_seconds: float = 10.0
    snapshot_interval_seconds: float = 30.0

    def validate(self, *, skip_firecrawl: bool = False) -> None:
        # England 官网邮箱只走规则，代表人来自 Companies House，不强依赖 LLM 配置。
        _ = skip_firecrawl
        return None

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        excel_files: list[Path] | None = None,
        gmap_workers: int = 128,
        firecrawl_workers: int = 128,
    ) -> CompanyNameConfig:
        fc_keys_raw = _env_str("FIRECRAWL_API_KEYS")
        fc_keys = [k.strip() for k in fc_keys_raw.split(",") if k.strip()] if fc_keys_raw else []
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            store_db_path=output_dir / "store.db",
            excel_files=excel_files or [],
            gmap_workers=gmap_workers,
            gmap_proxy=_env_str("GOOGLE_MAPS_PROXY_URL", "socks5h://127.0.0.1:7897"),
            firecrawl_workers=firecrawl_workers,
            firecrawl_keys_inline=fc_keys,
            firecrawl_keys_file=_env_str("FIRECRAWL_KEYS_FILE"),
            firecrawl_pool_db=_env_str("FIRECRAWL_POOL_DB"),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev"),
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 60.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 3),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 500),
            firecrawl_key_wait_seconds=_env_float("FIRECRAWL_KEY_WAIT_SECONDS", 60.0),
            firecrawl_key_cooldown_seconds=_env_float("FIRECRAWL_KEY_COOLDOWN_SECONDS", 86400.0),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            firecrawl_prefilter_limit=_env_int("FIRECRAWL_PREFILTER_LIMIT", 12),
            firecrawl_llm_pick_count=_env_int("FIRECRAWL_LLM_PICK_COUNT", 5),
            firecrawl_extract_max_urls=_env_int("FIRECRAWL_EXTRACT_MAX_URLS", 5),
            firecrawl_zero_retry_seconds=_env_float("FIRECRAWL_ZERO_RETRY_SECONDS", 43200.0),
            firecrawl_contact_form_retry_seconds=_env_float("FIRECRAWL_CONTACT_FORM_RETRY_SECONDS", 259200.0),
            llm_api_key=_env_str("LLM_API_KEY"),
            llm_base_url=_env_str("LLM_BASE_URL", DEFAULT_LLM_BASE_URL),
            llm_model=_env_str("LLM_MODEL", DEFAULT_LLM_MODEL),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT),
            llm_api_style=_env_str("LLM_API_STYLE", DEFAULT_LLM_API_STYLE),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
            crawl_backend=_env_str("CRAWL_BACKEND", "protocol"),
        )
