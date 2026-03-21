"""丹麦 Virk 配置。"""

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, "").strip() or default


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    normalized = raw.replace("\r", "\n").replace(";", ",")
    values: list[str] = []
    for chunk in normalized.split("\n"):
        for part in chunk.split(","):
            text = part.strip()
            if text and text not in values:
                values.append(text)
    return values


def _resolve_path(base: Path, raw: str, default: Path) -> Path:
    value = str(raw or "").strip()
    if not value:
        return default
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _has_firecrawl_keys(inline_keys: list[str] | None, keys_file: Path | None) -> bool:
    if inline_keys:
        return True
    if keys_file is None or not keys_file.exists():
        return False
    return bool(keys_file.read_text(encoding="utf-8").strip())


@dataclass(slots=True)
class VirkDenmarkConfig:
    project_root: Path = Path(".")
    output_dir: Path = Path(".")
    store_db_path: Path = Path("store.db")
    max_companies: int = 0
    page_size: int = 100
    search_workers: int = 1
    detail_workers: int = 4
    gmap_workers: int = 32
    firecrawl_workers: int = 96
    queue_poll_interval: float = 2.0
    stale_running_requeue_seconds: int = 600
    detail_task_max_retries: int = 5
    gmap_max_retries: int = 3
    firecrawl_task_max_retries: int = 5
    retry_backoff_cap_seconds: float = 180.0
    virk_base_url: str = "https://datacvr.virk.dk"
    virk_timeout_seconds: float = 30.0
    virk_proxy_url: str = "socks5h://127.0.0.1:7897"
    virk_min_interval_seconds: float = 0.8
    virk_rate_limit_retry_seconds: float = 20.0
    firecrawl_keys_inline: list[str] | None = None
    firecrawl_keys_file: Path | None = None
    firecrawl_pool_db: Path | None = None
    firecrawl_base_url: str = "https://api.firecrawl.dev/v2/"
    firecrawl_timeout_seconds: float = 45.0
    firecrawl_max_retries: int = 2
    firecrawl_key_per_limit: int = 2
    firecrawl_key_wait_seconds: int = 20
    firecrawl_key_cooldown_seconds: int = 90
    firecrawl_key_failure_threshold: int = 5
    llm_api_key: str = ""
    llm_base_url: str = "https://api.gpteamservices.com/v1"
    llm_model: str = "gpt-5.1-codex-mini"
    llm_reasoning_effort: str = "medium"
    llm_timeout_seconds: float = 120.0
    firecrawl_prefilter_limit: int = 40
    firecrawl_llm_pick_count: int = 16
    firecrawl_extract_max_urls: int = 12
    firecrawl_zero_retry_seconds: float = 43200.0
    firecrawl_contact_form_retry_seconds: float = 259200.0

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        max_companies: int,
        search_workers: int,
        detail_workers: int,
        gmap_workers: int,
        firecrawl_workers: int,
    ) -> "VirkDenmarkConfig":
        output = output_dir.resolve()
        return cls(
            project_root=project_root.resolve(),
            output_dir=output,
            store_db_path=output / "store.db",
            max_companies=max(max_companies, 0),
            page_size=_env_int("VIRK_DENMARK_PAGE_SIZE", 100),
            search_workers=max(search_workers, 1),
            detail_workers=max(detail_workers, 1),
            gmap_workers=max(gmap_workers, 1),
            firecrawl_workers=max(firecrawl_workers, 1),
            queue_poll_interval=_env_float("VIRK_DENMARK_QUEUE_POLL_INTERVAL", 2.0),
            stale_running_requeue_seconds=_env_int("VIRK_DENMARK_STALE_RUNNING_REQUEUE_SECONDS", 600),
            detail_task_max_retries=_env_int("VIRK_DENMARK_DETAIL_TASK_MAX_RETRIES", 5),
            gmap_max_retries=_env_int("VIRK_DENMARK_GMAP_MAX_RETRIES", 3),
            firecrawl_task_max_retries=_env_int("VIRK_DENMARK_FIRECRAWL_TASK_MAX_RETRIES", 5),
            retry_backoff_cap_seconds=_env_float("VIRK_DENMARK_RETRY_BACKOFF_CAP_SECONDS", 180.0),
            virk_base_url=_env_str("VIRK_BASE_URL", "https://datacvr.virk.dk"),
            virk_timeout_seconds=_env_float("VIRK_TIMEOUT_SECONDS", 30.0),
            virk_proxy_url=_env_str("VIRK_PROXY_URL", "socks5h://127.0.0.1:7897"),
            virk_min_interval_seconds=_env_float("VIRK_MIN_INTERVAL_SECONDS", 0.8),
            virk_rate_limit_retry_seconds=_env_float("VIRK_RATE_LIMIT_RETRY_SECONDS", 20.0),
            firecrawl_keys_inline=_env_list("FIRECRAWL_KEYS"),
            firecrawl_keys_file=_resolve_path(
                project_root,
                os.getenv("FIRECRAWL_KEYS_FILE", "").strip(),
                project_root / "output" / "firecrawl_keys.txt",
            ),
            firecrawl_pool_db=_resolve_path(
                project_root,
                os.getenv("FIRECRAWL_KEY_POOL_DB", "").strip(),
                project_root / "output" / "cache" / "firecrawl_keys.db",
            ),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev/v2/"),
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 45.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 2),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 2),
            firecrawl_key_wait_seconds=_env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20),
            firecrawl_key_cooldown_seconds=_env_int("FIRECRAWL_KEY_COOLDOWN_SECONDS", 90),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            llm_api_key=_env_str("LLM_API_KEY"),
            llm_base_url=_env_str("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
            llm_model=_env_str("LLM_MODEL", "gpt-5.1-codex-mini"),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", "medium"),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
            firecrawl_prefilter_limit=_env_int("FIRECRAWL_PREFILTER_LIMIT", 40),
            firecrawl_llm_pick_count=_env_int("FIRECRAWL_LLM_PICK_COUNT", 16),
            firecrawl_extract_max_urls=_env_int("FIRECRAWL_EXTRACT_MAX_URLS", 12),
            firecrawl_zero_retry_seconds=_env_float("FIRECRAWL_ZERO_RETRY_SECONDS", 43200.0),
            firecrawl_contact_form_retry_seconds=_env_float("FIRECRAWL_CONTACT_FORM_RETRY_SECONDS", 259200.0),
        )

    def validate(self, *, skip_firecrawl: bool = False) -> None:
        if not skip_firecrawl:
            if not _has_firecrawl_keys(self.firecrawl_keys_inline, self.firecrawl_keys_file):
                raise RuntimeError("Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。")
            if not self.llm_api_key or not self.llm_model:
                raise RuntimeError("Firecrawl 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。")
