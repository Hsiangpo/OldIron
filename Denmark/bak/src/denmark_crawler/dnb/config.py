"""丹麦 DNB 流式配置。"""

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
    pieces: list[str] = []
    for chunk in normalized.split("\n"):
        parts = [item.strip() for item in chunk.split(",")]
        pieces.extend([item for item in parts if item])
    unique: list[str] = []
    for item in pieces:
        if item not in unique:
            unique.append(item)
    return unique


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
    if keys_file is None:
        return False
    if not keys_file.exists():
        return False
    return bool(str(keys_file.read_text(encoding="utf-8")).strip())


@dataclass(slots=True)
class DnbDenmarkConfig:
    project_root: Path = Path(".")
    output_dir: Path = Path(".")
    store_db_path: Path = Path("store.db")
    snov_client_id: str = ""
    snov_client_secret: str = ""
    snov_timeout_seconds: float = 30.0
    snov_retry_delay_seconds: float = 10.0
    snov_max_retries: int = 5
    max_companies: int = 0
    dnb_pipeline_workers: int = 1
    dnb_workers: int = 1
    gmap_workers: int = 1
    snov_workers: int = 96
    queue_poll_interval: float = 2.0
    stale_running_requeue_seconds: int = 600
    gmap_max_retries: int = 3
    detail_task_max_retries: int = 8
    snov_task_max_retries: int = 5
    retry_backoff_cap_seconds: float = 180.0
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
    seed_file_path: Path | None = None

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        max_companies: int,
        dnb_pipeline_workers: int,
        dnb_workers: int,
        gmap_workers: int,
        snov_workers: int,
        seed_file_path: Path | None = None,
    ) -> "DnbDenmarkConfig":
        output_dir = output_dir.resolve()
        return cls(
            project_root=project_root.resolve(),
            output_dir=output_dir,
            store_db_path=output_dir / "store.db",
            snov_client_id=_env_str("SNOV_CLIENT_ID"),
            snov_client_secret=_env_str("SNOV_CLIENT_SECRET"),
            snov_timeout_seconds=_env_float("SNOV_TIMEOUT_SECONDS", 30.0),
            snov_retry_delay_seconds=_env_float("SNOV_RETRY_DELAY_SECONDS", 10.0),
            snov_max_retries=_env_int("SNOV_MAX_RETRIES", 5),
            max_companies=max(max_companies, 0),
            dnb_pipeline_workers=max(dnb_pipeline_workers, 1),
            dnb_workers=max(dnb_workers, 1),
            gmap_workers=max(gmap_workers, 1),
            snov_workers=max(snov_workers, 1),
            queue_poll_interval=_env_float("DNB_DENMARK_QUEUE_POLL_INTERVAL", 2.0),
            stale_running_requeue_seconds=_env_int(
                "DNB_DENMARK_STALE_RUNNING_REQUEUE_SECONDS",
                600,
            ),
            gmap_max_retries=_env_int("DNB_DENMARK_GMAP_MAX_RETRIES", 3),
            detail_task_max_retries=_env_int("DNB_DENMARK_DETAIL_TASK_MAX_RETRIES", 8),
            snov_task_max_retries=_env_int("DNB_DENMARK_FIRECRAWL_TASK_MAX_RETRIES", 5),
            retry_backoff_cap_seconds=_env_float(
                "DNB_DENMARK_RETRY_BACKOFF_CAP_SECONDS",
                180.0,
            ),
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
            seed_file_path=seed_file_path.resolve() if seed_file_path else None,
        )

    def validate(self, *, skip_firecrawl: bool = False, skip_snov: bool | None = None) -> None:
        if skip_snov is not None:
            skip_firecrawl = skip_snov
        if self.seed_file_path and not self.seed_file_path.exists():
            raise RuntimeError(f"未找到 DNB seed 文件: {self.seed_file_path}")
        if not skip_firecrawl:
            if not _has_firecrawl_keys(self.firecrawl_keys_inline, self.firecrawl_keys_file):
                raise RuntimeError("Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。")
            if not self.llm_api_key or not self.llm_model:
                raise RuntimeError("Firecrawl 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。")

