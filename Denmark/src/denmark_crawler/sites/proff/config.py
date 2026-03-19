"""Proff 配置。"""

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


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


def _resolve_path(base: Path, raw: str) -> Path | None:
    value = str(raw or "").strip()
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _load_queries(query_file: Path | None, inline_queries: list[str]) -> list[str]:
    if inline_queries:
        return inline_queries
    if query_file is not None and query_file.exists():
        values: list[str] = []
        for raw in query_file.read_text(encoding="utf-8").splitlines():
            text = raw.strip()
            if text and text not in values:
                values.append(text)
        if values:
            return values
    return []


def _has_firecrawl_keys(inline_keys: list[str], keys_file: Path | None) -> bool:
    if inline_keys:
        return True
    if keys_file is None or not keys_file.exists():
        return False
    return bool(keys_file.read_text(encoding="utf-8").strip())


@dataclass(slots=True)
class ProffDenmarkConfig:
    """Proff 运行配置。"""

    project_root: Path
    output_dir: Path
    store_db_path: Path
    base_url: str
    timeout_seconds: float
    proxy_url: str
    min_interval_seconds: float
    queue_poll_interval: float
    stale_running_requeue_seconds: int
    max_task_retries: int
    retry_backoff_cap_seconds: float
    search_workers: int
    gmap_workers: int
    firecrawl_workers: int
    gmap_max_retries: int
    firecrawl_task_max_retries: int
    max_pages_per_query: int
    max_results_per_segment: int
    max_companies: int
    queries: list[str]
    firecrawl_keys_inline: list[str]
    firecrawl_keys_file: Path | None
    firecrawl_pool_db: Path | None
    firecrawl_base_url: str
    firecrawl_timeout_seconds: float
    firecrawl_max_retries: int
    firecrawl_key_per_limit: int
    firecrawl_key_wait_seconds: int
    firecrawl_key_cooldown_seconds: int
    firecrawl_key_failure_threshold: int
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_reasoning_effort: str
    llm_timeout_seconds: float
    firecrawl_prefilter_limit: int
    firecrawl_llm_pick_count: int
    firecrawl_extract_max_urls: int
    firecrawl_zero_retry_seconds: float
    firecrawl_contact_form_retry_seconds: float
    prefer_go_gmap_backend: bool
    gmap_service_url: str

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        query_file: Path | None,
        inline_queries: list[str],
        max_pages_per_query: int,
        max_companies: int,
        search_workers: int,
        gmap_workers: int,
        firecrawl_workers: int,
    ) -> "ProffDenmarkConfig":
        resolved_output = output_dir.resolve()
        return cls(
            project_root=project_root.resolve(),
            output_dir=resolved_output,
            store_db_path=resolved_output / "store.db",
            base_url=_env_str("PROFF_BASE_URL", "https://www.proff.dk/branches%C3%B8g"),
            timeout_seconds=_env_float("PROFF_TIMEOUT_SECONDS", 30.0),
            proxy_url=_env_str("PROFF_PROXY_URL", "http://127.0.0.1:7897"),
            min_interval_seconds=_env_float("PROFF_MIN_INTERVAL_SECONDS", 0.2),
            queue_poll_interval=_env_float("PROFF_QUEUE_POLL_INTERVAL", 1.0),
            stale_running_requeue_seconds=_env_int("PROFF_STALE_RUNNING_REQUEUE_SECONDS", 900),
            max_task_retries=_env_int("PROFF_MAX_TASK_RETRIES", 5),
            retry_backoff_cap_seconds=_env_float("PROFF_RETRY_BACKOFF_CAP_SECONDS", 120.0),
            search_workers=max(int(search_workers or 1), 1),
            gmap_workers=max(int(gmap_workers or 1), 1),
            firecrawl_workers=max(int(firecrawl_workers or 1), 1),
            gmap_max_retries=_env_int("PROFF_GMAP_MAX_RETRIES", 3),
            firecrawl_task_max_retries=_env_int("PROFF_FIRECRAWL_TASK_MAX_RETRIES", 5),
            max_pages_per_query=max(int(max_pages_per_query or 1), 1),
            max_results_per_segment=_env_int("PROFF_MAX_RESULTS_PER_SEGMENT", 10000),
            max_companies=max(int(max_companies or 0), 0),
            queries=_load_queries(query_file, inline_queries),
            firecrawl_keys_inline=_env_list("FIRECRAWL_KEYS"),
            firecrawl_keys_file=_resolve_path(
                project_root,
                os.getenv("FIRECRAWL_KEYS_FILE", "").strip(),
            ) or (project_root / "output" / "firecrawl_keys.txt"),
            firecrawl_pool_db=_resolve_path(
                project_root,
                os.getenv("FIRECRAWL_KEY_POOL_DB", "").strip(),
            ) or (project_root / "output" / "cache" / "firecrawl_keys.db"),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev/v2/"),
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 45.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 2),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 2),
            firecrawl_key_wait_seconds=_env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20),
            firecrawl_key_cooldown_seconds=_env_int("FIRECRAWL_KEY_COOLDOWN_SECONDS", 90),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            llm_api_key=_env_str("LLM_API_KEY"),
            llm_base_url=_env_str("LLM_BASE_URL", "https://api.gpteamservices.com/v1"),
            llm_model=_env_str("LLM_MODEL", "gpt-5.4-mini"),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", "medium"),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
            firecrawl_prefilter_limit=_env_int("FIRECRAWL_PREFILTER_LIMIT", 40),
            firecrawl_llm_pick_count=_env_int("FIRECRAWL_LLM_PICK_COUNT", 8),
            firecrawl_extract_max_urls=_env_int("FIRECRAWL_EXTRACT_MAX_URLS", 8),
            firecrawl_zero_retry_seconds=_env_float("FIRECRAWL_ZERO_RETRY_SECONDS", 43200.0),
            firecrawl_contact_form_retry_seconds=_env_float("FIRECRAWL_CONTACT_FORM_RETRY_SECONDS", 259200.0),
            prefer_go_gmap_backend=_env_bool("PROFF_USE_GO_GMAP_BACKEND", True),
            gmap_service_url=_env_str("GMAP_SERVICE_URL", "http://127.0.0.1:8082"),
        )

    def validate(self, *, skip_firecrawl: bool = False) -> None:
        if skip_firecrawl:
            return
        if not _has_firecrawl_keys(self.firecrawl_keys_inline, self.firecrawl_keys_file):
            raise RuntimeError("Proff Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查 .env。")
        if not self.llm_api_key or not self.llm_model:
            raise RuntimeError("Proff Firecrawl 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。")


def resolve_query_file(project_root: Path, raw_value: str) -> Path | None:
    """解析 query 文件路径。"""
    return _resolve_path(project_root, raw_value)
