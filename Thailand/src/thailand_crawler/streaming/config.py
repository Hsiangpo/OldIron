"""流式主流程配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip()
    return values


def _env_int(name: str, default: int, fallback: dict[str, str] | None = None) -> int:
    raw = os.getenv(name, '').strip() or str((fallback or {}).get(name, '')).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_float(name: str, default: float, fallback: dict[str, str] | None = None) -> float:
    raw = os.getenv(name, '').strip() or str((fallback or {}).get(name, '')).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_str(name: str, default: str = '', fallback: dict[str, str] | None = None) -> str:
    return os.getenv(name, '').strip() or str((fallback or {}).get(name, '')).strip() or default


def _env_list(name: str, fallback: dict[str, str] | None = None) -> list[str]:
    raw = os.getenv(name, '').strip() or str((fallback or {}).get(name, '')).strip()
    if not raw:
        return []
    normalized = raw.replace('\r', '\n').replace(';', ',')
    pieces: list[str] = []
    for chunk in normalized.split('\n'):
        parts = [item.strip() for item in chunk.split(',')]
        pieces.extend([item for item in parts if item])
    unique: list[str] = []
    for item in pieces:
        if item not in unique:
            unique.append(item)
    return unique


def _resolve_path(base: Path, raw: str, default: Path) -> Path:
    value = str(raw or '').strip()
    if not value:
        return default
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()


@dataclass(slots=True)
class StreamPipelineConfig:
    project_root: Path
    output_dir: Path
    store_db_path: Path
    firecrawl_keys_inline: list[str]
    firecrawl_keys_file: Path
    firecrawl_pool_db: Path
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
    snov_client_id: str
    snov_client_secret: str
    snov_timeout_seconds: float
    snov_retry_delay_seconds: float
    snov_max_retries: int
    max_companies: int
    dnb_workers: int
    website_workers: int
    site_workers: int
    snov_workers: int
    queue_poll_interval: float
    stale_running_requeue_seconds: int
    website_max_retries: int
    site_max_retries: int
    snov_task_max_retries: int
    retry_backoff_cap_seconds: float

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        max_companies: int,
        dnb_workers: int,
        website_workers: int,
        site_workers: int,
        snov_workers: int,
    ) -> 'StreamPipelineConfig':
        output_dir = output_dir.resolve()
        store_db_path = output_dir / 'store.db'
        firecrawl_keys_file = _resolve_path(project_root, os.getenv('FIRECRAWL_KEYS_FILE', '').strip(), output_dir / 'firecrawl_keys.txt')
        firecrawl_pool_db = _resolve_path(project_root, os.getenv('FIRECRAWL_KEY_POOL_DB', '').strip(), output_dir / 'cache' / 'firecrawl_keys.db')
        return cls(
            project_root=project_root.resolve(),
            output_dir=output_dir,
            store_db_path=store_db_path,
            firecrawl_keys_inline=_env_list('FIRECRAWL_KEYS'),
            firecrawl_keys_file=firecrawl_keys_file,
            firecrawl_pool_db=firecrawl_pool_db,
            firecrawl_base_url=_env_str('FIRECRAWL_BASE_URL', 'https://api.firecrawl.dev/v2/'),
            firecrawl_timeout_seconds=_env_float('FIRECRAWL_TIMEOUT_SECONDS', 45.0),
            firecrawl_max_retries=_env_int('FIRECRAWL_MAX_RETRIES', 2),
            firecrawl_key_per_limit=_env_int('FIRECRAWL_KEY_PER_LIMIT', 2),
            firecrawl_key_wait_seconds=_env_int('FIRECRAWL_KEY_WAIT_SECONDS', 20),
            firecrawl_key_cooldown_seconds=_env_int('FIRECRAWL_KEY_COOLDOWN_SECONDS', 90),
            firecrawl_key_failure_threshold=_env_int('FIRECRAWL_KEY_FAILURE_THRESHOLD', 5),
            llm_api_key=_env_str('LLM_API_KEY'),
            llm_base_url=_env_str('LLM_BASE_URL'),
            llm_model=_env_str('LLM_MODEL'),
            llm_reasoning_effort=_env_str('LLM_REASONING_EFFORT', 'medium'),
            llm_timeout_seconds=_env_float('LLM_TIMEOUT_SECONDS', 120.0),
            snov_client_id=_env_str('SNOV_CLIENT_ID'),
            snov_client_secret=_env_str('SNOV_CLIENT_SECRET'),
            snov_timeout_seconds=_env_float('SNOV_TIMEOUT_SECONDS', 30.0),
            snov_retry_delay_seconds=_env_float('SNOV_RETRY_DELAY_SECONDS', 10.0),
            snov_max_retries=_env_int('SNOV_MAX_RETRIES', 5),
            max_companies=max(max_companies, 0),
            dnb_workers=max(dnb_workers, 1),
            website_workers=max(website_workers, 1),
            site_workers=max(site_workers, 1),
            snov_workers=max(snov_workers, 1),
            queue_poll_interval=_env_float('STREAM_QUEUE_POLL_INTERVAL', 2.0),
            stale_running_requeue_seconds=_env_int('STREAM_STALE_RUNNING_REQUEUE_SECONDS', 600),
            website_max_retries=_env_int('STREAM_WEBSITE_MAX_RETRIES', 3),
            site_max_retries=_env_int('STREAM_SITE_MAX_RETRIES', 5),
            snov_task_max_retries=_env_int('STREAM_SNOV_TASK_MAX_RETRIES', 5),
            retry_backoff_cap_seconds=_env_float('STREAM_RETRY_BACKOFF_CAP_SECONDS', 180.0),
        )

    def validate(self, *, skip_site: bool, skip_snov: bool) -> None:
        if not skip_site and not self.firecrawl_keys_inline:
            raise RuntimeError('site 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。')
        if not skip_site and (not self.llm_api_key or not self.llm_model):
            raise RuntimeError('site 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。')
        if not skip_snov and (not self.snov_client_id or not self.snov_client_secret):
            raise RuntimeError('snov 阶段缺少 SNOV_CLIENT_ID / SNOV_CLIENT_SECRET。')
