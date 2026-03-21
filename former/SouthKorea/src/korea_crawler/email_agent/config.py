"""邮箱补全代理配置。"""

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


@dataclass(slots=True)
class EmailAgentConfig:
    firecrawl_base_url: str
    firecrawl_keys_file: Path
    firecrawl_pool_db: Path
    firecrawl_timeout_seconds: float
    firecrawl_max_retries: int
    firecrawl_key_per_limit: int
    firecrawl_key_wait_seconds: int
    firecrawl_key_cooldown_seconds: int
    firecrawl_key_failure_threshold: int
    map_include_subdomains: bool
    map_limit: int
    max_rounds: int
    pick_per_round: int
    retry_backoff_seconds: float
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_reasoning_effort: str
    llm_timeout_seconds: float

    @classmethod
    def from_env(cls, project_root: Path) -> "EmailAgentConfig":
        firecrawl_keys_file = Path(
            os.getenv("FIRECRAWL_KEYS_FILE", "output/firecrawl_keys.txt")
        )
        if not firecrawl_keys_file.is_absolute():
            firecrawl_keys_file = project_root / firecrawl_keys_file
        firecrawl_pool_db = Path(
            os.getenv("FIRECRAWL_KEY_POOL_DB", "output/cache/firecrawl_keys.db")
        )
        if not firecrawl_pool_db.is_absolute():
            firecrawl_pool_db = project_root / firecrawl_pool_db
        return cls(
            firecrawl_base_url=os.getenv("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev/v2/").strip(),
            firecrawl_keys_file=firecrawl_keys_file,
            firecrawl_pool_db=firecrawl_pool_db,
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 45.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 2),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 2),
            firecrawl_key_wait_seconds=_env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20),
            firecrawl_key_cooldown_seconds=_env_int("FIRECRAWL_KEY_COOLDOWN_SECONDS", 90),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            map_include_subdomains=(
                os.getenv("EMAIL_AGENT_MAP_INCLUDE_SUBDOMAINS", "true").strip().lower()
                in {"1", "true", "yes", "y"}
            ),
            map_limit=_env_int("EMAIL_AGENT_MAP_LIMIT", 200),
            max_rounds=_env_int("EMAIL_AGENT_MAX_ROUNDS", 2),
            pick_per_round=_env_int("EMAIL_AGENT_PICK_PER_ROUND", 3),
            retry_backoff_seconds=_env_float("EMAIL_AGENT_RETRY_BACKOFF_SECONDS", 30.0),
            llm_api_key=os.getenv("LLM_API_KEY", "").strip(),
            llm_base_url=os.getenv("LLM_BASE_URL", "").strip(),
            llm_model=os.getenv("LLM_MODEL", "").strip(),
            llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", "medium").strip(),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
        )
