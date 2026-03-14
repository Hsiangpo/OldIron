"""Firecrawl 运行时配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_LLM_BASE_URL = "https://api.gpteamservices.com/v1"
_DEFAULT_LLM_MODEL = "gpt-5.1-codex-mini"
_DEFAULT_LLM_REASONING = "medium"
_DEFAULT_FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v2/"


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


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    normalized = raw.replace("\r", "\n").replace(";", ",")
    items: list[str] = []
    for chunk in normalized.split("\n"):
        for part in chunk.split(","):
            value = part.strip()
            if value and value not in items:
                items.append(value)
    return items


def _resolve_path(base: Path, raw: str, default: Path) -> Path:
    text = str(raw or "").strip()
    if not text:
        return default.resolve()
    path = Path(text)
    if path.is_absolute():
        return path
    return (base / path).resolve()


@dataclass(slots=True)
class FirecrawlRuntimeConfig:
    project_root: Path
    output_dir: Path
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
    map_limit: int
    candidate_prelimit: int
    selected_url_limit: int

    @classmethod
    def from_env(cls, *, project_root: Path, output_dir: Path) -> "FirecrawlRuntimeConfig":
        output_dir = output_dir.resolve()
        return cls(
            project_root=project_root.resolve(),
            output_dir=output_dir,
            firecrawl_keys_inline=_env_list("FIRECRAWL_KEYS"),
            firecrawl_keys_file=_resolve_path(
                project_root,
                _env_str("FIRECRAWL_KEYS_FILE"),
                output_dir / "firecrawl_keys.txt",
            ),
            firecrawl_pool_db=_resolve_path(
                project_root,
                _env_str("FIRECRAWL_KEY_POOL_DB"),
                output_dir / "cache" / "firecrawl_keys.db",
            ),
            firecrawl_base_url=_env_str("FIRECRAWL_BASE_URL", _DEFAULT_FIRECRAWL_BASE_URL),
            firecrawl_timeout_seconds=_env_float("FIRECRAWL_TIMEOUT_SECONDS", 45.0),
            firecrawl_max_retries=_env_int("FIRECRAWL_MAX_RETRIES", 2),
            firecrawl_key_per_limit=_env_int("FIRECRAWL_KEY_PER_LIMIT", 2),
            firecrawl_key_wait_seconds=_env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20),
            firecrawl_key_cooldown_seconds=_env_int("FIRECRAWL_KEY_COOLDOWN_SECONDS", 90),
            firecrawl_key_failure_threshold=_env_int("FIRECRAWL_KEY_FAILURE_THRESHOLD", 5),
            llm_api_key=_env_str("LLM_API_KEY"),
            llm_base_url=_env_str("LLM_BASE_URL", _DEFAULT_LLM_BASE_URL),
            llm_model=_env_str("LLM_MODEL", _DEFAULT_LLM_MODEL),
            llm_reasoning_effort=_env_str("LLM_REASONING_EFFORT", _DEFAULT_LLM_REASONING),
            llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
            map_limit=_env_int("FIRECRAWL_EMAIL_MAP_LIMIT", 200),
            candidate_prelimit=_env_int("FIRECRAWL_EMAIL_CANDIDATE_PRELIMIT", 30),
            selected_url_limit=_env_int("FIRECRAWL_EMAIL_SELECTED_URL_LIMIT", 12),
        )

    def validate(self) -> None:
        if not self.firecrawl_keys_inline:
            raise RuntimeError("Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。")
        if not self.llm_api_key:
            raise RuntimeError("Firecrawl 阶段缺少 LLM_API_KEY，请检查根目录 .env。")
        if not self.llm_model:
            raise RuntimeError("Firecrawl 阶段缺少 LLM_MODEL，请检查根目录 .env。")
