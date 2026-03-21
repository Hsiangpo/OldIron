"""韩国 DNB 流式配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from korea_crawler.email_agent.config import EmailAgentConfig


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


@dataclass(slots=True)
class DnbKoreaConfig:
    project_root: Path
    output_dir: Path
    store_db_path: Path
    snov_client_id: str
    snov_client_secret: str
    snov_timeout_seconds: float
    snov_retry_delay_seconds: float
    snov_max_retries: int
    max_companies: int
    dnb_pipeline_workers: int
    dnb_workers: int
    gmap_workers: int
    site_workers: int
    snov_workers: int
    queue_poll_interval: float
    stale_running_requeue_seconds: int
    gmap_max_retries: int
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
        dnb_pipeline_workers: int,
        dnb_workers: int,
        gmap_workers: int,
        site_workers: int,
        snov_workers: int,
    ) -> "DnbKoreaConfig":
        return cls(
            project_root=project_root.resolve(),
            output_dir=output_dir.resolve(),
            store_db_path=output_dir.resolve() / "store.db",
            snov_client_id=_env_str("SNOV_CLIENT_ID"),
            snov_client_secret=_env_str("SNOV_CLIENT_SECRET"),
            snov_timeout_seconds=_env_float("SNOV_TIMEOUT_SECONDS", 30.0),
            snov_retry_delay_seconds=_env_float("SNOV_RETRY_DELAY_SECONDS", 10.0),
            snov_max_retries=_env_int("SNOV_MAX_RETRIES", 5),
            max_companies=max(max_companies, 0),
            dnb_pipeline_workers=max(dnb_pipeline_workers, 1),
            dnb_workers=max(dnb_workers, 1),
            gmap_workers=max(gmap_workers, 1),
            site_workers=max(site_workers, 1),
            snov_workers=max(snov_workers, 1),
            queue_poll_interval=_env_float("DNB_KOREA_QUEUE_POLL_INTERVAL", 2.0),
            stale_running_requeue_seconds=_env_int(
                "DNB_KOREA_STALE_RUNNING_REQUEUE_SECONDS",
                600,
            ),
            gmap_max_retries=_env_int("DNB_KOREA_GMAP_MAX_RETRIES", 3),
            site_max_retries=_env_int("DNB_KOREA_SITE_MAX_RETRIES", 5),
            snov_task_max_retries=_env_int("DNB_KOREA_SNOV_TASK_MAX_RETRIES", 5),
            retry_backoff_cap_seconds=_env_float(
                "DNB_KOREA_RETRY_BACKOFF_CAP_SECONDS",
                180.0,
            ),
        )

    def validate(self, *, skip_site_name: bool, skip_snov: bool) -> None:
        if not skip_snov and (not self.snov_client_id or not self.snov_client_secret):
            raise RuntimeError("snov 阶段缺少 SNOV_CLIENT_ID / SNOV_CLIENT_SECRET。")
        if not skip_site_name:
            email_agent = EmailAgentConfig.from_env(self.project_root)
            if not email_agent.llm_api_key or not email_agent.llm_model:
                raise RuntimeError("site 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。")
            if not email_agent.firecrawl_keys_file.exists():
                raise RuntimeError("site 阶段缺少 Firecrawl key 文件。")
