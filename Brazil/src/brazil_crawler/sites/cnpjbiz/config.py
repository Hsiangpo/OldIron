"""CNPJ Biz 站点配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_API_STYLE
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_BASE_URL
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_MODEL
from shared.oldiron_core.fc_email.email_service import DEFAULT_LLM_REASONING_EFFORT


@dataclass(slots=True)
class CnpjBizConfig:
    project_root: Path
    output_dir: Path
    cdp_url: str = "http://127.0.0.1:9222"
    proxy_url: str = ""
    proxy_feed_url: str = ""
    proxy_feed_scheme: str = "http"
    list_workers: int = 1
    detail_workers: int = 8
    max_pages: int = 0
    queue_poll_interval: float = 2.0
    stale_running_requeue_seconds: float = 900.0
    log_interval_seconds: float = 10.0
    cdp_timeout_seconds: float = 10.0
    cookie_cache_seconds: float = 300.0
    llm_api_key: str = ""
    llm_base_url: str = DEFAULT_LLM_BASE_URL
    llm_model: str = DEFAULT_LLM_MODEL
    llm_reasoning_effort: str = DEFAULT_LLM_REASONING_EFFORT
    llm_api_style: str = DEFAULT_LLM_API_STYLE
    llm_timeout_seconds: float = 120.0

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        list_workers: int,
        detail_workers: int,
        max_pages: int,
    ) -> "CnpjBizConfig":
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            cdp_url=str(os.getenv("CNPJBIZ_CDP_URL", "http://127.0.0.1:9222") or "").strip() or "http://127.0.0.1:9222",
            proxy_url=str(os.getenv("CNPJBIZ_PROXY_URL", "") or "").strip(),
            proxy_feed_url=str(os.getenv("CNPJBIZ_PROXY_FEED_URL", "") or "").strip(),
            proxy_feed_scheme=str(os.getenv("CNPJBIZ_PROXY_FEED_SCHEME", "http") or "http").strip().lower() or "http",
            list_workers=max(int(list_workers or 1), 1),
            detail_workers=max(int(detail_workers or 1), 1),
            max_pages=max(int(max_pages or 0), 0),
            cdp_timeout_seconds=max(float(os.getenv("CNPJBIZ_CDP_TIMEOUT_SECONDS", "10") or "10"), 5.0),
            cookie_cache_seconds=max(float(os.getenv("CNPJBIZ_COOKIE_CACHE_SECONDS", "300") or "300"), 30.0),
            llm_api_key=str(os.getenv("LLM_API_KEY", "") or "").strip(),
            llm_base_url=str(os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL) or DEFAULT_LLM_BASE_URL).strip(),
            llm_model=str(os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL) or DEFAULT_LLM_MODEL).strip(),
            llm_reasoning_effort=str(os.getenv("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT) or "").strip(),
            llm_api_style=str(os.getenv("LLM_API_STYLE", DEFAULT_LLM_API_STYLE) or DEFAULT_LLM_API_STYLE).strip() or DEFAULT_LLM_API_STYLE,
            llm_timeout_seconds=max(float(os.getenv("LLM_TIMEOUT_SECONDS", "120") or "120"), 20.0),
        )
