"""DNB 巴西站点配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DnbBrConfig:
    project_root: Path
    output_dir: Path
    cdp_url: str = "http://127.0.0.1:9222"
    country_iso_two_code: str = "br"
    segment_workers: int = 4
    detail_workers: int = 4
    gmap_workers: int = 128
    email_workers: int = 128
    max_segments: int = 0
    max_pages_per_segment: int = 20
    industry_paths: tuple[str, ...] = ()
    queue_poll_interval: float = 2.0
    stale_running_requeue_seconds: float = 900.0
    log_interval_seconds: float = 10.0
    snapshot_interval_seconds: float = 30.0
    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_model: str = ""
    llm_reasoning_effort: str = "medium"
    llm_api_style: str = "auto"
    llm_timeout_seconds: float = 120.0

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        segment_workers: int,
        detail_workers: int,
        gmap_workers: int,
        email_workers: int,
        max_segments: int,
        max_pages_per_segment: int,
        industry_paths: str,
    ) -> "DnbBrConfig":
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            cdp_url=os.getenv("DNB_CDP_URL", "http://127.0.0.1:9222").strip() or "http://127.0.0.1:9222",
            segment_workers=max(int(segment_workers or 1), 1),
            detail_workers=max(int(detail_workers or 1), 1),
            gmap_workers=max(int(gmap_workers or 1), 1),
            email_workers=max(int(email_workers or 1), 1),
            max_segments=max(int(max_segments or 0), 0),
            max_pages_per_segment=max(int(max_pages_per_segment or 1), 1),
            industry_paths=tuple(
                item.strip()
                for item in str(industry_paths or "").split(",")
                if item.strip()
            ),
            llm_api_key=os.getenv("LLM_API_KEY", "").strip(),
            llm_base_url=os.getenv("LLM_BASE_URL", "https://api.gpteamservices.com/v1").strip(),
            llm_model=os.getenv("LLM_MODEL", "gpt-5.4-mini").strip(),
            llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", "medium").strip(),
            llm_api_style=os.getenv("LLM_API_STYLE", "auto").strip() or "auto",
            llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        )

    def validate(self, *, skip_email: bool) -> None:
        if skip_email:
            return
        if not self.llm_api_key:
            raise RuntimeError("DNB 巴西站点缺少 LLM_API_KEY。")
