"""Italy DNB 站点配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class ItalyDnbConfig:
    project_root: Path
    output_dir: Path
    country_iso_two_code: str = "it"
    segment_workers: int = 3
    verif_workers: int = 1
    email_workers: int = 8
    max_segments: int = 0
    max_pages_per_segment: int = 20
    industry_paths: tuple[str, ...] = ()
    queue_poll_interval: float = 2.0
    stale_running_requeue_seconds: float = 900.0
    proxy_url: str = "http://127.0.0.1:7897"
    verif_timeout_seconds: float = 180.0
    verif_headless: bool = False
    email_page_soft_limit: int = 8
    email_page_hard_limit: int = 16
    email_total_hard_limit: int = 20
    email_stop_same_domain_count: int = 2

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        segment_workers: int,
        verif_workers: int,
        email_workers: int,
        max_segments: int,
        max_pages_per_segment: int,
        industry_paths: str,
    ) -> "ItalyDnbConfig":
        proxy_url = str(
            os.getenv("HTTP_PROXY", os.getenv("HTTPS_PROXY", "http://127.0.0.1:7897")) or ""
        ).strip() or "http://127.0.0.1:7897"
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            segment_workers=max(int(segment_workers or 1), 1),
            verif_workers=max(int(verif_workers or 1), 1),
            email_workers=max(int(email_workers or 1), 1),
            max_segments=max(int(max_segments or 0), 0),
            max_pages_per_segment=max(int(max_pages_per_segment or 1), 1),
            industry_paths=tuple(
                item.strip()
                for item in str(industry_paths or "").split(",")
                if item.strip()
            ),
            proxy_url=proxy_url,
            verif_timeout_seconds=max(float(os.getenv("VERIF_TIMEOUT_SECONDS", "180") or "180"), 30.0),
            verif_headless=_env_bool("VERIF_HEADLESS", False),
            email_page_soft_limit=max(int(os.getenv("EMAIL_PAGE_SOFT_LIMIT", "8") or "8"), 0),
            email_page_hard_limit=max(int(os.getenv("EMAIL_PAGE_HARD_LIMIT", "16") or "16"), 1),
            email_total_hard_limit=max(int(os.getenv("PAGE_TOTAL_HARD_LIMIT", "20") or "20"), 1),
            email_stop_same_domain_count=max(int(os.getenv("EMAIL_STOP_SAME_DOMAIN_COUNT", "2") or "2"), 1),
        )

