"""共享交付规格。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


def never_suspicious(_record: dict[str, object]) -> bool:
    """默认不过滤疑似错配记录。"""
    return False


@dataclass(slots=True)
class DeliverySpec:
    """国家级交付规格。"""

    country_name: str
    suspicious_filter: Callable[[dict[str, object]], bool] = never_suspicious
    ignored_site_dirs: tuple[str, ...] = ("delivery",)
    candidate_filenames: tuple[str, ...] = (
        "final_companies.jsonl",
        "companies_with_emails.jsonl",
    )

