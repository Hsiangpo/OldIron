"""IEATPE 配置。"""

from __future__ import annotations

import os
import string
from dataclasses import dataclass
from pathlib import Path


def _normalize_letters(raw: str) -> tuple[str, ...]:
    tokens = [item.strip().upper() for item in str(raw or "").split(",") if item.strip()]
    if not tokens:
        tokens = list(string.ascii_uppercase)
    unique: list[str] = []
    for token in tokens:
        if len(token) == 1 and token in string.ascii_uppercase and token not in unique:
            unique.append(token)
    if not unique:
        raise ValueError("letters 必须是 A-Z 的逗号分隔列表。")
    return tuple(unique)


@dataclass(slots=True)
class IeatpeConfig:
    project_root: Path
    output_dir: Path
    flow: str = "12"
    letters: tuple[str, ...] = tuple(string.ascii_uppercase)
    list_workers: int = 4
    detail_workers: int = 12
    request_delay: float = 0.2
    timeout_seconds: float = 30.0
    proxy_url: str = ""
    stale_running_requeue_seconds: float = 600.0

    @classmethod
    def from_env(
        cls,
        *,
        project_root: Path,
        output_dir: Path,
        letters: str = "",
        flow: str = "12",
        list_workers: int = 4,
        detail_workers: int = 12,
        request_delay: float = 0.2,
        timeout_seconds: float = 30.0,
    ) -> "IeatpeConfig":
        return cls(
            project_root=project_root,
            output_dir=output_dir,
            flow=str(flow or "12").strip() or "12",
            letters=_normalize_letters(letters),
            list_workers=max(int(list_workers or 1), 1),
            detail_workers=max(int(detail_workers or 1), 1),
            request_delay=max(float(request_delay or 0.0), 0.0),
            timeout_seconds=max(float(timeout_seconds or 10.0), 10.0),
            proxy_url=os.getenv("HTTP_PROXY", "").strip() or "http://127.0.0.1:7897",
        )

    def validate(self) -> None:
        if not self.flow:
            raise RuntimeError("IEATPE 缺少 flow 配置。")
