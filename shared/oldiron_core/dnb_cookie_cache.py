"""DNB cookie 持久化缓存。"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


def load_dnb_cookie_snapshot(cache_file: Path, *, max_age_seconds: float) -> dict[str, Any] | None:
    """读取仍在有效期内的 DNB cookie 快照。"""
    path = Path(cache_file)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(payload, dict):
        return None
    cookies = payload.get("cookies")
    headers = payload.get("headers")
    saved_at = float(payload.get("saved_at") or 0.0)
    if not isinstance(cookies, list) or not isinstance(headers, dict):
        return None
    if max_age_seconds > 0 and saved_at > 0 and (time.time() - saved_at) > max_age_seconds:
        return None
    return {
        "cookies": cookies,
        "headers": headers,
        "saved_at": saved_at,
    }


def save_dnb_cookie_snapshot(
    cache_file: Path,
    *,
    cookies: list[dict[str, str]],
    headers: dict[str, str],
) -> None:
    """落盘 DNB cookie 快照，供后续进程复用。"""
    path = Path(cache_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at": time.time(),
        "cookies": list(cookies),
        "headers": dict(headers),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
