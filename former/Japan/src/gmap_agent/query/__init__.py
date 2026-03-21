from __future__ import annotations

from pathlib import Path
from typing import Iterable
import re


_SEPARATORS = ("|", ",", "，", ";", "；", "、")


def split_queries(raw: str | None) -> list[str]:
    if not raw:
        return []
    text = str(raw).strip()
    if not text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 1:
        return _dedupe_keep_order(lines)

    for sep in _SEPARATORS:
        if sep in text:
            parts = [part.strip() for part in re.split(r"[|,，;；、]+", text) if part.strip()]
            return _dedupe_keep_order(parts)

    return [text]


def load_queries(query: str | None, query_file: str | None) -> list[str]:
    items: list[str] = []
    if query_file:
        path = Path(str(query_file))
        if path.exists():
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                items.append(stripped)
    items.extend(split_queries(query))
    return _dedupe_keep_order(items)


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out
