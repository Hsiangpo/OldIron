from __future__ import annotations

from typing import Any, Iterable
from urllib.parse import urlparse, urlunparse


def get_nested(data: Any, path: Iterable[int], default: Any = None) -> Any:
    cur = data
    for key in path:
        if not isinstance(cur, list) or key >= len(cur):
            return default
        cur = cur[key]
    return cur


def flatten_strings(obj: Any) -> list[str]:
    strings: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, str):
            strings.append(value)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(obj)
    return strings


def is_url(text: str) -> bool:
    return text.startswith(("http://", "https://", "//"))


def normalize_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("//"):
        value = "https:" + value
    if not value.startswith("http://") and not value.startswith("https://"):
        value = "https://" + value
    parsed = urlparse(value)
    netloc = (parsed.netloc or "").strip()
    if not netloc or " " in netloc:
        return ""
    cleaned = parsed._replace(netloc=netloc, fragment="")
    return urlunparse(cleaned).rstrip("/")


def looks_like_domain(value: str) -> bool:
    text = value.strip().lower()
    if not text or " " in text or "@" in text:
        return False
    # 域名不允许下划线；同时避免把 Google 内部枚举值当成官网（如 SearchResult.TYPE_*）。
    if "_" in text:
        return False
    if "/" in text:
        return False
    return "." in text
