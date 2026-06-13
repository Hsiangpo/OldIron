from __future__ import annotations

from urllib.parse import urlparse


def build_email_teacher_pool(snapshot, website: str, *, limit: int = 60) -> list[str]:
    selected = {_teacher_identity_url(url) for url in snapshot.email_urls}
    homepage_key = _teacher_identity_url(website)
    result: list[str] = []
    seen: set[str] = set()
    ordered = sorted(
        snapshot.candidates,
        key=lambda item: (-item.email_final_score, item.depth, item.discovery_order, item.url),
    )
    for candidate in ordered:
        _append_email_teacher_url(candidate.url, result, seen, selected, homepage_key, limit)
        if len(result) >= limit:
            return result
    for url in snapshot.urls:
        _append_email_teacher_url(url, result, seen, selected, homepage_key, limit)
        if len(result) >= limit:
            break
    return result


def pick_email_urls_or_empty(
    llm_client,
    *,
    homepage: str,
    candidate_urls: list[str],
    existing_email_urls: list[str],
    target_count: int,
    deadline_monotonic: float | None,
) -> list[str]:
    picker = getattr(llm_client, "pick_email_urls", None)
    if not callable(picker):
        return []
    try:
        return picker(
            homepage=homepage,
            candidate_urls=candidate_urls,
            existing_email_urls=existing_email_urls,
            target_count=target_count,
            deadline_monotonic=deadline_monotonic,
        )
    except Exception:  # noqa: BLE001
        return []


def _append_email_teacher_url(
    url: str,
    result: list[str],
    seen: set[str],
    selected: set[str],
    homepage_key: str,
    limit: int,
) -> None:
    if len(result) >= limit:
        return
    value = str(url or "").strip()
    key = _teacher_identity_url(value)
    if not value or not key or key == homepage_key or key in selected or key in seen:
        return
    seen.add(key)
    result.append(value)


def _teacher_identity_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.netloc:
        return str(url or "").strip().lower().rstrip("/")
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").rstrip("/")
    if path == "/":
        path = ""
    query = str(parsed.query or "").strip()
    return f"{host}{path}?{query}" if query else f"{host}{path}"
