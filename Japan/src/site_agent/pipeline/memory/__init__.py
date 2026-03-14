from __future__ import annotations

from typing import Any

from ...models import PageContent


def _update_memory_visited(
    memory: dict[str, Any], visited: dict[str, PageContent]
) -> None:
    memory["visited"] = sorted(visited.keys())


def _update_memory_found(memory: dict[str, Any], info: dict[str, Any] | None) -> None:
    if not isinstance(info, dict):
        return
    found_obj = memory.get("found")
    found: dict[str, Any] = found_obj if isinstance(found_obj, dict) else {}
    for key in (
        "company_name",
        "representative",
        "capital",
        "employees",
        "email",
        "phone",
    ):
        value = info.get(key)
        if isinstance(value, str) and value.strip():
            found[key] = value.strip()
    memory["found"] = found


def _remember_failed(memory: dict[str, Any], url: str | None) -> None:
    if not url:
        return
    failed = memory.get("failed")
    if not isinstance(failed, list):
        failed = []
    if url not in failed:
        failed.append(url)
    memory["failed"] = failed[-80:]


def _remember_selected(memory: dict[str, Any], urls: list[str]) -> None:
    selected = memory.get("selected")
    if not isinstance(selected, list):
        selected = []
    for url in urls:
        if url not in selected:
            selected.append(url)
    memory["selected"] = selected[-120:]

