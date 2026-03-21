from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ...models import ExtractionResult, PageContent
from ...utils import safe_slug


def _result_to_record(result: ExtractionResult) -> dict[str, Any]:
    data = asdict(result)
    if result.raw_llm is None:
        data.pop("raw_llm", None)
    return data


def _save_pages_markdown(
    pages_dir: Path, website: str, pages: dict[str, PageContent]
) -> None:
    pages_dir.mkdir(parents=True, exist_ok=True)
    site_dir = pages_dir / safe_slug(website)
    site_dir.mkdir(parents=True, exist_ok=True)
    for page in pages.values():
        file_name = safe_slug(page.url) + ".md"
        path = site_dir / file_name
        path.write_text(page.markdown, encoding="utf-8")


def _load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records

