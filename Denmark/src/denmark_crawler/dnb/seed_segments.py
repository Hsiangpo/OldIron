"""DNB 静态种子切片。"""

from __future__ import annotations

import json
from pathlib import Path

from denmark_crawler.dnb.models import Segment


def load_seed_rows(path: str | Path) -> list[dict[str, object]]:
    """从 JSONL 加载 DNB 种子切片。"""
    source = Path(path).resolve()
    rows: list[dict[str, object]] = []
    for line in source.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if not isinstance(payload, dict):
            continue
        segment = Segment.from_dict(payload)
        row = segment.to_dict()
        row["segment_id"] = segment.segment_id
        rows.append(row)
    return rows

