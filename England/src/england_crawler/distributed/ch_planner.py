"""Companies House 静态切片规划。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from england_crawler.companies_house.client import normalize_company_name
from england_crawler.companies_house.input_source import load_company_names_from_source


def _bucket_index(normalized_name: str, shard_count: int) -> int:
    digest = hashlib.md5(normalized_name.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(int(shard_count), 1)


def _shard_filename(index: int) -> str:
    return f"shard-{index + 1:03d}.txt"


def plan_companies_house_shards(
    input_path: str | Path,
    output_dir: str | Path,
    *,
    shard_count: int,
) -> dict[str, object]:
    """把 Companies House 输入稳定切成多个文本分片。"""
    source = Path(input_path).resolve()
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    buckets: list[list[str]] = [[] for _ in range(max(int(shard_count), 1))]
    for company_name in load_company_names_from_source(source):
        normalized = normalize_company_name(company_name)
        if not normalized:
            continue
        buckets[_bucket_index(normalized, len(buckets))].append(company_name)

    shard_meta: list[dict[str, object]] = []
    total = 0
    for index, names in enumerate(buckets):
        filename = _shard_filename(index)
        shard_path = target_dir / filename
        shard_path.write_text("\n".join(names), encoding="utf-8")
        total += len(names)
        shard_meta.append(
            {
                "shard_id": index + 1,
                "file": filename,
                "company_count": len(names),
            }
        )

    summary = {
        "site": "companies-house",
        "input_path": str(source),
        "output_dir": str(target_dir),
        "shard_count": len(buckets),
        "total_companies": total,
        "shards": shard_meta,
    }
    (target_dir / "manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary
