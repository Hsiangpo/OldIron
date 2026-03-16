"""DNB 静态切片规划。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from england_crawler.dnb.catalog import load_naics_catalog
from england_crawler.dnb.models import Segment


def build_seed_rows(country_iso_two_code: str) -> list[dict[str, object]]:
    """生成 DNB 非重叠根种子切片。"""
    country = str(country_iso_two_code or "").strip().lower()
    rows: list[dict[str, object]] = []
    for category in load_naics_catalog():
        top_level = category.get("top_level", {})
        if not isinstance(top_level, dict):
            continue
        href = str(top_level.get("href", "")).strip()
        marker = "/business-directory/industry-analysis."
        if marker not in href or not href.endswith(".html"):
            continue
        industry_path = href.split(marker, 1)[1][:-5].strip().lower()
        if not industry_path:
            continue
        segment = Segment(
            industry_path=industry_path,
            country_iso_two_code=country,
            expected_count=0,
            segment_type="industry",
        )
        payload = segment.to_dict()
        payload["segment_id"] = segment.segment_id
        rows.append(payload)
    return rows


def _bucket_index(segment_id: str, shard_count: int) -> int:
    digest = hashlib.md5(segment_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(int(shard_count), 1)


def _write_segments(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def plan_dnb_shards(
    output_dir: str | Path,
    *,
    shard_count: int,
    country_iso_two_code: str,
) -> dict[str, object]:
    """把 DNB 根种子切成多个固定分片。"""
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    seed_rows = build_seed_rows(country_iso_two_code)
    buckets: list[list[dict[str, object]]] = [[] for _ in range(max(int(shard_count), 1))]
    for row in seed_rows:
        buckets[_bucket_index(str(row["segment_id"]), len(buckets))].append(row)

    shard_meta: list[dict[str, object]] = []
    for index, rows in enumerate(buckets):
        filename = f"shard-{index + 1:03d}.segments.jsonl"
        _write_segments(target_dir / filename, rows)
        shard_meta.append(
            {
                "shard_id": index + 1,
                "file": filename,
                "segment_count": len(rows),
            }
        )

    summary = {
        "site": "dnb",
        "country_iso_two_code": str(country_iso_two_code or "").strip().lower(),
        "output_dir": str(target_dir),
        "shard_count": len(buckets),
        "total_segments": len(seed_rows),
        "shards": shard_meta,
    }
    (target_dir / "manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary
