"""DNB 历史 store 到分片 run 目录的迁移。"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from denmark_crawler.dnb.catalog import load_naics_catalog
from denmark_crawler.dnb.seed_segments import load_seed_rows
from denmark_crawler.dnb.store import DnbDenmarkStore


def _copy_sqlite_db(source_db: Path, target_db: Path) -> None:
    target_db.parent.mkdir(parents=True, exist_ok=True)
    if target_db.exists():
        target_db.unlink()
    source = sqlite3.connect(source_db)
    target = sqlite3.connect(target_db)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()


def _load_manifest(shard_dir: Path) -> dict[str, object]:
    manifest_path = shard_dir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"未找到分片清单: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _parse_shard_index(shard_file: Path) -> int:
    stem = shard_file.stem
    number = stem.split("-", 1)[1].split(".", 1)[0]
    return max(int(number) - 1, 0)


def _stable_bucket(value: str, shard_count: int) -> int:
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(int(shard_count), 1)


def _top_level_root_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    marker = "/business-directory/industry-analysis."
    for category in load_naics_catalog():
        top_level = category.get("top_level", {})
        if not isinstance(top_level, dict):
            continue
        href = str(top_level.get("href", "")).strip()
        if marker not in href or not href.endswith(".html"):
            continue
        root = href.split(marker, 1)[1][:-5].strip().lower()
        if not root:
            continue
        mapping[root] = root
        subcategories = category.get("subcategories", [])
        if not isinstance(subcategories, list):
            continue
        for item in subcategories:
            if not isinstance(item, dict):
                continue
            child_href = str(item.get("href", "")).strip()
            if marker not in child_href or not child_href.endswith(".html"):
                continue
            child = child_href.split(marker, 1)[1][:-5].strip().lower()
            if child:
                mapping[child] = root
    return mapping


def _load_keep_values(conn: sqlite3.Connection, table_name: str, values: set[str]) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TEMP TABLE {table_name}(item_id TEXT PRIMARY KEY)")
    conn.executemany(
        f"INSERT INTO {table_name}(item_id) VALUES(?)",
        [(value,) for value in sorted(values)],
    )


def _dnb_keep_duns(conn: sqlite3.Connection, shard_index: int, shard_count: int) -> set[str]:
    rows = conn.execute("SELECT duns FROM companies ORDER BY duns").fetchall()
    keep: set[str] = set()
    for (duns,) in rows:
        value = str(duns or "").strip()
        if value and _stable_bucket(value, shard_count) == shard_index:
            keep.add(value)
    return keep


def _bootstrap_dnb_store(
    legacy_db: Path,
    shard_file: Path,
    shard_count: int,
    output_root: Path,
    root_map: dict[str, str],
) -> dict[str, object]:
    shard_index = _parse_shard_index(shard_file)
    target_dir = output_root / f"dnb-shard-{shard_index + 1:03d}"
    shutil.rmtree(target_dir, ignore_errors=True)
    target_db = target_dir / "store.db"
    _copy_sqlite_db(legacy_db, target_db)
    seed_rows = load_seed_rows(shard_file)
    keep_roots = {str(row.get("industry_path", "")).strip().lower() for row in seed_rows if str(row.get("industry_path", "")).strip()}
    conn = sqlite3.connect(target_db)
    try:
        keep_duns = _dnb_keep_duns(conn, shard_index, shard_count)
        _load_keep_values(conn, "tmp_keep_duns", keep_duns)
        conn.execute(
            """
            DELETE FROM companies
            WHERE NOT EXISTS (
                SELECT 1 FROM tmp_keep_duns WHERE tmp_keep_duns.item_id = companies.duns
            )
            """
        )
        conn.execute(
            """
            DELETE FROM final_companies
            WHERE NOT EXISTS (
                SELECT 1 FROM tmp_keep_duns WHERE tmp_keep_duns.item_id = final_companies.duns
            )
            """
        )
        for table in ("detail_queue", "gmap_queue", "site_queue", "snov_queue"):
            if not _table_exists(conn, table):
                continue
            conn.execute(
                f"""
                DELETE FROM {table}
                WHERE NOT EXISTS (
                    SELECT 1 FROM tmp_keep_duns WHERE tmp_keep_duns.item_id = {table}.duns
                )
                """
            )

        discovery_rows = conn.execute("SELECT segment_id, industry_path FROM dnb_discovery_queue").fetchall()
        for segment_id, industry_path in discovery_rows:
            root = root_map.get(str(industry_path).strip().lower(), str(industry_path).strip().lower())
            if root not in keep_roots:
                conn.execute("DELETE FROM dnb_discovery_queue WHERE segment_id = ?", (segment_id,))

        segment_rows = conn.execute("SELECT segment_id, industry_path FROM dnb_segments").fetchall()
        for segment_id, industry_path in segment_rows:
            root = root_map.get(str(industry_path).strip().lower(), str(industry_path).strip().lower())
            if root not in keep_roots:
                conn.execute("DELETE FROM dnb_segments WHERE segment_id = ?", (segment_id,))

        if _table_exists(conn, "runtime_meta"):
            conn.execute("DELETE FROM runtime_meta WHERE meta_key = 'dnb_seed_signature'")
        conn.commit()
    finally:
        conn.close()

    store = DnbDenmarkStore(target_db)
    try:
        store.ensure_seed_signature(seed_rows)
        store.export_jsonl_snapshots(target_dir)
    finally:
        store.close()
    return {
        "shard_file": str(shard_file),
        "output_dir": str(target_dir),
        "root_count": len(keep_roots),
    }


def bootstrap_dnb_shards(
    legacy_db_path: str | Path,
    shard_dir: str | Path,
    output_root: str | Path,
) -> dict[str, object]:
    """把旧 DNB store 切到各 shard run 目录。"""
    legacy_db = Path(legacy_db_path).resolve()
    if not legacy_db.exists():
        raise RuntimeError(f"未找到旧 DNB store: {legacy_db}")
    shard_root = Path(shard_dir).resolve()
    output_dir = Path(output_root).resolve()
    manifest = _load_manifest(shard_root)
    shard_files = sorted(shard_root.glob("shard-*.segments.jsonl"))
    shard_count = int(manifest.get("shard_count", len(shard_files)) or len(shard_files))
    root_map = _top_level_root_map()
    items: list[dict[str, object]] = []
    for shard_file in shard_files:
        items.append(
            _bootstrap_dnb_store(
                legacy_db=legacy_db,
                shard_file=shard_file,
                shard_count=shard_count,
                output_root=output_dir,
                root_map=root_map,
            )
        )
    return {
        "site": "dnb",
        "legacy_db": str(legacy_db),
        "shard_count": shard_count,
        "items": items,
    }

