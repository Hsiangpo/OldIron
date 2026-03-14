from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from site_agent.utils import canonical_site_key, normalize_url

from ..store import utc_now_iso, write_json_atomic, read_json


@dataclass(frozen=True)
class KeywordPaths:
    keyword_id: str
    query: str
    keyword_dir: Path
    meta_json: Path
    success_jsonl: Path
    success_csv: Path
    failed_jsonl: Path
    failed_csv: Path


def keyword_id_from_query(query: str) -> str:
    q = (query or "").strip()
    digest = hashlib.sha1(q.encode("utf-8")).hexdigest()
    return digest[:12]


def keyword_id_from_scope(*, country: str, region: str, keyword: str) -> str:
    seed = json.dumps(
        {
            "country": (country or "").strip(),
            "region": (region or "").strip(),
            "keyword": (keyword or "").strip(),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


def build_keyword_paths(keywords_dir: Path, *, keyword_id: str, query: str) -> KeywordPaths:
    base = keywords_dir / keyword_id
    return KeywordPaths(
        keyword_id=keyword_id,
        query=query,
        keyword_dir=base,
        meta_json=base / "meta.json",
        success_jsonl=base / "output.success.jsonl",
        success_csv=base / "output.success.csv",
        failed_jsonl=base / "output.failed.jsonl",
        failed_csv=base / "output.failed.csv",
    )


def load_keyword_query(keywords_dir: Path, keyword_id: str) -> str | None:
    meta = read_json(keywords_dir / keyword_id / "meta.json")
    if not isinstance(meta, dict):
        return None
    label = meta.get("label") or meta.get("query")
    if isinstance(label, str) and label.strip():
        return label.strip()
    return None


def upsert_keyword_from_records(
    keywords_dir: Path,
    *,
    query: str,
    country: str | None = None,
    region: str | None = None,
    records: Iterable[dict[str, Any]],
    job_id: str | None = None,
    job_created_at: str | None = None,
) -> KeywordPaths:
    q = (query or "").strip()
    if not q:
        raise ValueError("query 不能为空")

    keywords_dir.mkdir(parents=True, exist_ok=True)
    country_str = (country or "").strip()
    region_str = (region or "").strip()
    keyword_id = keyword_id_from_scope(country=country_str or "其他", region=region_str or "全域", keyword=q)
    label = f"{country_str or '其他'} / {region_str or '全域'} / {q}"
    paths = build_keyword_paths(keywords_dir, keyword_id=keyword_id, query=label)
    paths.keyword_dir.mkdir(parents=True, exist_ok=True)

    existing_success = _read_jsonl(paths.success_jsonl)
    existing_failed = _read_jsonl(paths.failed_jsonl)
    merged = _merge_records(existing_success + existing_failed, list(records))

    success_records = [r for r in merged.values() if r.get("status") in ("ok", "partial")]
    failed_records = [r for r in merged.values() if r.get("status") == "failed"]

    _write_jsonl_atomic(paths.success_jsonl, success_records)
    _write_csv_atomic(paths.success_csv, success_records)
    _write_jsonl_atomic(paths.failed_jsonl, failed_records)
    _write_csv_atomic(paths.failed_csv, failed_records)

    now = utc_now_iso()
    meta = read_json(paths.meta_json) or {}
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("id", keyword_id)
    meta.setdefault("query", q)
    meta.setdefault("label", label)
    if country_str:
        meta.setdefault("country", country_str)
    if region_str:
        meta.setdefault("region", region_str)
    meta.setdefault("created_at", now)
    meta["updated_at"] = now
    meta["counts"] = {
        "success": len(success_records),
        "failed": len(failed_records),
        "total": len(success_records) + len(failed_records),
    }
    if job_id:
        meta["last_job_id"] = job_id
    if job_created_at:
        meta["last_job_at"] = job_created_at
    write_json_atomic(paths.meta_json, meta)
    return paths


def read_keyword_records(
    keywords_dir: Path,
    *,
    keyword_id: str,
    status: str | None = None,
) -> list[dict[str, Any]]:
    base = keywords_dir / keyword_id
    success = _read_jsonl(base / "output.success.jsonl")
    failed = _read_jsonl(base / "output.failed.jsonl")
    items = success + failed
    if status in ("ok", "partial", "failed"):
        items = [r for r in items if r.get("status") == status]
    return items


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    items.append(obj)
    except Exception:
        return []
    return items


def _merge_records(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def key_of(rec: dict[str, Any]) -> str | None:
        website = rec.get("website")
        if not isinstance(website, str) or not website.strip():
            return None
        key = canonical_site_key(website.strip())
        return key or normalize_url(website.strip())

    for rec in existing:
        key = key_of(rec)
        if not key:
            continue
        merged[key] = rec

    for rec in incoming:
        key = key_of(rec)
        if not key:
            continue
        prev = merged.get(key)
        if not isinstance(prev, dict):
            merged[key] = rec
            continue
        merged[key] = _merge_one(prev, rec)

    # 归一化状态
    for key, rec in list(merged.items()):
        merged[key] = _normalize_status(rec)

    return merged


def _merge_one(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    merged = dict(old)

    for field, source_field in (
        ("company_name", "company_name_source_url"),
        ("representative", "representative_source_url"),
        ("phone", "phone_source_url"),
        ("email", "email_source_url"),
    ):
        new_val = new.get(field)
        if isinstance(new_val, str) and new_val.strip():
            merged[field] = new_val.strip()
            src = new.get(source_field)
            if isinstance(src, str) and src.strip():
                merged[source_field] = src.strip()

    for field in ("input_name", "notes"):
        new_val = new.get(field)
        if isinstance(new_val, str) and new_val.strip():
            if not (isinstance(merged.get(field), str) and str(merged.get(field) or "").strip()):
                merged[field] = new_val.strip()

    # 来源 URLs：做并集
    src_urls = []
    for v in (old.get("source_urls"), new.get("source_urls")):
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str) and item.strip():
                    src_urls.append(item.strip())
    if src_urls:
        dedup: list[str] = []
        seen: set[str] = set()
        for u in src_urls:
            if u in seen:
                continue
            seen.add(u)
            dedup.append(u)
        merged["source_urls"] = dedup

    # 抓取时间：取更晚的
    old_time = old.get("extracted_at")
    new_time = new.get("extracted_at")
    if isinstance(new_time, str) and new_time.strip():
        if not isinstance(old_time, str) or not old_time.strip() or new_time > old_time:
            merged["extracted_at"] = new_time

    # error：只有当最终仍失败时才保留
    if isinstance(new.get("error"), str) and new.get("error").strip():
        merged["error"] = new.get("error")

    # raw_llm：优先保留更完整的（字段更多的）
    if isinstance(new.get("raw_llm"), dict):
        old_raw = old.get("raw_llm")
        if not isinstance(old_raw, dict):
            merged["raw_llm"] = new.get("raw_llm")
        else:
            merged["raw_llm"] = old_raw if _record_score(old) >= _record_score(new) else new.get("raw_llm")

    return merged


def _record_score(rec: dict[str, Any]) -> int:
    score = 0
    for k in ("company_name", "representative", "email", "phone"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            score += 1
    status = rec.get("status")
    if status == "ok":
        score += 2
    if status == "partial":
        score += 1
    return score


def _normalize_status(rec: dict[str, Any]) -> dict[str, Any]:
    company = rec.get("company_name")
    rep = rec.get("representative")
    email = rec.get("email")
    has_company = isinstance(company, str) and company.strip()
    has_rep = isinstance(rep, str) and rep.strip()
    has_email = isinstance(email, str) and email.strip()
    if has_company and has_rep and has_email:
        rec["status"] = "ok"
        rec["error"] = None
        return rec
    if has_company or has_rep or has_email:
        rec["status"] = "partial"
        rec["error"] = None
        return rec
    rec["status"] = "failed"
    if not (isinstance(rec.get("error"), str) and str(rec.get("error") or "").strip()):
        rec["error"] = "no_content"
    return rec


def _write_jsonl_atomic(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(path)


def _write_csv_atomic(path: Path, records: list[dict[str, Any]]) -> None:
    # CSV 只给老板用：保持与站点输出一致的列顺序
    headers = [
        "input_name",
        "website",
        "company_name",
        "representative",
        "phone",
        "email",
        "company_name_source_url",
        "representative_source_url",
        "phone_source_url",
        "email_source_url",
        "notes",
        "status",
        "error",
        "extracted_at",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(",".join(_csv_escape(h) for h in headers) + "\n")
        for rec in records:
            row = []
            for h in headers:
                val = rec.get(h)
                if val is None:
                    row.append("")
                elif isinstance(val, str):
                    row.append(val)
                else:
                    row.append(str(val))
            f.write(",".join(_csv_escape(x) for x in row) + "\n")
    tmp.replace(path)


def _csv_escape(value: str) -> str:
    text = value or ""
    if any(ch in text for ch in (",", "\"", "\n", "\r")):
        return "\"" + text.replace("\"", "\"\"") + "\""
    return text
