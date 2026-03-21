from __future__ import annotations

import json
from pathlib import Path

from web_agent.service.job import _build_gmap_queries, _collect_company_names
from web_agent.service.job import _load_parallel_sync_count, _write_parallel_sync_count
from web_agent.service.job import _normalize_site_required_fields
from web_agent.service.job import _extract_company_name_from_query
from web_agent.service.job import _estimate_migrated_query_resume_index
from web_agent.service.job import _is_simple_mode
from web_agent.service.job import _looks_legacy_query_sample
from web_agent.service.job import JobService
from web_agent.store import build_job_paths


def test_build_gmap_queries_uses_company_names_only() -> None:
    names = ["株式会社A", "株式会社A", " 株式会社B ", ""]
    queries = _build_gmap_queries(names, limit=None)
    assert queries == ["株式会社A", "株式会社B"]


def test_build_gmap_queries_honors_limit() -> None:
    names = ["株式会社A", "株式会社B", "株式会社C"]
    queries = _build_gmap_queries(names, limit=2)
    assert queries == ["株式会社A", "株式会社B"]


def test_collect_company_names_without_limit(tmp_path: Path) -> None:
    path = tmp_path / "registry.jsonl"
    rows = [
        {"name": "株式会社A"},
        {"company_name": "株式会社B"},
        {"name": "株式会社A"},
        {"name": ""},
        {"name": "株式会社C"},
    ]
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")

    assert _collect_company_names(path, limit=None) == ["株式会社A", "株式会社B", "株式会社C"]
    assert _collect_company_names(path, limit=2) == ["株式会社A", "株式会社B"]


def test_prepare_gmap_stage_resume_reuses_existing_queries_with_legacy_meta(tmp_path: Path) -> None:
    jobs_dir = tmp_path / "jobs"
    job_dir = jobs_dir / "job_resume"
    paths = build_job_paths(jobs_dir, "job_resume", job_dir=job_dir)
    paths.gmap_dir.mkdir(parents=True, exist_ok=True)
    paths.registry_dir.mkdir(parents=True, exist_ok=True)

    query_file = paths.gmap_dir / "queries.txt"
    query_file.write_text("旧关键词A\n旧关键词B\n", encoding="utf-8")
    (paths.gmap_dir / "query_checkpoint.json").write_text(
        json.dumps({"next_query_index": 1, "total_queries": 2}, ensure_ascii=False),
        encoding="utf-8",
    )
    (paths.gmap_dir / "queries.meta.json").write_text(
        json.dumps({"format_version": 1, "query_count": 999, "format": "legacy"}, ensure_ascii=False),
        encoding="utf-8",
    )

    registry_output = paths.registry_dir / "output.jsonl"
    registry_output.write_text(
        "\n".join(
            [
                json.dumps({"name": "新公司1"}, ensure_ascii=False),
                json.dumps({"name": "新公司2"}, ensure_ascii=False),
                json.dumps({"name": "新公司3"}, ensure_ascii=False),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    service = JobService(jobs_dir)
    prepared = service._prepare_gmap_stage(paths, {"resume": True, "gmap": {}}, registry_output)

    assert prepared is not None
    args, _ = prepared
    assert "--query-file" in args
    assert query_file.read_text(encoding="utf-8") == "旧关键词A\n旧关键词B\n"
    assert (paths.gmap_dir / "query_checkpoint.json").exists()

    meta = json.loads((paths.gmap_dir / "queries.meta.json").read_text(encoding="utf-8"))
    assert meta.get("format_version") == 2
    assert meta.get("query_count") == 2


def test_parallel_sync_checkpoint_roundtrip(tmp_path: Path) -> None:
    gmap_dir = tmp_path / "gmap"
    gmap_dir.mkdir(parents=True, exist_ok=True)
    assert _load_parallel_sync_count(gmap_dir) == 0
    _write_parallel_sync_count(gmap_dir, 123)
    assert _load_parallel_sync_count(gmap_dir) == 123


def test_normalize_site_required_fields_drops_phone_by_default() -> None:
    fields = ["company_name", "representative", "capital", "employees", "phone", "email"]
    assert _normalize_site_required_fields(fields, require_phone=False) == [
        "company_name",
        "representative",
        "email",
    ]


def test_normalize_site_required_fields_keeps_phone_when_required() -> None:
    fields = ["company_name", "representative", "capital", "employees", "phone", "email"]
    assert _normalize_site_required_fields(fields, require_phone=True) == [
        "company_name",
        "representative",
        "email",
        "phone",
    ]


def test_extract_company_name_from_legacy_query() -> None:
    assert _extract_company_name_from_query("株式会社A official site 大阪府") == "株式会社A"
    assert _extract_company_name_from_query("株式会社B") == "株式会社B"


def test_looks_legacy_query_sample() -> None:
    assert _looks_legacy_query_sample(["株式会社A official site 大阪府"]) is True
    assert _looks_legacy_query_sample(["株式会社A", "株式会社B"]) is False


def test_estimate_migrated_query_resume_index() -> None:
    old_processed = [
        "株式会社A official site 大阪府",
        "株式会社B official site 大阪府",
    ]
    new_queries = ["株式会社A", "株式会社B", "株式会社C"]
    assert (
        _estimate_migrated_query_resume_index(
            old_processed_queries=old_processed,
            new_queries=new_queries,
        )
        == 2
    )


def test_is_simple_mode_from_request_mode() -> None:
    request = {"mode": "simple", "site": {"simple_mode": False}}
    assert _is_simple_mode(request) is True


def test_is_simple_mode_from_site_flag() -> None:
    request = {"site": {"simple_mode": True}}
    assert _is_simple_mode(request) is True
