from __future__ import annotations

import json
from pathlib import Path

from site_agent.pipeline.fields import _normalize_required_fields
from site_agent.pipeline.runtime import _apply_resume_mode_reopen


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_default_required_fields_excludes_phone() -> None:
    assert _normalize_required_fields(None) == ["company_name", "email", "representative"]


def test_partial_resume_mode_reopens_partial_done(tmp_path: Path) -> None:
    run_dir = tmp_path / "site"
    _write_jsonl(
        run_dir / "output.partial.jsonl",
        [
            {"website": "https://a.example.com"},
            {"website": "https://b.example.com"},
        ],
    )
    done = {"a.example.com", "b.example.com", "c.example.com"}
    _apply_resume_mode_reopen(run_dir, done, "partial", True)
    assert done == {"c.example.com"}
