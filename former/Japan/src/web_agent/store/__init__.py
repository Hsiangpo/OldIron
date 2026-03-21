from __future__ import annotations

import json
import os
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_job_id(suffix: str | None = None) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{stamp}_{secrets.token_hex(2)}"
    if suffix:
        return f"{base}_{suffix}"
    return base


_INVALID_JOB_CHARS = r'<>:"/\\|?*'


def normalize_job_suffix(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(rf"[{re.escape(_INVALID_JOB_CHARS)}]", "_", text)
    text = re.sub(r"\s+", "", text)
    text = text.strip(" ._")
    return text[:40]


def default_jobs_dir() -> Path:
    value = (os.environ.get("WEB_JOBS_DIR") or "").strip()
    if value:
        return Path(value)
    return Path("output") / "web_jobs"


@dataclass
class JobPaths:
    job_dir: Path
    job_json: Path
    log_path: Path
    gmap_dir: Path
    registry_dir: Path
    site_dir: Path
    input_path: Path


def iter_job_dirs(jobs_dir: Path) -> list[Path]:
    if not jobs_dir.exists():
        return []
    job_dirs: list[Path] = []
    for job_json in jobs_dir.rglob("job.json"):
        job_dir = job_json.parent
        if job_dir.is_dir():
            job_dirs.append(job_dir)
    return job_dirs


def _resolve_job_dir(jobs_dir: Path, job_id: str) -> Path:
    direct = jobs_dir / job_id
    if direct.exists():
        return direct
    for job_dir in iter_job_dirs(jobs_dir):
        if job_dir.name == job_id:
            return job_dir
    return direct


def build_job_paths(jobs_dir: Path, job_id: str, *, job_dir: Path | None = None) -> JobPaths:
    resolved = job_dir if isinstance(job_dir, Path) else _resolve_job_dir(jobs_dir, job_id)
    job_dir = resolved
    return JobPaths(
        job_dir=job_dir,
        job_json=job_dir / "job.json",
        log_path=job_dir / "job.log",
        gmap_dir=job_dir / "gmap",
        registry_dir=job_dir / "registry",
        site_dir=job_dir / "site",
        input_path=job_dir / "input.jsonl",
    )


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    for _ in range(3):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError:
            time.sleep(0.05)
        except json.JSONDecodeError:
            return None
        except OSError:
            time.sleep(0.05)
    return None


def list_jobs(jobs_dir: Path, limit: int = 30) -> list[dict[str, Any]]:
    if not jobs_dir.exists():
        return []
    items: list[dict[str, Any]] = []
    for job_dir in iter_job_dirs(jobs_dir):
        job = read_json(job_dir / "job.json")
        if not isinstance(job, dict):
            continue
        job["id"] = job.get("id") or job_dir.name
        items.append(job)
    items.sort(key=lambda j: (j.get("created_at") or "", j.get("id") or ""), reverse=True)
    return items[: max(1, limit)]
