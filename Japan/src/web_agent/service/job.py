from __future__ import annotations

import asyncio
import contextlib
import json
import os
import traceback
from pathlib import Path
from typing import Any, Iterator

from hojin_agent.pipeline import export_companies as hojin_export_companies
from site_agent.config import PipelineSettings
from site_agent.pipeline import reset_log_sink as reset_site_log_sink
from site_agent.pipeline import run_pipeline as run_site_pipeline
from site_agent.pipeline import set_log_sink as set_site_log_sink

from .logging_utils import _append_job_log
from .prefecture import normalize_prefecture_display
from ..runner import python_module_args, run_subprocess
from ..store import (
    JobPaths,
    build_job_paths,
    new_job_id,
    normalize_job_suffix,
    read_json,
    utc_now_iso,
    write_json_atomic,
)

TERMINAL_STATUSES = {"succeeded", "failed", "canceled"}
_CITY_JOB_DIR_PARTS = ["市", "区", "町", "村"]
_PREF_JOB_DIR_PARTS = ["都", "道", "府", "县"]
_GMAP_CONCURRENCY_FIXED = 16
_SITE_CONCURRENCY_FIXED = 16
_LLM_CONCURRENCY_FIXED = 16
_DEFAULT_LLM_BASE_URL = "https://api.gpteamservices.com/v1"
_SITE_REQUIRED_FIELD_ORDER = ("company_name", "representative", "email", "phone")
_SITE_DEFAULT_REQUIRED_FIELDS = ["company_name", "representative", "email"]
_LEGACY_QUERY_MARKER = "official site"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return None


def _coerce_str_strip(value: Any) -> str | None:
    text = _coerce_str(value)
    if text is None:
        return None
    text = text.strip()
    return text if text else None


def _as_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_positive_limit(value: Any) -> int | None:
    parsed = _as_int(value, default=None)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    if value is None:
        return default
    return bool(value)


def _normalize_site_required_fields(fields: Any, *, require_phone: bool = False) -> list[str]:
    if isinstance(fields, list):
        raw = {str(item).strip() for item in fields if isinstance(item, str) and str(item).strip()}
    else:
        raw = set()
    normalized = [name for name in _SITE_REQUIRED_FIELD_ORDER if name in raw]
    if not require_phone:
        normalized = [name for name in normalized if name != "phone"]
    return normalized or list(_SITE_DEFAULT_REQUIRED_FIELDS)


def _is_simple_mode(request: dict[str, Any]) -> bool:
    mode = _coerce_str_strip(request.get("mode"))
    if isinstance(mode, str) and mode.lower() == "simple":
        return True
    site_cfg = _as_dict(request.get("site"))
    return _as_bool(site_cfg.get("simple_mode"), False)


def _job_group_dir(jobs_dir: Path, group: str | None) -> Path:
    if group == "prefecture":
        return jobs_dir.joinpath(*_PREF_JOB_DIR_PARTS)
    if group == "city":
        return jobs_dir.joinpath(*_CITY_JOB_DIR_PARTS)
    return jobs_dir


def _infer_job_group(payload: dict[str, Any]) -> str | None:
    group = _coerce_str_strip(payload.get("job_group"))
    if group in ("prefecture", "city"):
        return group
    registry = _as_dict(payload.get("registry"))
    if _coerce_str_strip(registry.get("city")):
        return "city"
    if _coerce_str_strip(registry.get("prefecture")):
        return "prefecture"
    location = _coerce_str_strip(registry.get("location"))
    if location and location.endswith(("都", "道", "府", "县")):
        return "prefecture"
    return None


def _job_suffix_from_payload(payload: dict[str, Any]) -> str:
    registry = _as_dict(payload.get("registry"))
    city = _coerce_str_strip(registry.get("city"))
    if city:
        return normalize_job_suffix(city)
    pref = _coerce_str_strip(registry.get("prefecture"))
    if pref:
        return normalize_job_suffix(pref)
    location = _coerce_str_strip(registry.get("location"))
    if location and location.endswith(("都", "道", "府", "县")):
        return normalize_job_suffix(location)
    return ""


def _resolve_registry_location(registry: dict[str, Any]) -> str:
    pref = _coerce_str_strip(registry.get("prefecture"))
    location = _coerce_str_strip(registry.get("location"))
    for candidate in (pref, location):
        normalized = normalize_prefecture_display(candidate)
        if normalized:
            return normalized
    if location == "日本":
        return "全国"
    return location or "全国"


def _extract_secrets(payload: dict[str, Any]) -> dict[str, str]:
    secrets: dict[str, str] = {}
    for key in (
        "llm_api_key",
        "snov_extension_selector",
        "snov_extension_token",
        "snov_extension_fingerprint",
    ):
        value = payload.pop(key, None)
        text = _coerce_str_strip(value)
        if text:
            secrets[key] = text
    return secrets


def _read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if not path.exists():
        return items
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
    return items


def _count_jsonl_records(path: Path) -> int:
    return _count_nonempty_lines(path)


def _count_nonempty_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _write_jsonl_delta(input_path: Path, output_path: Path, *, start_index: int) -> int:
    if not input_path.exists():
        return 0
    start = max(0, int(start_index))
    written = 0
    index = 0
    with input_path.open("r", encoding="utf-8", errors="replace") as src, output_path.open(
        "w", encoding="utf-8"
    ) as dst:
        for line in src:
            if not line.strip():
                continue
            if index >= start:
                text = line if line.endswith("\n") else line + "\n"
                dst.write(text)
                written += 1
            index += 1
    return written


def _safe_non_negative(value: Any) -> int:
    parsed = _as_int(value, default=0) or 0
    return max(0, int(parsed))


def _site_processed_count(site_dir: Path) -> int:
    checkpoint = read_json(site_dir / "checkpoint.json")
    if not isinstance(checkpoint, dict):
        return 0
    counters = _as_dict(checkpoint.get("counters"))
    return _safe_non_negative(counters.get("processed"))


def _parallel_sync_checkpoint_path(gmap_dir: Path) -> Path:
    return gmap_dir / "parallel_site_sync.json"


def _load_parallel_sync_count(gmap_dir: Path) -> int:
    payload = read_json(_parallel_sync_checkpoint_path(gmap_dir))
    if not isinstance(payload, dict):
        return 0
    return _safe_non_negative(payload.get("synced_gmap_count"))


def _write_parallel_sync_count(gmap_dir: Path, count: int) -> None:
    write_json_atomic(
        _parallel_sync_checkpoint_path(gmap_dir),
        {
            "updated_at": utc_now_iso(),
            "synced_gmap_count": int(max(0, count)),
        },
    )


def _find_latest_named_file(root: Path, file_name: str) -> Path | None:
    if not root.exists():
        return None
    candidates = sorted(
        root.rglob(file_name),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _iter_company_names(path: Path, *, limit: int | None = None) -> Iterator[str]:
    if not path.exists():
        return
    seen: set[str] = set()
    cap = _normalize_positive_limit(limit)
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(item, dict):
                continue
            name = _coerce_str_strip(item.get("name")) or _coerce_str_strip(item.get("company_name"))
            if not name or name in seen:
                continue
            seen.add(name)
            yield name
            count += 1
            if cap is not None and count >= cap:
                return


def _collect_company_names(path: Path, *, limit: int | None = None) -> list[str]:
    return list(_iter_company_names(path, limit=limit))


def _build_gmap_queries(names: list[str], *, limit: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    cap = _normalize_positive_limit(limit)
    for name in names:
        merged = _coerce_str_strip(name)
        if not merged or merged in seen:
            continue
        seen.add(merged)
        out.append(merged)
        if cap is not None and len(out) >= cap:
            return out
    return out


def _write_queries_file(path: Path, queries: list[str]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for query in queries:
            f.write(query + "\n")


def _read_query_prefix(path: Path, *, limit: int) -> list[str]:
    if limit <= 0 or not path.exists():
        return []
    out: list[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            out.append(text)
            if len(out) >= limit:
                break
    return out


def _extract_company_name_from_query(query: str) -> str:
    text = _coerce_str_strip(query) or ""
    if not text:
        return ""
    lower = text.lower()
    marker_idx = lower.find(_LEGACY_QUERY_MARKER)
    if marker_idx <= 0:
        return text
    name = text[:marker_idx].strip()
    return name or text


def _looks_legacy_query_sample(sample: list[str]) -> bool:
    return any(_LEGACY_QUERY_MARKER in line.lower() for line in sample if isinstance(line, str))


def _registry_has_more_company_names(path: Path, *, threshold: int) -> bool:
    limit = max(1, int(max(0, threshold)) + 1)
    count = 0
    for _ in _iter_company_names(path, limit=limit):
        count += 1
        if count > threshold:
            return True
    return False


def _estimate_migrated_query_resume_index(
    *,
    old_processed_queries: list[str],
    new_queries: list[str],
) -> int:
    if not old_processed_queries or not new_queries:
        return 0
    processed_names: set[str] = set()
    for line in old_processed_queries:
        name = _extract_company_name_from_query(line)
        if name:
            processed_names.add(name)
    if not processed_names:
        return 0
    index = 0
    for query in new_queries:
        if query in processed_names:
            index += 1
            continue
        break
    return min(index, len(new_queries))




class JobService:
    def __init__(self, jobs_dir: Path, keywords_dir: Path | None = None) -> None:
        self.jobs_dir = jobs_dir
        self.keywords_dir = keywords_dir or (jobs_dir.parent / "web_keywords")
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._secrets: dict[str, dict[str, str]] = {}
        self._lock = asyncio.Lock()

    def get_job_paths(self, job_id: str) -> JobPaths:
        return build_job_paths(self.jobs_dir, job_id)

    def load_job(self, job_id: str) -> dict[str, Any] | None:
        paths = self.get_job_paths(job_id)
        return read_json(paths.job_json)

    async def create_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = dict(payload)
        if _is_simple_mode(req):
            req["parallel_pipeline"] = True
            req["resume_mode"] = None
        suffix = _job_suffix_from_payload(req)
        job_id = new_job_id(suffix or None)
        job_group = _infer_job_group(req)
        job_base_dir = _job_group_dir(self.jobs_dir, job_group)
        paths = build_job_paths(job_base_dir, job_id)
        secrets = _extract_secrets(req)
        if secrets:
            self._secrets[job_id] = secrets
        site_req = _as_dict(req.get("site"))
        if _is_simple_mode(req):
            site_req["resume_mode"] = None
        site_req["run_dir"] = str(paths.site_dir)
        req["site"] = site_req
        require_phone = _as_bool(site_req.get("require_phone"), _as_bool(req.get("require_phone"), False))
        req["fields"] = _normalize_site_required_fields(req.get("fields"), require_phone=require_phone)
        req["has_llm_api_key"] = bool(self._secrets.get(job_id, {}).get("llm_api_key"))
        job = {
            "id": job_id,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "status": "queued",
            "stage": None,
            "request": req,
            "paths": {
                "job_dir": str(paths.job_dir),
                "log": str(paths.log_path),
                "gmap_dir": str(paths.gmap_dir),
                "registry_dir": str(paths.registry_dir),
                "site_dir": str(paths.site_dir),
                "input_path": str(paths.input_path),
            },
            "result": None,
            "error": None,
        }
        write_json_atomic(paths.job_json, job)
        paths.log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.log_path.write_text("", encoding="utf-8-sig")
        await self.start_job(job_id)
        return job

    async def resume_job(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        paths = self.get_job_paths(job_id)
        req = dict(payload)
        secrets = _extract_secrets(req)
        if secrets:
            self._secrets[job_id] = secrets
        existing = _as_dict(read_json(paths.job_json))
        existing_req = _as_dict(existing.get("request"))
        merged: dict[str, Any] = dict(existing_req)
        merged.update(req)
        if _is_simple_mode(merged):
            merged["parallel_pipeline"] = True
            merged["resume_mode"] = None
        merged["resume"] = True
        site_req = _as_dict(existing_req.get("site"))
        site_req.update(_as_dict(req.get("site")))
        if _is_simple_mode(merged):
            site_req["resume_mode"] = None
        site_req["run_dir"] = str(paths.site_dir)
        site_req["resume"] = True
        merged["site"] = site_req
        require_phone = _as_bool(site_req.get("require_phone"), _as_bool(merged.get("require_phone"), False))
        merged["fields"] = _normalize_site_required_fields(merged.get("fields"), require_phone=require_phone)
        merged["has_llm_api_key"] = bool(self._secrets.get(job_id, {}).get("llm_api_key"))
        existing.update(
            {
                "updated_at": utc_now_iso(),
                "status": "queued",
                "stage": None,
                "request": merged,
                "error": None,
            }
        )
        write_json_atomic(paths.job_json, existing)
        _append_job_log(paths.log_path, "[任务] 续跑任务")
        await self.start_job(job_id)
        return existing

    async def start_job(self, job_id: str) -> None:
        async with self._lock:
            task = self._tasks.get(job_id)
            if task and not task.done():
                return
            new_task = asyncio.create_task(self._run_job(job_id), name=f"web-job-{job_id}")
            self._tasks[job_id] = new_task
            new_task.add_done_callback(lambda _: self._tasks.pop(job_id, None))

    async def cancel_job(self, job_id: str) -> bool:
        task = self._tasks.get(job_id)
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        paths = self.get_job_paths(job_id)
        job = _as_dict(read_json(paths.job_json))
        if not job:
            return False
        job["status"] = "canceled"
        job["updated_at"] = utc_now_iso()
        write_json_atomic(paths.job_json, job)
        _append_job_log(paths.log_path, "[任务] 已取消")
        return True

    async def _run_job(self, job_id: str) -> None:
        paths = self.get_job_paths(job_id)
        job = _as_dict(read_json(paths.job_json))
        if not job:
            return
        request = _as_dict(job.get("request"))
        secrets = self._secrets.get(job_id, {})
        try:
            await self._set_job_state(job_id, status="running", stage="registry", error=None)
            _append_job_log(paths.log_path, f"[任务] 开始执行：{job_id}")
            registry_output = paths.registry_dir / "output.jsonl"
            if _as_bool(request.get("resume"), False) and registry_output.exists():
                _append_job_log(paths.log_path, f"[法人] 续跑复用名录：{registry_output}")
            else:
                registry_output = await self._run_registry_stage(paths, request)
            result_payload: dict[str, Any] = {"registry_output": str(registry_output)}
            if _as_bool(request.get("registry_enrich"), False):
                parallel_enabled = _as_bool(request.get("parallel_pipeline"), True)
                gmap_output: Path | None
                site_output: Path | None
                if parallel_enabled:
                    await self._set_job_state(job_id, stage="gmap")
                    gmap_output, site_output = await self._run_gmap_and_site_parallel_stage(
                        job_id,
                        paths,
                        request,
                        registry_output,
                        secrets,
                    )
                else:
                    site_input = registry_output
                    await self._set_job_state(job_id, stage="gmap")
                    gmap_output = await self._run_gmap_stage(paths, request, registry_output)
                    if gmap_output is not None:
                        site_input = gmap_output
                    await self._set_job_state(job_id, stage="site")
                    site_output = await self._run_site_stage(paths, request, site_input, secrets)
                if gmap_output is not None:
                    result_payload["gmap_output"] = str(gmap_output)
                if site_output is not None:
                    result_payload["site_output"] = str(site_output)
                    result_payload["site_counts"] = {
                        "total": _count_jsonl_records(site_output),
                        "success": _count_jsonl_records(paths.site_dir / "output.success.jsonl"),
                        "partial": _count_jsonl_records(paths.site_dir / "output.partial.jsonl"),
                        "failed": _count_jsonl_records(paths.site_dir / "output.failed.jsonl"),
                    }
            await self._set_job_state(
                job_id,
                status="succeeded",
                stage=None,
                result=result_payload,
                error=None,
            )
            _append_job_log(paths.log_path, "[任务] 完成")
        except asyncio.CancelledError:
            await self._set_job_state(job_id, status="canceled", stage=None, error="canceled")
            _append_job_log(paths.log_path, "[任务] 已取消")
            raise
        except Exception as exc:
            _append_job_log(paths.log_path, f"[任务] 失败：{exc}")
            _append_job_log(paths.log_path, traceback.format_exc())
            await self._set_job_state(job_id, status="failed", stage=None, error=str(exc))


    async def _set_job_state(self, job_id: str, **updates: Any) -> None:
        async with self._lock:
            paths = self.get_job_paths(job_id)
            job = _as_dict(read_json(paths.job_json))
            if not job:
                return
            job.update(updates)
            job["updated_at"] = utc_now_iso()
            write_json_atomic(paths.job_json, job)

    async def _run_registry_stage(self, paths: JobPaths, request: dict[str, Any]) -> Path:
        registry = _as_dict(request.get("registry"))
        location = _resolve_registry_location(registry)
        city = _coerce_str_strip(registry.get("city"))
        max_records = _as_int(registry.get("max_records"), default=None)
        max_records = None if max_records is not None and max_records <= 0 else max_records
        _append_job_log(paths.log_path, f"[法人] 开始导出公司名录：{location}")
        meta = await asyncio.to_thread(
            hojin_export_companies,
            location=location,
            city_filter=city,
            output_dir=paths.registry_dir,
            cache_dir=Path("output") / "hojin_cache",
            company_only=_as_bool(registry.get("company_only"), True),
            active_only=_as_bool(registry.get("active_only"), True),
            latest_only=_as_bool(registry.get("latest_only"), True),
            max_records=max_records,
            log_sink=lambda line: _append_job_log(paths.log_path, line),
        )
        output_jsonl = None
        if isinstance(meta, dict):
            output = _as_dict(meta.get("output"))
            output_path = _coerce_str_strip(output.get("jsonl"))
            if output_path:
                output_jsonl = Path(output_path)
        if output_jsonl is None or not output_jsonl.exists():
            output_jsonl = _find_latest_named_file(paths.registry_dir, "output.jsonl")
        if output_jsonl is None or not output_jsonl.exists():
            raise RuntimeError("名录导出结果缺失：output.jsonl")
        if output_jsonl != paths.registry_dir / "output.jsonl":
            (paths.registry_dir / "output.jsonl").write_text(
                output_jsonl.read_text(encoding="utf-8", errors="replace"),
                encoding="utf-8",
            )
            output_jsonl = paths.registry_dir / "output.jsonl"
        _append_job_log(paths.log_path, f"[法人] 导出完成：{output_jsonl}")
        return output_jsonl

    def _prepare_gmap_stage(
        self,
        paths: JobPaths,
        request: dict[str, Any],
        registry_output: Path,
    ) -> tuple[list[str], Path] | None:
        gmap_cfg = _as_dict(request.get("gmap"))
        job_resume = _as_bool(request.get("resume"), False)
        resume_enabled = _as_bool(gmap_cfg.get("resume"), True)
        company_name_limit = _normalize_positive_limit(gmap_cfg.get("company_name_limit"))
        if company_name_limit is None:
            company_name_limit = _normalize_positive_limit(gmap_cfg.get("max_company_names"))
        max_queries = _normalize_positive_limit(gmap_cfg.get("max_queries"))

        paths.gmap_dir.mkdir(parents=True, exist_ok=True)
        query_file = paths.gmap_dir / "queries.txt"
        meta_file = paths.gmap_dir / "queries.meta.json"
        checkpoint_file = paths.gmap_dir / "query_checkpoint.json"
        query_count = 0

        meta = _as_dict(read_json(meta_file))
        meta_version = _as_int(meta.get("format_version"), default=None)
        meta_count = _as_int(meta.get("query_count"), default=None)
        meta_ok = meta_version == 2 and isinstance(meta_count, int) and meta_count > 0
        limits_configured = company_name_limit is not None or max_queries is not None
        meta_source = (_coerce_str_strip(meta.get("source")) or "").lower()

        should_regenerate_queries = True
        should_reuse_existing_queries = (
            job_resume and resume_enabled and query_file.exists() and not limits_configured
        )
        migrate_reason: str | None = None
        old_processed_queries: list[str] = []
        if should_reuse_existing_queries and meta_ok:
            query_count = int(meta_count)
            should_regenerate_queries = False
            _append_job_log(paths.log_path, f"[谷歌] 续跑复用 queries.txt：queries={query_count}")
        elif should_reuse_existing_queries:
            recovered_count = _count_nonempty_lines(query_file)
            if recovered_count > 0:
                query_count = recovered_count
                should_regenerate_queries = False
                _append_job_log(
                    paths.log_path,
                    f"[谷歌] 续跑复用旧 queries.txt（兼容模式）：queries={query_count}",
                )
                write_json_atomic(
                    meta_file,
                    {
                        "format_version": 2,
                        "format": "company_name",
                        "query_count": query_count,
                        "generated_at": utc_now_iso(),
                        "source": "recovered_from_existing_queries",
                    },
                )
            else:
                _append_job_log(paths.log_path, "[谷歌] 旧 queries.txt 为空，将按当前规则重建")

        if should_reuse_existing_queries and not should_regenerate_queries:
            sample = _read_query_prefix(query_file, limit=20)
            legacy_format = _looks_legacy_query_sample(sample)
            recovered_limited = (
                meta_source == "recovered_from_existing_queries"
                and query_count > 0
                and _registry_has_more_company_names(registry_output, threshold=query_count)
            )
            if legacy_format:
                migrate_reason = "legacy_query_format"
            elif recovered_limited:
                migrate_reason = "legacy_query_cap"
            if migrate_reason:
                old_checkpoint = _as_dict(read_json(checkpoint_file))
                old_next_query_index = min(
                    query_count,
                    _safe_non_negative(old_checkpoint.get("next_query_index")),
                )
                old_processed_queries = _read_query_prefix(
                    query_file,
                    limit=old_next_query_index,
                )
                should_regenerate_queries = True
                reason_text = "旧关键词格式" if migrate_reason == "legacy_query_format" else "旧关键词数量受限"
                _append_job_log(
                    paths.log_path,
                    f"[谷歌] 检测到{reason_text}，自动迁移为“仅公司名”全量关键词",
                )

        if should_regenerate_queries:
            # Regenerate query file and reset query checkpoint, so resume index matches current queries.
            if checkpoint_file.exists():
                with contextlib.suppress(Exception):
                    checkpoint_file.unlink()
            if migrate_reason:
                names = _collect_company_names(registry_output, limit=company_name_limit)
                queries = _build_gmap_queries(names, limit=max_queries)
                _write_queries_file(query_file, queries)
                query_count = len(queries)
                migrated_index = _estimate_migrated_query_resume_index(
                    old_processed_queries=old_processed_queries,
                    new_queries=queries,
                )
                write_json_atomic(
                    checkpoint_file,
                    {
                        "updated_at": utc_now_iso(),
                        "next_query_index": migrated_index,
                        "total_queries": query_count,
                        "source": "migrated_legacy_queries",
                    },
                )
                _append_job_log(
                    paths.log_path,
                    f"[谷歌] 关键词迁移完成：{len(old_processed_queries)} 已处理 -> 新进度 {migrated_index}/{query_count}",
                )
            else:
                with query_file.open("w", encoding="utf-8") as f:
                    for name in _iter_company_names(registry_output, limit=company_name_limit):
                        f.write(name + "\n")
                        query_count += 1
                        if max_queries is not None and query_count >= max_queries:
                            break
            write_json_atomic(
                meta_file,
                {
                    "format_version": 2,
                    "format": "company_name",
                    "query_count": query_count,
                    "generated_at": utc_now_iso(),
                    "source": "migrated_legacy_queries" if migrate_reason else "company_name_queries",
                },
            )

        if query_count <= 0:
            _append_job_log(paths.log_path, "[谷歌] 无可用查询，跳过官网搜索")
            return None

        cap_text = str(max_queries) if max_queries is not None else "none"
        _append_job_log(
            paths.log_path,
            f"[谷歌] 启动官网搜索：companies={query_count} query_cap={cap_text} queries={query_count}",
        )

        args = python_module_args(
            "gmap_agent",
            [
                "--query-file",
                str(query_file),
                "--run-dir",
                str(paths.gmap_dir),
                "--output-dir",
                str(paths.gmap_dir),
                "--concurrency",
                str(_GMAP_CONCURRENCY_FIXED),
            ],
        )
        if _is_simple_mode(request):
            args.append("--phone-enrich")
        if _as_bool(gmap_cfg.get("resume"), True):
            args.append("--resume")
        if _coerce_str_strip(gmap_cfg.get("bbox")):
            args.extend(["--bbox", str(gmap_cfg["bbox"])])
        if _coerce_str_strip(gmap_cfg.get("search_pb")):
            args.extend(["--search-pb", str(gmap_cfg["search_pb"])])
        if _coerce_str_strip(gmap_cfg.get("cookie")):
            args.extend(["--cookie", str(gmap_cfg["cookie"])])
        if _coerce_str_strip(gmap_cfg.get("proxy")):
            args.extend(["--proxy", str(gmap_cfg["proxy"])])
        return args, paths.gmap_dir / "places_with_websites.jsonl"

    def _resolve_gmap_output(self, paths: JobPaths, expected_output: Path) -> Path | None:
        if expected_output.exists():
            return expected_output
        return _find_latest_named_file(paths.gmap_dir, "places_with_websites.jsonl")

    async def _run_gmap_stage(
        self,
        paths: JobPaths,
        request: dict[str, Any],
        registry_output: Path,
    ) -> Path | None:
        prepared = self._prepare_gmap_stage(paths, request, registry_output)
        if prepared is None:
            return None
        args, expected_output = prepared
        code = await run_subprocess(args, paths.log_path, "gmap_agent", cwd=Path.cwd())
        if code != 0:
            raise RuntimeError(f"gmap_agent exited with code {code}")
        gmap_output = self._resolve_gmap_output(paths, expected_output)
        if gmap_output is None or not gmap_output.exists():
            _append_job_log(paths.log_path, "[谷歌] 未发现 places_with_websites.jsonl")
            return None
        paths.input_path.write_text(gmap_output.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
        _append_job_log(paths.log_path, f"[谷歌] 官网搜索完成：{gmap_output}")
        return gmap_output

    async def _run_gmap_and_site_parallel_stage(
        self,
        job_id: str,
        paths: JobPaths,
        request: dict[str, Any],
        registry_output: Path,
        secrets: dict[str, str],
    ) -> tuple[Path | None, Path | None]:
        prepared = self._prepare_gmap_stage(paths, request, registry_output)
        site_request: dict[str, Any] = dict(request)
        site_cfg = dict(_as_dict(site_request.get("site")))
        site_cfg["resume"] = True
        site_request["site"] = site_cfg
        site_request["resume"] = True

        if prepared is None:
            await self._set_job_state(job_id, stage="site")
            site_output = await self._run_site_stage(paths, site_request, registry_output, secrets)
            return None, site_output

        args, expected_output = prepared
        _append_job_log(paths.log_path, "[并行] 启用并行流水线：Google官网发现 与 官网抓取并行执行")
        gmap_task = asyncio.create_task(
            run_subprocess(args, paths.log_path, "gmap_agent", cwd=Path.cwd()),
            name=f"gmap-{job_id}",
        )
        processed_count = _load_parallel_sync_count(paths.gmap_dir)
        if processed_count <= 0:
            processed_count = _site_processed_count(paths.site_dir)
        site_output: Path | None = None
        gmap_output: Path | None = None

        try:
            while True:
                gmap_output = self._resolve_gmap_output(paths, expected_output)
                current_count = (
                    _count_jsonl_records(gmap_output)
                    if gmap_output is not None and gmap_output.exists()
                    else 0
                )
                if processed_count > current_count:
                    processed_count = current_count
                    _write_parallel_sync_count(paths.gmap_dir, processed_count)
                if current_count > processed_count and gmap_output is not None:
                    delta = current_count - processed_count
                    delta_written = _write_jsonl_delta(
                        gmap_output,
                        paths.input_path,
                        start_index=processed_count,
                    )
                    if delta_written <= 0:
                        processed_count = current_count
                        _write_parallel_sync_count(paths.gmap_dir, processed_count)
                        continue
                    _append_job_log(
                        paths.log_path,
                        f"[并行] 官网新增 {delta_written} 条（累计 {current_count}），触发站点抓取",
                    )
                    await self._set_job_state(job_id, stage="site")
                    site_output = await self._run_site_stage(paths, site_request, paths.input_path, secrets)
                    processed_count = current_count
                    _write_parallel_sync_count(paths.gmap_dir, processed_count)
                    await self._set_job_state(job_id, stage="gmap")

                if gmap_task.done():
                    code = await gmap_task
                    if code != 0:
                        raise RuntimeError(f"gmap_agent exited with code {code}")
                    break
                await asyncio.sleep(2.0)
        except Exception:
            if not gmap_task.done():
                gmap_task.cancel()
                with contextlib.suppress(Exception):
                    await gmap_task
            raise

        gmap_output = self._resolve_gmap_output(paths, expected_output)
        if gmap_output is None or not gmap_output.exists():
            _append_job_log(paths.log_path, "[谷歌] 未发现 places_with_websites.jsonl")
            if site_output is None:
                await self._set_job_state(job_id, stage="site")
                site_output = await self._run_site_stage(paths, site_request, registry_output, secrets)
            return None, site_output

        final_count = _count_jsonl_records(gmap_output)
        if final_count > processed_count:
            delta = final_count - processed_count
            delta_written = _write_jsonl_delta(
                gmap_output,
                paths.input_path,
                start_index=processed_count,
            )
            if delta_written <= 0:
                processed_count = final_count
                _write_parallel_sync_count(paths.gmap_dir, processed_count)
                _append_job_log(paths.log_path, f"[谷歌] 官网搜索完成：{gmap_output}")
                return gmap_output, site_output
            _append_job_log(
                paths.log_path,
                f"[并行] 收尾新增 {delta_written} 条（累计 {final_count}），执行最终站点抓取",
            )
            await self._set_job_state(job_id, stage="site")
            site_output = await self._run_site_stage(paths, site_request, paths.input_path, secrets)
            processed_count = final_count
            _write_parallel_sync_count(paths.gmap_dir, processed_count)
        elif site_output is None and final_count > 0:
            delta_written = _write_jsonl_delta(
                gmap_output,
                paths.input_path,
                start_index=processed_count,
            )
            if delta_written <= 0:
                _append_job_log(paths.log_path, f"[谷歌] 官网搜索完成：{gmap_output}")
                return gmap_output, site_output
            await self._set_job_state(job_id, stage="site")
            site_output = await self._run_site_stage(paths, site_request, paths.input_path, secrets)
            processed_count = final_count
            _write_parallel_sync_count(paths.gmap_dir, processed_count)

        _append_job_log(paths.log_path, f"[谷歌] 官网搜索完成：{gmap_output}")
        return gmap_output, site_output


    async def _run_site_stage(
        self,
        paths: JobPaths,
        request: dict[str, Any],
        input_path: Path,
        secrets: dict[str, str],
    ) -> Path:
        if not input_path.exists():
            raise RuntimeError(f"site input not found: {input_path}")
        site_cfg = _as_dict(request.get("site"))
        simple_mode = _as_bool(site_cfg.get("simple_mode"), _is_simple_mode(request))
        resolved_resume_mode = (
            None
            if simple_mode
            else (
                _coerce_str_strip(site_cfg.get("resume_mode"))
                or _coerce_str_strip(request.get("resume_mode"))
            )
        )
        require_phone = _as_bool(
            site_cfg.get("require_phone"),
            _as_bool(request.get("require_phone"), False),
        )
        fields = _normalize_site_required_fields(request.get("fields"), require_phone=require_phone)
        llm_api_key = _coerce_str_strip(secrets.get("llm_api_key")) or _coerce_str_strip(request.get("llm_api_key"))
        use_llm = _as_bool(
            site_cfg.get("use_llm"),
            _as_bool(request.get("use_llm"), False),
        )
        if use_llm and not llm_api_key:
            raise RuntimeError("已启用 LLM 模式但缺少 llm_api_key")
        llm_api_key = llm_api_key or ""

        snov_extension_selector = _coerce_str_strip(secrets.get("snov_extension_selector"))
        snov_extension_token = _coerce_str_strip(secrets.get("snov_extension_token"))
        snov_extension_fingerprint = _coerce_str_strip(secrets.get("snov_extension_fingerprint"))
        snov_extension_cdp_host = (
            _coerce_str_strip(request.get("snov_extension_cdp_host"))
            or _coerce_str_strip(os.environ.get("SNOV_EXTENSION_CDP_HOST"))
            or _coerce_str_strip(os.environ.get("SNOV_CDP_HOST"))
        )
        snov_extension_cdp_port = _as_int(request.get("snov_extension_cdp_port"), default=None)
        if snov_extension_cdp_port is None:
            snov_extension_cdp_port = _as_int(os.environ.get("SNOV_EXTENSION_CDP_PORT"), default=None)
        if snov_extension_cdp_port is None:
            snov_extension_cdp_port = _as_int(os.environ.get("SNOV_CDP_PORT"), default=None)
        if snov_extension_cdp_port is not None and not snov_extension_cdp_host:
            snov_extension_cdp_host = "127.0.0.1"
        # Keep Snov enabled by default so email enrichment always attempts extension prefetch.
        snov_extension_only = _as_bool(request.get("snov_extension_only"), True)

        settings = PipelineSettings(
            input_path=input_path,
            output_base_dir=paths.site_dir.parent,
            run_dir=paths.site_dir,
            concurrency=_as_int(site_cfg.get("concurrency"), default=_SITE_CONCURRENCY_FIXED) or _SITE_CONCURRENCY_FIXED,
            llm_concurrency=_as_int(site_cfg.get("llm_concurrency"), default=_LLM_CONCURRENCY_FIXED) or _LLM_CONCURRENCY_FIXED,
            max_pages=_as_int(site_cfg.get("max_pages"), default=10) or 10,
            max_rounds=_as_int(site_cfg.get("max_rounds"), default=3) or 3,
            max_sites=_as_int(site_cfg.get("max_sites"), default=None),
            page_timeout=_as_int(site_cfg.get("page_timeout"), default=20000) or 20000,
            max_content_chars=_as_int(site_cfg.get("max_content_chars"), default=20000) or 20000,
            save_pages=_as_bool(site_cfg.get("save_pages"), False),
            resume=_as_bool(site_cfg.get("resume"), _as_bool(request.get("resume"), False)),
            llm_api_key=llm_api_key,
            llm_base_url=(
                _coerce_str_strip(request.get("llm_base_url"))
                or _coerce_str_strip(os.environ.get("LLM_BASE_URL"))
                or _DEFAULT_LLM_BASE_URL
            ),
            llm_model=_coerce_str_strip(request.get("llm_model")) or "gpt-5.1-codex-mini",
            llm_temperature=float(site_cfg.get("llm_temperature") or request.get("llm_temperature") or 0.0),
            llm_max_output_tokens=_as_int(request.get("llm_max_output_tokens"), default=1200) or 1200,
            llm_reasoning_effort=_coerce_str_strip(request.get("llm_reasoning_effort")),
            use_llm=use_llm,
            crawler_reset_every=_as_int(site_cfg.get("crawler_reset_every"), default=0) or 0,
            site_timeout_seconds=float(site_cfg["site_timeout_seconds"])
            if site_cfg.get("site_timeout_seconds") is not None
            else None,
            snov_extension_selector=snov_extension_selector,
            snov_extension_token=snov_extension_token,
            snov_extension_fingerprint=snov_extension_fingerprint,
            snov_extension_cdp_host=snov_extension_cdp_host,
            snov_extension_cdp_port=snov_extension_cdp_port,
            snov_extension_only=snov_extension_only,
            skip_email=_as_bool(site_cfg.get("skip_email"), False),
            required_fields=fields,
            keyword=_coerce_str_strip(site_cfg.get("keyword")),
            keyword_filter_enabled=use_llm
            and _as_bool(site_cfg.get("keyword_filter_enabled"), False),
            keyword_min_confidence=float(site_cfg.get("keyword_min_confidence") or 0.6),
            email_max_per_domain=_as_int(site_cfg.get("email_max_per_domain"), default=0) or 0,
            email_details_limit=_as_int(site_cfg.get("email_details_limit"), default=80) or 80,
            pdf_max_pages=_as_int(site_cfg.get("pdf_max_pages"), default=4) or 4,
            resume_mode=resolved_resume_mode,
            firecrawl_keys_path=Path(_coerce_str_strip(site_cfg.get("firecrawl_keys_path")))
            if _coerce_str_strip(site_cfg.get("firecrawl_keys_path"))
            else None,
            firecrawl_base_url=_coerce_str_strip(site_cfg.get("firecrawl_base_url")),
            firecrawl_extract_enabled=use_llm
            and _as_bool(site_cfg.get("firecrawl_extract_enabled"), False),
            firecrawl_extract_max_urls=_as_int(site_cfg.get("firecrawl_extract_max_urls"), default=6) or 6,
            firecrawl_key_per_limit=_as_int(site_cfg.get("firecrawl_key_per_limit"), default=2) or 2,
            firecrawl_key_wait_seconds=_as_int(site_cfg.get("firecrawl_key_wait_seconds"), default=120) or 120,
            simple_mode=simple_mode,
        )
        snov_ready = bool((snov_extension_selector and snov_extension_token) or snov_extension_cdp_port)
        _append_job_log(
            paths.log_path,
            "[官网] Snov 预检配置："
            f"only={int(bool(snov_extension_only))} "
            f"ready={int(snov_ready)} "
            f"cdp={snov_extension_cdp_host or '-'}:{snov_extension_cdp_port if snov_extension_cdp_port is not None else '-'}",
        )
        _append_job_log(paths.log_path, f"[官网] 启动 site_agent：{input_path}")
        token = set_site_log_sink(lambda line: _append_job_log(paths.log_path, line))
        try:
            await run_site_pipeline(settings)
        finally:
            reset_site_log_sink(token)
        output_jsonl = settings.run_dir / "output.jsonl"
        if not output_jsonl.exists():
            raise RuntimeError("site_agent 未生成 output.jsonl")
        _append_job_log(paths.log_path, f"[官网] 抓取完成：{output_jsonl}")
        return output_jsonl
