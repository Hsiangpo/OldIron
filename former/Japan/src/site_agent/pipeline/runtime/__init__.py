from __future__ import annotations

import asyncio
import random
from pathlib import Path
from typing import Any

from ...config import PipelineSettings, RunStrategy, get_strategy_for_mode
from ...crawler import CrawlerClient
from ...errors import SnovMaskedEmailError
from ...input_loader import load_sites
from ...llm_client import LLMClient
from ...models import ExtractionResult, SiteInput
from ...output_writer import ensure_run_dir, write_checkpoint, write_csv, write_json, write_jsonl
from ...snov_client import SnovClient
from ...utils import canonical_site_key, normalize_url, utc_now_iso
from ..io import _load_checkpoint, _load_jsonl_records, _result_to_record
from ..logging import (
    _humanize_exception,
    _log,
    _print_ts,
    _resolve_input_name,
)
from ..process import _process_site
from ..process.simple_phone import SimplePhoneResolver

def _apply_max_sites(sites: list[SiteInput], max_sites: int | None) -> list[SiteInput]:
    if max_sites is None:
        return sites
    try:
        value = int(max_sites)
    except (TypeError, ValueError):
        return sites
    if value <= 0:
        return sites
    return sites[:value]

class _SimpleCrawlerStub:
    """simple 模式占位 crawler，若被误用可快速暴露问题。"""

    async def fetch_page(self, _url: str) -> Any:
        raise RuntimeError("simple_mode_no_crawl")

    async def fetch_page_rendered(self, _url: str) -> Any:
        raise RuntimeError("simple_mode_no_crawl")

    async def fetch_pages(self, _urls: list[str]) -> list[Any]:
        return []

def _collect_pending_sites(sites: list[SiteInput], done: set[str]) -> list[SiteInput]:
    pending: list[SiteInput] = []
    for site in sites:
        site_key = canonical_site_key(site.website) or normalize_url(site.website) or site.website
        if site_key in done:
            continue
        pending.append(site)
    return pending

async def _run_pending_sites(
    *,
    pending_sites: list[SiteInput],
    crawler: Any,
    llm_client: LLMClient,
    settings: PipelineSettings,
    pages_dir: Path,
    snov_client: SnovClient | None,
    strategy: RunStrategy,
    site_sem: asyncio.Semaphore,
    results: list[ExtractionResult],
    done: set[str],
    counters: dict[str, int],
    checkpoint_path: Path,
    total_sites: int,
    output_jsonl: Path, output_csv: Path,
    success_jsonl: Path, success_csv: Path,
    partial_jsonl: Path, partial_csv: Path,
    failed_jsonl: Path, failed_csv: Path,
    simple_phone_resolver: SimplePhoneResolver | None,
) -> None:
    if not pending_sites:
        return
    max_inflight = max(1, settings.concurrency)
    running: set[asyncio.Task[ExtractionResult]] = set()
    async def _run_site(current_site: SiteInput) -> ExtractionResult:
        async with site_sem:
            return await _bounded_process_site(
                current_site,
                crawler,
                llm_client,
                settings,
                pages_dir,
                snov_client,
                strategy,
                simple_phone_resolver=simple_phone_resolver,
            )
    async def _consume_result(task: asyncio.Task[ExtractionResult]) -> None:
        result = await task
        if result.status == "retry":
            _print_ts(f"[延迟重试] {result.website}", flush=True)
            return
        results.append(result)
        _persist_result_files(
            result,
            output_jsonl=output_jsonl,
            output_csv=output_csv,
            success_jsonl=success_jsonl,
            success_csv=success_csv,
            partial_jsonl=partial_jsonl,
            partial_csv=partial_csv,
            failed_jsonl=failed_jsonl,
            failed_csv=failed_csv,
        )
        done.add(canonical_site_key(result.website) or normalize_url(result.website) or result.website)
        _update_counters(counters, result.status)
        write_checkpoint(
            checkpoint_path,
            {
                "updated_at": utc_now_iso(),
                "total": total_sites,
                "done": sorted(done),
                "counters": counters,
            },
        )
        _print_ts(
            f"[进度] {counters['processed']}/{total_sites} {result.website} 状态={result.status}",
            flush=True,
        )
    for site in pending_sites:
        running.add(asyncio.create_task(_run_site(site)))
        if len(running) < max_inflight:
            continue
        done_tasks, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done_tasks:
            await _consume_result(task)
    while running:
        done_tasks, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done_tasks:
            await _consume_result(task)

async def run_pipeline(settings: PipelineSettings) -> None:
    sites = load_sites(settings.input_path)
    sites = _apply_max_sites(sites, settings.max_sites)
    if not sites:
        raise SystemExit("输入中没有有效的官网链接")
    run_dir = ensure_run_dir(settings.run_dir)
    checkpoint_path = run_dir / "checkpoint.json"
    output_jsonl = run_dir / "output.jsonl"
    output_json = run_dir / "output.json"
    output_csv = run_dir / "output.csv"
    success_jsonl = run_dir / "output.success.jsonl"
    success_json = run_dir / "output.success.json"
    success_csv = run_dir / "output.success.csv"
    partial_jsonl = run_dir / "output.partial.jsonl"
    partial_json = run_dir / "output.partial.json"
    partial_csv = run_dir / "output.partial.csv"
    failed_jsonl = run_dir / "output.failed.jsonl"
    failed_json = run_dir / "output.failed.json"
    failed_csv = run_dir / "output.failed.csv"
    pages_dir = run_dir / "pages"

    checkpoint = _load_checkpoint(checkpoint_path) if settings.resume else {}
    done = _load_done_set_from_checkpoint(checkpoint)
    effective_resume_mode = settings.resume_mode
    if settings.simple_mode:
        text = (effective_resume_mode or "").strip()
        if text:
            _print_ts(f"[续跑] simple 模式忽略历史续跑模式：{text}")
        effective_resume_mode = None
    _apply_resume_mode_reopen(run_dir, done, effective_resume_mode, settings.resume)
    crawl_sem = asyncio.Semaphore(max(1, settings.concurrency))
    llm_sem = asyncio.Semaphore(max(1, settings.llm_concurrency))
    llm_client = _build_llm_client(settings, llm_sem)
    snov_client = _build_snov_client(settings)
    results: list[ExtractionResult] = []
    counters = _load_counters(checkpoint)
    total_sites = len(sites)
    strategy = get_strategy_for_mode(effective_resume_mode, settings.max_rounds, settings.max_pages)
    effective_llm_select = strategy.allow_llm_link_select and bool(settings.use_llm)
    _print_ts(f"[启动] 站点数={total_sites} 输出目录={run_dir}")
    _print_ts(f"[策略] 运行模式={strategy.mode} 最大轮次={strategy.max_rounds} 最大页数={strategy.max_pages}")
    _print_ts(f"[策略] LLM 开关={'开' if settings.use_llm else '关'}")
    _print_ts(f"[策略] 允许 LLM 选链={effective_llm_select}")
    _print_ts(f"[策略] 并发配置: 站点={settings.concurrency} LLM={settings.llm_concurrency}")
    # Always enforce site-level concurrency. Timeout and concurrency are independent.
    site_sem = asyncio.Semaphore(max(1, settings.concurrency))
    simple_phone_resolver: SimplePhoneResolver | None = None
    if settings.simple_mode:
        try:
            simple_phone_resolver = SimplePhoneResolver()
        except Exception as exc:
            _print_ts(f"[simple] 电话补抓初始化失败：{_humanize_exception(exc)}")

    pending_sites = _collect_pending_sites(sites, done)
    if settings.simple_mode:
        _print_ts(f"[simple] 待处理站点数={len(pending_sites)}")
        _print_ts("[simple] 跳过 crawler 初始化，直接处理 Google Maps 字段")
        await _run_pending_sites(
            pending_sites=pending_sites,
            crawler=_SimpleCrawlerStub(),
            llm_client=llm_client,
            settings=settings,
            pages_dir=pages_dir,
            snov_client=snov_client,
            strategy=strategy,
            site_sem=site_sem,
            results=results,
            done=done,
            counters=counters,
            checkpoint_path=checkpoint_path,
            total_sites=total_sites,
            output_jsonl=output_jsonl, output_csv=output_csv,
            success_jsonl=success_jsonl, success_csv=success_csv,
            partial_jsonl=partial_jsonl, partial_csv=partial_csv,
            failed_jsonl=failed_jsonl, failed_csv=failed_csv,
            simple_phone_resolver=simple_phone_resolver,
        )
    else:
        async with CrawlerClient(
            crawl_sem,
            page_timeout=settings.page_timeout,
            keys_path=settings.firecrawl_keys_path,
            base_url=settings.firecrawl_base_url,
            per_key_limit=settings.firecrawl_key_per_limit,
            wait_seconds=settings.firecrawl_key_wait_seconds,
        ) as crawler:
            await _run_pending_sites(
                pending_sites=pending_sites,
                crawler=crawler,
                llm_client=llm_client,
                settings=settings,
                pages_dir=pages_dir,
                snov_client=snov_client,
                strategy=strategy,
                site_sem=site_sem,
                results=results,
                done=done,
                counters=counters,
                checkpoint_path=checkpoint_path,
                total_sites=total_sites,
                output_jsonl=output_jsonl, output_csv=output_csv,
                success_jsonl=success_jsonl, success_csv=success_csv,
                partial_jsonl=partial_jsonl, partial_csv=partial_csv,
                failed_jsonl=failed_jsonl, failed_csv=failed_csv,
                simple_phone_resolver=simple_phone_resolver,
            )

    all_records = _load_jsonl_records(output_jsonl) if output_jsonl.exists() else []
    success_records = _load_jsonl_records(success_jsonl) if success_jsonl.exists() else []
    partial_records = _load_jsonl_records(partial_jsonl) if partial_jsonl.exists() else []
    failed_records = _load_jsonl_records(failed_jsonl) if failed_jsonl.exists() else []
    if not all_records:
        all_records = [_result_to_record(r) for r in results]
    if not success_records:
        success_records = [r for r in all_records if r.get("status") == "ok"]
    if not partial_records:
        partial_records = [r for r in all_records if r.get("status") == "partial"]
    if not failed_records:
        failed_records = [r for r in all_records if r.get("status") == "failed"]
    write_json(output_json, all_records)
    write_json(success_json, success_records)
    write_json(partial_json, partial_records)
    write_json(failed_json, failed_records)
def _load_done_set_from_checkpoint(checkpoint: dict[str, Any]) -> set[str]:
    done: set[str] = set()
    for value in checkpoint.get("done") or []:
        if not isinstance(value, str):
            continue
        key = canonical_site_key(value) or normalize_url(value) or value
        if key:
            done.add(key)
    return done
def _load_counters(checkpoint: dict[str, Any]) -> dict[str, int]:
    counters_obj = checkpoint.get("counters")
    if isinstance(counters_obj, dict):
        return counters_obj
    return {"processed": 0, "ok": 0, "partial": 0, "failed": 0}


def _apply_resume_mode_reopen(
    run_dir: Path,
    done: set[str],
    resume_mode: str | None,
    resume_enabled: bool,
) -> None:
    if not resume_enabled:
        return
    mode = (resume_mode or "").strip().lower()
    if mode not in {"partial", "failed", "representative"}:
        return
    reopen_keys = _collect_reopen_keys(run_dir, mode)
    if not reopen_keys:
        _print_ts(f"[续跑] 模式={mode}，未命中可重跑历史记录")
        return
    before = len(done)
    done.difference_update(reopen_keys)
    reopened = before - len(done)
    _print_ts(f"[续跑] 模式={mode}，重跑历史 {reopened} 条")


def _collect_reopen_keys(run_dir: Path, mode: str) -> set[str]:
    keys: set[str] = set()
    if mode in {"partial", "failed"}:
        source = run_dir / f"output.{mode}.jsonl"
        if not source.exists():
            return keys
        records = _load_jsonl_records(source)
        for record in records:
            website = record.get("website") if isinstance(record, dict) else None
            if not isinstance(website, str):
                continue
            key = canonical_site_key(website) or normalize_url(website) or website
            if key:
                keys.add(key)
        return keys
    # representative 模式：重跑“代表人缺失”的历史记录（不区分原状态）。
    source = run_dir / "output.jsonl"
    if not source.exists():
        return keys
    records = _load_jsonl_records(source)
    for record in records:
        if not isinstance(record, dict):
            continue
        website = record.get("website")
        if not isinstance(website, str):
            continue
        rep = record.get("representative")
        rep_text = rep.strip() if isinstance(rep, str) else ""
        if rep_text and rep_text != "未找到代表人":
            continue
        key = canonical_site_key(website) or normalize_url(website) or website
        if key:
            keys.add(key)
    return keys


def _build_llm_client(settings: PipelineSettings, llm_sem: asyncio.Semaphore) -> LLMClient:
    return LLMClient(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        model=settings.llm_model,
        semaphore=llm_sem,
        slot_count=settings.llm_concurrency,
        temperature=settings.llm_temperature,
        max_output_tokens=settings.llm_max_output_tokens,
        reasoning_effort=settings.llm_reasoning_effort,
    )


def _build_snov_client(settings: PipelineSettings) -> SnovClient | None:
    extension_selector = (settings.snov_extension_selector or "").strip() or None
    extension_token = (settings.snov_extension_token or "").strip() or None
    extension_fingerprint = (settings.snov_extension_fingerprint or "").strip() or None
    extension_cdp_host = (settings.snov_extension_cdp_host or "").strip() or None
    extension_cdp_port = settings.snov_extension_cdp_port
    extension_ready = bool((extension_selector and extension_token) or extension_cdp_port)
    if not (extension_ready or settings.snov_extension_only):
        return None
    return SnovClient(
        extension_selector=extension_selector,
        extension_token=extension_token,
        extension_fingerprint=extension_fingerprint,
        extension_cdp_host=extension_cdp_host,
        extension_cdp_port=extension_cdp_port,
        extension_only=bool(settings.snov_extension_only),
    )


def _persist_result_files(
    result: ExtractionResult,
    *,
    output_jsonl: Path,
    output_csv: Path,
    success_jsonl: Path,
    success_csv: Path,
    partial_jsonl: Path,
    partial_csv: Path,
    failed_jsonl: Path,
    failed_csv: Path,
) -> None:
    record = _result_to_record(result)
    write_jsonl(output_jsonl, record)
    write_csv(output_csv, [result])
    if result.status == "ok":
        write_jsonl(success_jsonl, record)
        write_csv(success_csv, [result])
        _print_ts(f"[落盘] success +1 -> {success_jsonl.name} ({result.website})", flush=True)
    elif result.status == "partial":
        write_jsonl(partial_jsonl, record)
        write_csv(partial_csv, [result])
        _print_ts(f"[落盘] partial +1 -> {partial_jsonl.name} ({result.website})", flush=True)
    elif result.status == "failed":
        write_jsonl(failed_jsonl, record)
        write_csv(failed_csv, [result])
        _print_ts(f"[落盘] failed +1 -> {failed_jsonl.name} ({result.website})", flush=True)


def _update_counters(counters: dict[str, int], status: str) -> None:
    counters["processed"] = counters.get("processed", 0) + 1
    if status in counters:
        counters[status] = counters.get(status, 0) + 1


async def _bounded_process_site(
    site: SiteInput,
    crawler: CrawlerClient,
    llm: LLMClient,
    settings: PipelineSettings,
    pages_dir: Path,
    snov_client: SnovClient | None = None,
    strategy: RunStrategy | None = None,
    simple_phone_resolver: SimplePhoneResolver | None = None,
) -> ExtractionResult:
    timeout_seconds = settings.site_timeout_seconds
    if timeout_seconds is not None and timeout_seconds <= 0:
        timeout_seconds = None
    try:
        if timeout_seconds:
            return await asyncio.wait_for(
                _process_site(
                    site,
                    crawler,
                    llm,
                    settings,
                    pages_dir,
                    snov_client,
                    strategy,
                    simple_phone_resolver=simple_phone_resolver,
                ),
                timeout=timeout_seconds,
            )
        return await _process_site(
            site,
            crawler,
            llm,
            settings,
            pages_dir,
            snov_client,
            strategy,
            simple_phone_resolver=simple_phone_resolver,
        )
    except asyncio.TimeoutError:
        _log(site.website, f"站点处理超时（{timeout_seconds:.0f}s），标记为失败")
        return _build_failure_result(site, _resolve_input_name(site), "site_timeout", notes="site_timeout")
    except SnovMaskedEmailError:
        return await _retry_after_snov_masked(site, crawler, llm, settings, pages_dir, snov_client, strategy)
    except Exception as exc:
        _log(site.website, f"该官网暂时无法处理：{_humanize_exception(exc)}")
        return _build_failure_result(site, _resolve_input_name(site), str(exc))


async def _retry_after_snov_masked(
    site: SiteInput,
    crawler: CrawlerClient,
    llm: LLMClient,
    settings: PipelineSettings,
    pages_dir: Path,
    snov_client: SnovClient | None,
    strategy: RunStrategy | None,
) -> ExtractionResult:
    refreshed = False
    if snov_client is not None and getattr(snov_client, "_just_refreshed", False):
        _log(site.website, "Snov 返回脱敏邮箱，已在预检阶段刷新扩展 cookie")
        snov_client._just_refreshed = False
        refreshed = True
    elif snov_client is not None and getattr(snov_client, "extension_cdp_port", None):
        refreshed = snov_client.refresh_extension_cookies()
        if refreshed:
            _log(site.website, "Snov 返回脱敏邮箱，已刷新扩展 cookie")
        else:
            _log(site.website, "Snov 返回脱敏邮箱，刷新扩展 cookie 失败")
    if refreshed:
        delay = random.uniform(5.0, 20.0)
        _log(site.website, f"Snov 脱敏邮箱：{delay:.1f}s 后立即重试一次")
        await asyncio.sleep(delay)
        try:
            return await _process_site(site, crawler, llm, settings, pages_dir, snov_client, strategy)
        except SnovMaskedEmailError:
            _log(site.website, "Snov 脱敏邮箱仍未解开，转入延迟重试队列")
    _log(site.website, "Snov 返回脱敏邮箱，已进入延迟重试队列")
    name_hint = _resolve_input_name(site)
    return _build_failure_result(
        site,
        name_hint,
        "snov_masked",
        notes="snov_masked_retry",
        status="retry",
    )


def _build_failure_result(
    site: SiteInput,
    input_name: str | None,
    error: str,
    *,
    notes: str | None = None,
    status: str = "failed",
) -> ExtractionResult:
    return ExtractionResult(
        website=site.website,
        input_name=input_name,
        company_name=input_name if isinstance(input_name, str) and input_name.strip() else None,
        representative=None,
        capital=None,
        employees=None,
        email=None,
        emails=None,
        email_count=0,
        phone=None,
        company_name_source_url=None,
        representative_source_url=None,
        capital_source_url=None,
        employees_source_url=None,
        email_source_url=None,
        phone_source_url=None,
        notes=notes,
        source_urls=[],
        status=status,
        error=error,
        extracted_at=utc_now_iso(),
        raw_llm=None,
    )

