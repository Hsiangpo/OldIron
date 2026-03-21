from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from ...config import PipelineSettings, RunStrategy, get_strategy_for_mode
from ...crawler import CrawlerClient
from ...errors import SnovMaskedEmailError
from ...llm_client import LLMClient
from ...models import ExtractionResult, PageContent, SiteInput
from ...snov_client import SnovClient
from ...utils import normalize_url, utc_now_iso
from ..email import _apply_email_policy, _prefetch_snov_emails
from ..fields import _missing_fields, _normalize_required_fields
from ..heuristics import _apply_heuristic_extraction, _sanitize_info
from ..logging import (
    _humanize_exception,
    _log,
    _log_timing_summary,
    _resolve_input_name,
    _timing_end,
    _timing_start,
    drop_snov_prefetch_task,
    register_snov_prefetch_task,
)
from ..memory import _remember_failed, _update_memory_visited
from ..result import _build_result
from ..selection import _merge_links
from ..sitemap import _collect_sitemap_links, _prefetch_key_urls_from_sitemap
from .firecrawl import _extract_with_firecrawl, _should_skip_by_keyword
from .rounds import _extract_with_rounds
from .simple_phone import SimplePhoneResolver


def _resolve_strategy(settings: PipelineSettings, strategy: RunStrategy | None) -> RunStrategy:
    if strategy is not None:
        return strategy
    return get_strategy_for_mode(settings.resume_mode, settings.max_rounds, settings.max_pages)


def _build_failure_result(
    site: SiteInput,
    *,
    input_name: str | None,
    source_urls: list[str],
    error: str,
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
        source_urls=source_urls,
        status=status,
        error=error,
        extracted_at=utc_now_iso(),
        raw_llm=None,
    )


def _seed_info_from_site(site: SiteInput, *, skip_email: bool) -> dict[str, Any]:
    seed_info: dict[str, Any] = {}
    if not skip_email or not isinstance(site.raw, dict):
        return seed_info
    existing_email = site.raw.get("email")
    if isinstance(existing_email, str) and existing_email.strip():
        seed_info["email"] = existing_email.strip()
    existing_emails = site.raw.get("emails")
    if isinstance(existing_emails, list):
        cleaned = [e.strip() for e in existing_emails if isinstance(e, str) and e.strip()]
        if cleaned:
            seed_info["emails"] = cleaned
            seed_info["email_count"] = len(cleaned)
            if "email" not in seed_info:
                seed_info["email"] = cleaned[0]
    return seed_info


def _extract_simple_phone(site: SiteInput) -> str | None:
    raw = site.raw if isinstance(site.raw, dict) else {}
    for key in ("phone", "telephone", "tel", "phone_number"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _build_simple_info(site: SiteInput) -> dict[str, Any]:
    info: dict[str, Any] = {"evidence": {}}
    input_name = _resolve_input_name(site)
    if isinstance(input_name, str) and input_name.strip():
        info["company_name"] = input_name.strip()
        info["evidence"]["company_name"] = {"url": None, "quote": "maps_name"}
    phone = _extract_simple_phone(site)
    if isinstance(phone, str) and phone.strip():
        info["phone"] = phone.strip()
        info["evidence"]["phone"] = {"url": None, "quote": phone.strip()}
    return info


async def _try_enrich_simple_phone(
    site: SiteInput,
    info: dict[str, Any],
    resolver: SimplePhoneResolver | None,
) -> None:
    if resolver is None:
        return
    existing_phone = info.get("phone")
    if isinstance(existing_phone, str) and existing_phone.strip():
        return
    phone = await asyncio.to_thread(resolver.resolve_from_raw, site.raw)
    if not isinstance(phone, str) or not phone.strip():
        return
    phone_text = phone.strip()
    info["phone"] = phone_text
    evidence = info.get("evidence")
    if not isinstance(evidence, dict):
        evidence = {}
        info["evidence"] = evidence
    evidence["phone"] = {"url": None, "quote": phone_text}
    _log(site.website, f"simple CID补抓座机：{phone_text}（来源：Google Maps 详情）")


def _log_simple_extracted_fields(website: str, info: dict[str, Any]) -> None:
    company_name = info.get("company_name")
    phone = info.get("phone")
    if isinstance(company_name, str) and company_name.strip():
        _log(website, f"simple 命中公司名：{company_name.strip()}（来源：Google Maps）")
    else:
        _log(website, "simple 未命中公司名")
    if isinstance(phone, str) and phone.strip():
        _log(website, f"simple 命中座机：{phone.strip()}（来源：Google Maps）")
    else:
        _log(website, "simple 未命中座机")


def _prepare_snov_prefetch(
    site: SiteInput,
    settings: PipelineSettings,
    strategy: RunStrategy,
    memory: dict[str, Any],
    snov_client: SnovClient | None,
    *,
    skip_email: bool,
) -> tuple[asyncio.Task[list[str]] | None, float | None]:
    if skip_email:
        _log(site.website, "跳过邮箱补全模式，跳过 Snov 预取")
        return None, None
    if not strategy.allow_snov_prefetch:
        _log(site.website, "策略关闭：跳过 Snov 邮箱预取")
        return None, None
    memory["snov_prefetch_attempted"] = True
    prefetch_started = time.time()
    prefetch_task = asyncio.create_task(
        _prefetch_snov_emails(site.website, snov_client, settings, max_wait_seconds=30)
    )
    register_snov_prefetch_task(site.website, prefetch_task, prefetch_started)
    return prefetch_task, prefetch_started


async def _fetch_homepage_with_recovery(
    site: SiteInput,
    crawler: CrawlerClient,
    memory: dict[str, Any],
) -> PageContent:
    homepage_timer = _timing_start(memory, "homepage_fetch")
    homepage = await crawler.fetch_page(site.website)
    _timing_end(memory, "homepage_fetch", homepage_timer)
    if homepage.success:
        return homepage
    _log(site.website, f"首页打开失败（可能网络波动/拦截）：{homepage.error}")
    if memory.get("homepage_retry"):
        _remember_failed(memory, homepage.url)
        return homepage
    error_text = (homepage.error or "").lower()
    if not ("timeout" in error_text or "exceeded" in error_text or "timed out" in error_text):
        _remember_failed(memory, homepage.url)
        return homepage
    _log(site.website, "首页加载超时，尝试渲染重试")
    memory["homepage_retry"] = True
    render_timer = _timing_start(memory, "homepage_render")
    rendered_home = await crawler.fetch_page_rendered(site.website)
    _timing_end(memory, "homepage_render", render_timer)
    if rendered_home.success:
        memory["rendered_homepage"] = True
        return rendered_home
    _log(site.website, f"首页渲染重试失败：{rendered_home.error or 'unknown_error'}")
    _remember_failed(memory, homepage.url)
    return homepage


def _early_failure_from_homepage(
    site: SiteInput,
    homepage: PageContent,
) -> ExtractionResult | None:
    if not homepage.success:
        return None
    http_status = _detect_http_error_status(homepage)
    input_name = _resolve_input_name(site)
    source_urls = [homepage.url] if homepage.url else [site.website]
    if http_status in (403, 404):
        _log(site.website, f"首页返回 {http_status}，直接判定失败")
        return _build_failure_result(
            site,
            input_name=input_name,
            source_urls=source_urls,
            error=f"http_{http_status}",
        )
    if _is_parked_page(homepage):
        _log(site.website, "首页疑似停放/占位页，直接判定失败")
        return _build_failure_result(
            site,
            input_name=input_name,
            source_urls=source_urls,
            error="parked_domain",
        )
    return None


async def _expand_from_homepage(
    site: SiteInput,
    crawler: CrawlerClient,
    homepage: PageContent,
    visited: dict[str, PageContent],
    memory: dict[str, Any],
    strategy: RunStrategy,
    *,
    max_pages: int,
    required_fields: list[str],
) -> PageContent:
    visited[homepage.url] = homepage
    _prefer_html_for_short_content(site.website, homepage)
    _hint_overview_neighbor_links(site.website, homepage, memory)
    _log(site.website, f"已打开首页：{homepage.title or homepage.url}")
    _update_memory_visited(memory, visited)
    if not (homepage.links or []):
        _log(site.website, "首页未提取到导航链接，尝试等待渲染补充导航")
        render_timer = _timing_start(memory, "homepage_render")
        rendered_home = await crawler.fetch_page_rendered(site.website)
        _timing_end(memory, "homepage_render", render_timer)
        if rendered_home.success and rendered_home.links:
            base_url = normalize_url(homepage.url) or homepage.url
            rendered_url = normalize_url(rendered_home.url) or rendered_home.url
            if rendered_url == base_url and rendered_home.url != homepage.url:
                visited.pop(homepage.url, None)
            visited[rendered_home.url] = rendered_home
            homepage = rendered_home
            _prefer_html_for_short_content(site.website, rendered_home)
            _hint_overview_neighbor_links(site.website, rendered_home, memory)
            _log(site.website, f"首页渲染后补充链接 {len(rendered_home.links)} 条")
            _update_memory_visited(memory, visited)
        elif rendered_home.success:
            _log(site.website, "首页渲染后仍未发现导航链接")
        else:
            _log(site.website, f"首页渲染失败：{rendered_home.error or 'unknown_error'}")
    if max_pages - len(visited) > 0 and not memory.get("hints"):
        sitemap_timer = _timing_start(memory, "sitemap_collect")
        sitemap_links = await _collect_sitemap_links(
            site.website,
            crawler,
            memory,
            allow_pdf=strategy.allow_pdf_extract,
        )
        _timing_end(memory, "sitemap_collect", sitemap_timer)
        if sitemap_links:
            _log(site.website, f"Sitemap 收录链接 {len(sitemap_links)} 条")
            sitemap_prefetch_timer = _timing_start(memory, "sitemap_prefetch")
            await _prefetch_key_urls_from_sitemap(
                site.website,
                crawler,
                visited,
                sitemap_links,
                memory,
                max_pages=max_pages,
                allow_pdf=strategy.allow_pdf_extract,
            )
            _timing_end(memory, "sitemap_prefetch", sitemap_prefetch_timer)
    if max_pages - len(visited) > 0:
        need_rep = "representative" in required_fields
        chain_timer = _timing_start(memory, "company_chain_fetch")
        await _open_company_info_chain(
            site.website,
            crawler,
            visited,
            memory,
            max_pages=max_pages,
            allow_pdf=strategy.allow_pdf_extract,
            need_rep=need_rep,
        )
        _timing_end(memory, "company_chain_fetch", chain_timer)
    return homepage


def _consume_prefetch_if_done(
    site: SiteInput,
    memory: dict[str, Any],
    prefetch_task: asyncio.Task[list[str]] | None,
    prefetch_started: float | None,
) -> tuple[asyncio.Task[list[str]] | None, float | None]:
    if prefetch_task is None or not prefetch_task.done():
        return prefetch_task, prefetch_started
    try:
        prefetched_emails = prefetch_task.result()
    except SnovMaskedEmailError:
        raise
    except Exception as exc:
        _log(site.website, f"Snov 预取失败：{_humanize_exception(exc)}")
        prefetched_emails = []
    if isinstance(prefetched_emails, list) and prefetched_emails:
        memory["snov_prefetched_emails"] = prefetched_emails
    memory["snov_prefetch_consumed"] = True
    drop_snov_prefetch_task(site.website)
    return None, None


async def _extract_site_info(
    site: SiteInput,
    crawler: CrawlerClient,
    llm: LLMClient,
    visited: dict[str, PageContent],
    links_pool: list[Any],
    settings: PipelineSettings,
    memory: dict[str, Any],
    snov_client: SnovClient | None,
    strategy: RunStrategy,
    seed_info: dict[str, Any],
) -> dict[str, Any] | None:
    extract_timer = _timing_start(memory, "extract_rounds")
    if settings.firecrawl_extract_enabled:
        pre_info = _apply_heuristic_extraction({}, visited, required_fields=settings.required_fields)
        pre_info = _sanitize_info(pre_info)
        need_phone = "phone" in _normalize_required_fields(settings.required_fields)
        if isinstance(pre_info.get("representative"), str) and pre_info.get("representative").strip() and not need_phone:
            _log(site.website, "代表人已由规则命中，跳过 Firecrawl 提取")
            info = pre_info
        else:
            info = await _extract_with_firecrawl(site, crawler, visited, settings, memory)
        if isinstance(info, dict):
            input_name = _resolve_input_name(site)
            if isinstance(input_name, str) and input_name.strip():
                info.setdefault("company_name", input_name.strip())
                evidence = info.get("evidence")
                if not isinstance(evidence, dict):
                    evidence = {}
                    info["evidence"] = evidence
                evidence.setdefault("company_name", {"url": None, "quote": "input_name"})
        email_timer = _timing_start(memory, "email_policy")
        info = await _apply_email_policy(info, visited, site.website, memory, settings, snov_client)
        _timing_end(memory, "email_policy", email_timer)
        _log_extracted_info(site.website, info)
        required_fields = settings.required_fields
        if settings.skip_email:
            required_fields = [f for f in required_fields if f != "email"]
        missing_after_fc = _missing_fields(info, required_fields=required_fields)
        if missing_after_fc:
            info = await _extract_with_rounds(
                site,
                crawler,
                llm,
                visited,
                links_pool,
                settings,
                memory,
                snov_client,
                seed_info=info,
                strategy=strategy,
            )
    else:
        info = await _extract_with_rounds(
            site,
            crawler,
            llm,
            visited,
            links_pool,
            settings,
            memory,
            snov_client,
            seed_info=seed_info,
            strategy=strategy,
        )
    _timing_end(memory, "extract_rounds", extract_timer)
    return info


async def _process_site(
    site: SiteInput,
    crawler: CrawlerClient,
    llm: LLMClient,
    settings: PipelineSettings,
    pages_dir: Path,
    snov_client: SnovClient | None = None,
    strategy: RunStrategy | None = None,
    simple_phone_resolver: SimplePhoneResolver | None = None,
) -> ExtractionResult:
    strategy = _resolve_strategy(settings, strategy)
    visited: dict[str, PageContent] = {}
    memory: dict[str, Any] = {
        "summary": None,
        "hints": [],
        "visited": [],
        "failed": [],
        "found": {},
        "summary_version": 0,
        "site_started_at": time.perf_counter(),
        "timings": {},
    }

    def _finalize(result: ExtractionResult) -> ExtractionResult:
        _log_timing_summary(site.website, memory)
        return result

    if settings.simple_mode:
        _log(site.website, "simple 模式：使用 Google Maps 字段，不进行官网抓取")
        info = _build_simple_info(site)
        await _try_enrich_simple_phone(site, info, simple_phone_resolver)
        _log_simple_extracted_fields(site.website, info)
        return _finalize(_build_result(site, {}, info, required_fields=settings.required_fields, memory=memory))

    max_pages = max(1, settings.max_pages)
    skip_email = bool(getattr(settings, "skip_email", False))
    seed_info = _seed_info_from_site(site, skip_email=skip_email)
    _log(site.website, "开始处理站点")
    prefetch_task, prefetch_started = _prepare_snov_prefetch(
        site, settings, strategy, memory, snov_client, skip_email=skip_email
    )

    homepage = await _fetch_homepage_with_recovery(site, crawler, memory)
    if not homepage.success:
        _remember_failed(memory, homepage.url)
    early = _early_failure_from_homepage(site, homepage)
    if early is not None:
        _remember_failed(memory, homepage.url)
        return _finalize(early)

    if homepage.success:
        homepage = await _expand_from_homepage(
            site,
            crawler,
            homepage,
            visited,
            memory,
            strategy,
            max_pages=max_pages,
            required_fields=settings.required_fields,
        )

    prefetch_task, prefetch_started = _consume_prefetch_if_done(site, memory, prefetch_task, prefetch_started)
    if prefetch_task is not None:
        memory["snov_prefetch_task"] = prefetch_task
        memory["snov_prefetch_started"] = prefetch_started

    if not visited:
        input_name = _resolve_input_name(site)
        source_urls = [homepage.url] if homepage.url else [site.website]
        return _finalize(
            _build_failure_result(
                site,
                input_name=input_name,
                source_urls=source_urls,
                error=homepage.error or "crawl_failed",
            )
        )

    links_pool = _merge_links(visited, memory, allow_pdf=strategy.allow_pdf_extract)
    keyword = (settings.keyword or "").strip() if settings.keyword else ""
    if settings.use_llm and settings.keyword_filter_enabled and keyword and strategy.allow_llm_keyword_filter:
        should_skip, decision = await _should_skip_by_keyword(
            site.website, keyword, llm, visited, settings, memory
        )
        if should_skip:
            reason = decision.get("reason") if isinstance(decision, dict) else None
            reason_text = f"（{reason.strip()}）" if isinstance(reason, str) and reason.strip() else ""
            _log(site.website, f"关键词过滤：不匹配“{keyword}”，跳过该站点{reason_text}")
            input_name = _resolve_input_name(site)
            return _finalize(
                _build_failure_result(
                    site,
                    input_name=input_name,
                    source_urls=sorted(visited.keys()),
                    error="keyword_mismatch",
                    notes=f"keyword_mismatch:{keyword}",
                )
            )

    info = await _extract_site_info(
        site,
        crawler,
        llm,
        visited,
        links_pool,
        settings,
        memory,
        snov_client,
        strategy,
        seed_info,
    )
    if isinstance(info, dict):
        rep = info.get("representative")
        has_rep = isinstance(rep, str) and rep.strip()
        if not has_rep:
            notes = info.get("notes")
            tail = "rep_force_skipped"
            if isinstance(notes, str) and notes.strip():
                if tail not in notes:
                    info["notes"] = f"{notes};{tail}"
            else:
                info["notes"] = tail
    if settings.save_pages:
        from ..io import _save_pages_markdown

        _save_pages_markdown(pages_dir, site.website, visited)
    return _finalize(_build_result(site, visited, info, required_fields=settings.required_fields, memory=memory))


# Imported late to keep import graph explicit.
from ..crawl import (  # noqa: E402
    _detect_http_error_status,
    _hint_overview_neighbor_links,
    _is_parked_page,
    _open_company_info_chain,
    _prefer_html_for_short_content,
)
from ..logging import _log_extracted_info  # noqa: E402

