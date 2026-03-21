from __future__ import annotations

from typing import Any

from ...config import PipelineSettings, RunStrategy, get_strategy_for_mode
from ...crawler import CrawlerClient
from ...llm_client import LLMClient
from ...models import LinkItem, PageContent, SiteInput
from ..crawl import _fetch_pages_batch, _filter_fetch_urls, _get_homepage_context
from ..email import _apply_email_policy
from ..fields import _missing_fields
from ..heuristics import (
    _apply_heuristic_extraction,
    _backfill_rep_evidence,
    _merge_info,
    _sanitize_info,
)
from ..logging import (
    _format_fields_zh,
    _log,
    _log_extracted_info,
    _timing_end,
    _timing_start,
)
from ..memory import _remember_selected, _update_memory_found
from ..selection import (
    _COMPANY_OVERVIEW_KEYWORDS,
    _REP_PAGE_KEYWORDS,
    _email_key_pages_exhausted,
    _ensure_key_pages_in_selection,
    _filter_rep_candidate_urls,
    _merge_links,
    _prefilter_links_for_llm,
    _remaining_links,
    _select_pages_for_llm,
    _top_key_urls_for_email,
    _top_matching_urls,
)
from .firecrawl import _build_pages_payload, _guess_common_company_paths, _has_rep_and_email


def _resolve_round_strategy(settings: PipelineSettings, strategy: RunStrategy | None) -> RunStrategy:
    if strategy is not None:
        return strategy
    return get_strategy_for_mode(settings.resume_mode, settings.max_rounds, settings.max_pages)


def _ensure_input_name_in_info(site: SiteInput, info: dict[str, Any] | None, memory: dict[str, Any]) -> dict[str, Any]:
    data = info if isinstance(info, dict) else {}
    input_name = getattr(site, "input_name", None)
    if isinstance(input_name, str) and input_name.strip():
        data.setdefault("company_name", input_name.strip())
        evidence = data.get("evidence")
        if not isinstance(evidence, dict):
            evidence = {}
            data["evidence"] = evidence
        evidence.setdefault("company_name", {"url": None, "quote": "input_name"})
        memory["last_missing"] = ["representative", "email"]
    return data


async def _run_llm_extraction_step(
    site: SiteInput,
    llm: LLMClient,
    visited: dict[str, PageContent],
    settings: PipelineSettings,
    memory: dict[str, Any],
    info: dict[str, Any],
    strategy: RunStrategy,
) -> dict[str, Any]:
    llm_missing = _missing_fields(info, required_fields=["company_name", "representative"])
    if not llm_missing:
        return info
    llm_cap = (
        settings.llm_max_pages
        if isinstance(settings.llm_max_pages, int) and settings.llm_max_pages > 0
        else settings.max_pages
    )
    max_llm_pages = min(len(visited), max(3, min(settings.max_pages, llm_cap)))
    focus_pages = _select_pages_for_llm(visited, max_pages=max_llm_pages, missing_fields=llm_missing)
    pages_payload = _build_pages_payload(
        focus_pages or list(visited.values()),
        settings.max_content_chars,
        allow_pdf=strategy.allow_pdf_extract,
        prefer_fit=True,
        prefer_raw_html=True,
    )
    if not pages_payload:
        return info
    llm_timer = _timing_start(memory, "llm_extract")
    llm_info = await llm.extract_company_info(site.website, pages_payload, memory=memory)
    _timing_end(memory, "llm_extract", llm_timer)
    if not isinstance(llm_info, dict):
        return info
    merged = _merge_info(info, llm_info)
    merged = _backfill_rep_evidence(merged, visited)
    merged = _sanitize_info(merged)
    _update_memory_found(memory, merged)
    return merged


async def _try_open_rep_pages(
    site: SiteInput,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    remaining: list[LinkItem],
    settings: PipelineSettings,
    memory: dict[str, Any],
    max_pages_allowed: int,
) -> bool:
    rep_attempts = int(memory.get("rep_pages_attempts") or 0)
    if rep_attempts >= 2:
        return False
    rep_tried = memory.get("rep_pages_tried")
    rep_tried_set = set(rep_tried) if isinstance(rep_tried, list) else set()
    rep_candidates = _top_matching_urls(remaining, _REP_PAGE_KEYWORDS, limit=6)
    if not rep_candidates:
        rep_candidates = _top_matching_urls(remaining, _COMPANY_OVERVIEW_KEYWORDS, limit=4)
    if not rep_candidates:
        hinted = memory.get("hints")
        if isinstance(hinted, list):
            rep_candidates.extend([u for u in hinted if isinstance(u, str) and u.strip()])
    if not rep_candidates:
        guessed = _guess_common_company_paths(site.website, visited, limit=6)
        if guessed:
            _log(site.website, f"规则补充代表人常见路径：{', '.join(guessed)}")
            rep_candidates.extend(guessed)
            hints = memory.get("hints")
            if not isinstance(hints, list):
                hints = []
            for url in guessed:
                if url not in hints:
                    hints.append(url)
            memory["hints"] = hints[-40:]
    rep_candidates = _filter_rep_candidate_urls(rep_candidates)
    rep_candidates = _filter_fetch_urls(rep_candidates, visited, memory, max_pages=max_pages_allowed)
    rep_candidates = [u for u in rep_candidates if u not in rep_tried_set]
    if not rep_candidates:
        return False
    take = max(1, min(2, max_pages_allowed - len(visited)))
    more = rep_candidates[:take]
    _log(site.website, f"Representative missing, rule-open pages: {', '.join(more)}")
    rep_tried_set.update(more)
    memory["rep_pages_tried"] = list(rep_tried_set)[-80:]
    memory["rep_pages_attempts"] = rep_attempts + 1
    rep_fetch_timer = _timing_start(memory, "rep_pages_fetch")
    await _fetch_pages_batch(
        site.website,
        crawler,
        more,
        visited,
        memory,
        max_pages=max_pages_allowed,
        label="rule_open_rep_pages",
    )
    _timing_end(memory, "rep_pages_fetch", rep_fetch_timer)
    return True


async def _try_open_email_pages(
    site: SiteInput,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    remaining: list[LinkItem],
    settings: PipelineSettings,
    memory: dict[str, Any],
) -> bool:
    if memory.get("email_pages_tried") or not remaining or len(visited) >= settings.max_pages:
        return False
    email_candidates = _top_key_urls_for_email(remaining)
    email_candidates = _filter_fetch_urls(
        email_candidates, visited, memory, max_pages=settings.max_pages
    )
    if not email_candidates:
        memory["email_pages_tried"] = True
        return False
    take = max(1, min(2, settings.max_pages - len(visited)))
    more = email_candidates[:take]
    _log(site.website, f"邮箱缺失，规则补充打开联系页：{', '.join(more)}")
    memory["email_pages_tried"] = True
    email_fetch_timer = _timing_start(memory, "email_pages_fetch")
    await _fetch_pages_batch(
        site.website,
        crawler,
        more,
        visited,
        memory,
        max_pages=settings.max_pages,
        label="并发打开邮箱页面",
    )
    _timing_end(memory, "email_pages_fetch", email_fetch_timer)
    return True


async def _try_llm_select_pages(
    site: SiteInput,
    llm: LLMClient,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    remaining: list[LinkItem],
    missing: list[str],
    memory: dict[str, Any],
    max_pages_allowed: int,
) -> bool:
    select_count = max(1, min(3, max_pages_allowed - len(visited)))
    if select_count <= 0:
        return False
    _log(site.website, f"AI 正在补全：优先寻找 {_format_fields_zh(missing)} 相关页面")
    homepage_context = _get_homepage_context(site.website, visited, memory, max_chars=2000)
    llm_candidates = _prefilter_links_for_llm(remaining, missing, limit=80)
    if len(llm_candidates) < len(remaining):
        _log(site.website, f"AI 选链候选压缩：{len(remaining)} -> {len(llm_candidates)}")
    select_timer = _timing_start(memory, "llm_select_links")
    more = await llm.select_links(
        site.website,
        llm_candidates,
        max_select=select_count,
        missing_fields=missing,
        memory=memory,
        homepage_context=homepage_context,
    )
    _timing_end(memory, "llm_select_links", select_timer)
    more = _ensure_key_pages_in_selection(more, remaining, missing, select_count, visited, memory)
    if not more:
        return False
    _log(site.website, f"AI 追加打开：{', '.join(more)}")
    _remember_selected(memory, more)
    llm_fetch_timer = _timing_start(memory, "llm_pages_fetch")
    await _fetch_pages_batch(
        site.website,
        crawler,
        more,
        visited,
        memory,
        max_pages=max_pages_allowed,
        label="并发追加页面",
    )
    _timing_end(memory, "llm_pages_fetch", llm_fetch_timer)
    return True


async def _try_rule_select_pages(
    site: SiteInput,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    remaining: list[LinkItem],
    missing: list[str],
    memory: dict[str, Any],
    max_pages_allowed: int,
) -> bool:
    select_count = max(1, min(3, max_pages_allowed - len(visited)))
    if select_count <= 0:
        return False
    _log(site.website, f"规则补全：优先寻找 {_format_fields_zh(missing)} 相关页面")
    candidates = _prefilter_links_for_llm(remaining, missing, limit=80)
    if len(candidates) < len(remaining):
        _log(site.website, f"规则候选压缩：{len(remaining)} -> {len(candidates)}")

    picked_urls: list[str] = []
    if "representative" in missing:
        picked_urls.extend(_top_matching_urls(candidates, _REP_PAGE_KEYWORDS, limit=8))
    picked_urls.extend(_top_matching_urls(candidates, _COMPANY_OVERVIEW_KEYWORDS, limit=8))
    if "email" in missing:
        picked_urls.extend(_top_key_urls_for_email(candidates))
    if not picked_urls:
        picked_urls.extend([link.url for link in candidates if isinstance(link.url, str) and link.url.strip()])

    deduped: list[str] = []
    seen: set[str] = set()
    for url in picked_urls:
        if not isinstance(url, str):
            continue
        u = url.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(u)

    deduped = _filter_fetch_urls(deduped, visited, memory, max_pages=max_pages_allowed)
    deduped = _ensure_key_pages_in_selection(
        deduped, remaining, missing, select_count, visited, memory
    )
    more = deduped[:select_count]
    if not more:
        return False

    _log(site.website, f"规则追加打开：{', '.join(more)}")
    _remember_selected(memory, more)
    fetch_timer = _timing_start(memory, "rule_pages_fetch")
    await _fetch_pages_batch(
        site.website,
        crawler,
        more,
        visited,
        memory,
        max_pages=max_pages_allowed,
        label="规则追加页面",
    )
    _timing_end(memory, "rule_pages_fetch", fetch_timer)
    return True


async def _extract_with_rounds(
    site: SiteInput,
    crawler: CrawlerClient,
    llm: LLMClient,
    visited: dict[str, PageContent],
    links_pool: list[LinkItem],
    settings: PipelineSettings,
    memory: dict[str, Any],
    snov_client: Any | None = None,
    seed_info: dict[str, Any] | None = None,
    strategy: RunStrategy | None = None,
) -> dict[str, Any] | None:
    strategy = _resolve_round_strategy(settings, strategy)
    rounds = strategy.max_rounds
    allow_llm_link_select = strategy.allow_llm_link_select and settings.use_llm
    info = _ensure_input_name_in_info(site, seed_info, memory)
    for round_index in range(rounds):
        links_pool = _merge_links(visited, memory, allow_pdf=strategy.allow_pdf_extract)
        remaining = _remaining_links(visited, links_pool, memory)
        info = _sanitize_info(info if isinstance(info, dict) else {})
        info = _apply_heuristic_extraction(info, visited, required_fields=settings.required_fields)
        if settings.use_llm:
            info = await _run_llm_extraction_step(site, llm, visited, settings, memory, info, strategy)
        email_timer = _timing_start(memory, "email_policy")
        info = await _apply_email_policy(info, visited, site.website, memory, settings, snov_client)
        _timing_end(memory, "email_policy", email_timer)
        _log_extracted_info(site.website, info)
        _update_memory_found(memory, info)

        required_fields = settings.required_fields
        if settings.skip_email:
            required_fields = [f for f in required_fields if f != "email"]
        missing = _missing_fields(info, required_fields=required_fields)
        memory["last_missing"] = missing
        rep_bonus_cap = 2
        rep_bonus_used = int(memory.get("rep_pages_bonus_used") or 0)
        has_email = isinstance(info.get("email"), str) and bool(info.get("email").strip())
        rep_missing = "representative" in missing
        rep_bonus_remaining = max(0, rep_bonus_cap - rep_bonus_used) if rep_missing and has_email else 0
        max_pages_allowed = settings.max_pages + rep_bonus_remaining
        if rep_bonus_remaining > 0 and not memory.get("rep_pages_bonus_logged"):
            memory["rep_pages_bonus_logged"] = True
            _log(
                site.website,
                "代表人补全：触发额外页面预算 "
                f"+{rep_bonus_remaining}（{settings.max_pages} -> {max_pages_allowed}）",
            )
        if missing:
            _log(site.website, f"还缺：{_format_fields_zh(missing)}")

        if "representative" in missing and remaining and len(visited) < max_pages_allowed:
            changed = await _try_open_rep_pages(
                site, crawler, visited, remaining, settings, memory, max_pages_allowed
            )
            if changed:
                extra_used = max(0, len(visited) - settings.max_pages)
                if extra_used:
                    memory["rep_pages_bonus_used"] = max(rep_bonus_used, extra_used)
                continue

        if missing == ["email"]:
            if await _try_open_email_pages(site, crawler, visited, remaining, settings, memory):
                continue
            _log(site.website, "邮箱仍缺，Snov 未返回邮箱，停止继续扩展页面")
            break

        if not missing:
            if _has_rep_and_email(info):
                memory["early_stop"] = True
                _log(site.website, "代表人+邮箱已齐全，停止继续扩展页面")
            break

        if not remaining or len(visited) >= max_pages_allowed:
            break
        if round_index + 1 >= rounds:
            break
        changed = False
        if allow_llm_link_select and settings.use_llm:
            changed = await _try_llm_select_pages(
                site, llm, crawler, visited, remaining, missing, memory, max_pages_allowed
            )
        else:
            changed = await _try_rule_select_pages(
                site, crawler, visited, remaining, missing, memory, max_pages_allowed
            )
        if not changed:
            break
        extra_used = max(0, len(visited) - settings.max_pages)
        if extra_used:
            memory["rep_pages_bonus_used"] = max(rep_bonus_used, extra_used)
        if missing == ["email"] and _email_key_pages_exhausted(visited, links_pool, memory):
            break
    return info

