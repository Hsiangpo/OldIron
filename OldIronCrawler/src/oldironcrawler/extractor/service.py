from __future__ import annotations

from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
import threading
import time

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.company_rules import clean_company_name_candidate, extract_company_name_fallback
from oldironcrawler.extractor.discovery_fallback import has_non_homepage_email_target
from oldironcrawler.extractor.email_page_selection import build_email_teacher_pool, pick_email_urls_or_empty
from oldironcrawler.extractor.email_rules import (
    collect_emails_for_pages,
    join_emails,
    merge_ai_emails_for_website,
)
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmExtractionResult, LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages, join_phones
from oldironcrawler.extractor.protocol_client import SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.protocol_runtime import DaemonProbeExecutor
from oldironcrawler.extractor.representative_search import ActiveRepresentativeSearchResult
from oldironcrawler.extractor.service_discovery import (
    DiscoverySnapshot,
    _build_discovery_snapshot,
    _build_reused_primary_pages,
    _collect_email_rule_pages,
    _collect_primary_email_rule_pages,
    _discover_value_snapshot,
    _fetch_email_overflow_pages,
    _fetch_primary_pages,
    _filter_network_primary_urls,
    _get_email_page_hard_limit,
    _get_email_page_soft_limit,
    _get_email_stop_same_domain_count,
    _get_page_total_hard_limit,
    _get_rep_page_limit,
    _has_enough_discovery_coverage,
    _merge_pages_into_map,
    _merge_unique_urls,
    _plan_fetch_targets,
    _resolve_discovery_deadline,
    _should_fetch_email_overflow_after_primary_fetch,
    _select_initial_primary_urls,
    _select_pages_from_map,
    _select_unfetched_primary_urls,
)
from oldironcrawler.extractor.shell_page import (
    build_shell_alias_map,
    canonicalize_shell_target_urls,
    replace_shell_pages_with_evidence,
)
from oldironcrawler.extractor.value_rules import (
    canonicalize_target_url,
    extract_learning_tokens,
    merge_representative_urls,
)
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore, SiteResult, SiteStageMetrics

_DEFAULT_AI_EMAIL_CONCURRENCY = 32
_DEFAULT_AI_EMAIL_TIMEOUT_SECONDS = 16.0
_AI_EMAIL_SEMAPHORE_LOCK = threading.Lock()
_AI_EMAIL_SEMAPHORE: threading.Semaphore | None = None
_AI_EMAIL_SEMAPHORE_LIMIT = 0
_AI_EMAIL_EXECUTOR_LOCK = threading.Lock()
_AI_EMAIL_EXECUTOR: DaemonProbeExecutor | None = None
_AI_EMAIL_EXECUTOR_LIMIT = 0
@dataclass
class SiteProcessingResult:
    result: SiteResult
    learning_feedback: "LearningFeedback"
    stage_metrics: SiteStageMetrics
@dataclass
class LearningFeedback:
    rep_positive_tokens: list[str]
    rep_negative_tokens: list[str]
    email_positive_tokens: list[str]
    email_negative_tokens: list[str]


@dataclass
class AiEmailFuture:
    future: Future
    started_monotonic: float


class SiteProfileService:
    def __init__(
        self,
        config: AppConfig,
        store: RuntimeStore,
        learning_store: GlobalLearningStore,
        llm_client: WebsiteLlmClient,
        page_pool: PageFetchPool,
        representative_searcher=None,
    ) -> None:
        self._config = config
        self._store = store
        self._learning_store = learning_store
        self._llm = llm_client
        self._page_pool = page_pool
        self._representative_searcher = representative_searcher

    def process(
        self,
        site_id: int,
        website: str,
        *,
        input_company_name: str = "",
        deadline_monotonic: float | None = None,
    ) -> SiteProcessingResult:
        metrics = SiteStageMetrics()
        collect_email_enabled = bool(getattr(self._config, "collect_email_enabled", True))
        collect_phone_enabled = bool(getattr(self._config, "collect_phone_enabled", True))
        collect_company_name_enabled = bool(getattr(self._config, "collect_company_name_enabled", True))
        extract_rep_enabled = bool(getattr(self._config, "extract_representative_enabled", True))
        input_company = str(input_company_name or "").strip()
        has_input_company = collect_company_name_enabled and bool(input_company)
        # AI 网页提取只在「需要补公司名」或「要提代表人」时才跑；
        # 表里已带公司名且关闭提取代表人时，这段 LLM 整体跳过。
        need_llm_extract = extract_rep_enabled or (collect_company_name_enabled and not has_input_company)
        need_contact_extract = collect_email_enabled or collect_phone_enabled
        rep_target_count = _get_rep_page_limit(self._config) if need_llm_extract else 0
        search_started = time.monotonic()
        search_future = _start_active_representative_search(
            self._representative_searcher,
            company_name=input_company if collect_company_name_enabled else "",
            website=website,
            deadline_monotonic=deadline_monotonic,
        )
        rep_learned = self._learning_store.load_scores("representative")
        email_learned = self._learning_store.load_scores("email")
        discovery_deadline_monotonic = _resolve_discovery_deadline(self._config, deadline_monotonic)
        discovery_protocol = SiteProtocolClient(
            _build_site_protocol_config(self._config, discovery_deadline_monotonic)
        )
        protocol: SiteProtocolClient | None = None
        try:
            try:
                discovery = self._time_call(
                    metrics,
                    "discover_ms",
                    lambda: _discover_value_snapshot(
                        discovery_protocol,
                        website,
                        rep_learned,
                        email_learned,
                        rep_target_count=rep_target_count,
                        contact_target_enabled=need_contact_extract,
                        discovery_deadline_monotonic=discovery_deadline_monotonic,
                    ),
                )
            finally:
                discovery_protocol.close()
            protocol = SiteProtocolClient(_build_site_protocol_config(self._config, deadline_monotonic))
            rep_urls = []
            if need_llm_extract:
                rep_urls = self._resolve_representative_urls(discovery, website, metrics, deadline_monotonic)
            email_urls = []
            if need_contact_extract:
                email_urls = self._resolve_email_urls(discovery, website, metrics, deadline_monotonic)
            fetch_plan = _plan_fetch_targets(self._config, website, rep_urls, email_urls)
            metrics.discovered_url_count = len(discovery.urls)
            metrics.rep_url_count = len(fetch_plan["rep_urls"])
            metrics.email_url_count = len(fetch_plan["email_primary_urls"]) + len(fetch_plan["email_overflow_urls"])
            metrics.target_url_count = len(fetch_plan["all_primary_urls"]) + len(fetch_plan["email_overflow_urls"])
            self._store.update_stage_metrics(site_id, metrics)
            page_map, rep_pages, llm_result = self._collect_budgeted_pages(
                protocol,
                website,
                fetch_plan,
                discovery.homepage_html,
                metrics,
                deadline_monotonic,
                need_llm_extract=need_llm_extract,
                collect_email_enabled=collect_email_enabled,
                collect_phone_enabled=collect_phone_enabled,
            )
            metrics.rep_url_count = len(rep_pages)
            metrics.fetched_page_count = len(page_map)
            self._store.update_stage_metrics(site_id, metrics)
            fetched_pages = list(page_map.values())
            if not collect_company_name_enabled:
                company_name = ""
            elif has_input_company:
                # 表里已经给了公司名，直接采用，不再让 AI 提取公司名。
                company_name = input_company
            else:
                company_name = clean_company_name_candidate(str(llm_result.company_name or "").strip())
                if not company_name:
                    company_name = clean_company_name_candidate(self._time_call(
                        metrics,
                        "company_rule_ms",
                        lambda: extract_company_name_fallback(
                            website,
                            [(page.url, page.html) for page in fetched_pages],
                        ),
                    ))
            if search_future is None and collect_company_name_enabled and not has_input_company and company_name:
                search_started = time.monotonic()
                search_future = _start_active_representative_search(
                    self._representative_searcher,
                    company_name=company_name,
                    website=website,
                    deadline_monotonic=deadline_monotonic,
                )
            email_rule_pages = _collect_email_rule_pages(page_map, fetch_plan) if need_contact_extract else []
            ai_email_future = None
            if need_contact_extract and collect_email_enabled:
                ai_email_future = _start_ai_email_future(
                    llm_client=self._llm,
                    homepage=website,
                    email_rule_pages=email_rule_pages,
                    deadline_monotonic=deadline_monotonic,
                    ai_email_concurrency=getattr(self._config, "ai_email_concurrency", _DEFAULT_AI_EMAIL_CONCURRENCY),
                    ai_email_timeout_seconds=getattr(
                        self._config,
                        "ai_email_timeout_seconds",
                        _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS,
                    ),
                )
            emails: list[str] = []
            email_sources: dict[str, list[str]] = {}
            phones: list[str] = []
            if need_contact_extract:
                emails, email_sources, phones, _phone_sources = self._time_call(
                    metrics,
                    "email_rule_ms",
                    lambda: _collect_contact_details(
                        website,
                        email_rule_pages,
                        collect_email_enabled=collect_email_enabled,
                        collect_phone_enabled=collect_phone_enabled,
                    ),
                )
                if collect_email_enabled:
                    emails = _merge_ai_email_future(
                        website=website,
                        rule_emails=emails,
                        email_rule_pages=email_rule_pages,
                        ai_email_future=ai_email_future,
                        metrics=metrics,
                        deadline_monotonic=deadline_monotonic,
                    )
                else:
                    emails = []
                    email_sources = {}
            searched_representative = _finish_active_representative_search(
                search_future,
                metrics,
                search_started,
                deadline_monotonic,
            )
            self._store.update_stage_metrics(site_id, metrics)
            # 关闭「提取代表人」时，丢弃 AI 抽到的代表人及其证据，只保留公司名。
            effective_representative = str(llm_result.representative or "").strip() if extract_rep_enabled else ""
            effective_evidence_url = str(llm_result.evidence_url or "").strip() if extract_rep_enabled else ""
            effective_evidence_quote = str(llm_result.evidence_quote or "").strip() if extract_rep_enabled else ""
            learning_feedback = build_learning_feedback(
                representative=effective_representative,
                evidence_url=effective_evidence_url,
                rep_urls=fetch_plan["rep_urls"],
                rep_fetched_urls=[page.url for page in rep_pages],
                emails=join_emails(emails),
                email_sources=list(email_sources.keys()),
                email_urls=[*fetch_plan["email_primary_urls"], *fetch_plan["email_overflow_urls"]],
                email_fetched_urls=[url for url, _html in email_rule_pages],
            )
            return SiteProcessingResult(
                result=SiteResult(
                    company_name=company_name,
                    representative=effective_representative,
                    emails=join_emails(emails),
                    website=website,
                    phones=join_phones(phones),
                    searched_representative=searched_representative.representative,
                    searched_representative_evidence_url=searched_representative.evidence_url,
                    searched_representative_confidence=searched_representative.confidence,
                    evidence_url=effective_evidence_url,
                    evidence_quote=effective_evidence_quote,
                ),
                learning_feedback=learning_feedback,
                stage_metrics=metrics,
            )
        except Exception:
            self._store.update_stage_metrics(site_id, metrics)
            raise
        finally:
            if protocol is not None:
                protocol.close()

    def _resolve_representative_urls(
        self,
        discovery: DiscoverySnapshot,
        website: str,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> list[str]:
        rep_urls = list(discovery.rep_urls)
        missing_count = max(_get_rep_page_limit(self._config) - len(rep_urls), 0)
        if missing_count <= 0 or not discovery.teacher_pool:
            return rep_urls
        extra_urls = self._time_call(
            metrics,
            "llm_pick_ms",
            lambda: self._llm.pick_representative_urls(
                homepage=website,
                candidate_urls=discovery.teacher_pool,
                target_count=missing_count,
                deadline_monotonic=deadline_monotonic,
            ),
        )
        return merge_representative_urls(rep_urls, extra_urls, limit=_get_rep_page_limit(self._config))

    def _resolve_email_urls(
        self,
        discovery: DiscoverySnapshot,
        website: str,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> list[str]:
        email_urls = list(discovery.email_urls)
        if has_non_homepage_email_target(website, email_urls):
            return email_urls
        candidate_urls = build_email_teacher_pool(discovery, website)
        if not candidate_urls:
            return email_urls
        target_count = max(_get_email_page_hard_limit(self._config) - len(email_urls), 1)
        picker_deadline = _resolve_ai_email_deadline(
            deadline_monotonic,
            ai_email_timeout_seconds=getattr(
                self._config,
                "ai_email_timeout_seconds",
                _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS,
            ),
        )
        picked_urls = self._time_call(
            metrics,
            "llm_pick_ms",
            lambda: pick_email_urls_or_empty(
                self._llm,
                homepage=website,
                candidate_urls=candidate_urls,
                existing_email_urls=email_urls,
                target_count=target_count,
                deadline_monotonic=picker_deadline,
            ),
        )
        allowed = set(candidate_urls)
        return _merge_unique_urls(
            email_urls,
            [url for url in picked_urls if url in allowed],
            limit=_get_email_page_hard_limit(self._config),
        )

    def _collect_budgeted_pages(
        self,
        protocol: SiteProtocolClient,
        website: str,
        fetch_plan: dict[str, list[str]],
        homepage_html: str,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
        *,
        need_llm_extract: bool = True,
        collect_email_enabled: bool = True,
        collect_phone_enabled: bool = True,
    ) -> tuple[dict[str, object], list, LlmExtractionResult]:
        primary_fetch_ms = 0
        overflow_fetch_ms = 0
        page_map: dict[str, object] = {}
        try:
            reused_primary_pages = _build_reused_primary_pages(website, fetch_plan, homepage_html)
            _merge_pages_into_map(page_map, reused_primary_pages)
            cascade_email_primary = collect_email_enabled and not collect_phone_enabled
            initial_primary_urls = _select_initial_primary_urls(
                fetch_plan,
                cascade_email_primary=cascade_email_primary,
            )
            primary_pages, primary_fetch_ms = _fetch_primary_pages(
                protocol,
                _filter_network_primary_urls(initial_primary_urls, reused_primary_pages),
                page_concurrency=self._config.page_concurrency,
                page_pool=self._page_pool,
            )
            _merge_pages_into_map(page_map, primary_pages)
            shell_alias_map = build_shell_alias_map(
                start_url=website,
                page_map=page_map,
                target_urls=[*fetch_plan["all_primary_urls"], *fetch_plan["email_overflow_urls"]],
            )
            primary_fetch_ms += replace_shell_pages_with_evidence(
                page_map,
                initial_primary_urls,
                proxy_url=self._config.proxy_url,
                timeout_seconds=self._config.request_timeout_seconds,
                deadline_monotonic=deadline_monotonic,
            )
            rep_pages = _select_pages_from_map(
                page_map,
                canonicalize_shell_target_urls(fetch_plan["rep_urls"], shell_alias_map),
            )
            if need_llm_extract:
                llm_result = self._extract_primary_representative(
                    website,
                    rep_pages,
                    metrics,
                    deadline_monotonic,
                )
            else:
                # 不需要 AI 抽公司名/代表人时，给一个空结果，后续走表内公司名 + 规则邮箱/电话。
                llm_result = LlmExtractionResult(
                    company_name="", representative="", evidence_url="", evidence_quote=""
                )
            remaining_primary_pages, remaining_fetch_ms = self._fetch_remaining_primary_pages_if_needed(
                protocol,
                website,
                fetch_plan,
                page_map,
                cascade_email_primary=cascade_email_primary,
            )
            primary_fetch_ms += remaining_fetch_ms
            _merge_pages_into_map(page_map, remaining_primary_pages)
            if remaining_primary_pages:
                remaining_primary_urls = [page.url for page in remaining_primary_pages]
                primary_fetch_ms += replace_shell_pages_with_evidence(
                    page_map,
                    remaining_primary_urls,
                    proxy_url=self._config.proxy_url,
                    timeout_seconds=self._config.request_timeout_seconds,
                    deadline_monotonic=deadline_monotonic,
                )
            overflow_pages, overflow_fetch_ms = self._fetch_email_overflow_pages_if_needed(
                protocol,
                website,
                fetch_plan,
                page_map,
            )
            _merge_pages_into_map(page_map, overflow_pages)
            if overflow_pages:
                overflow_fetch_ms += replace_shell_pages_with_evidence(
                    page_map,
                    fetch_plan["email_overflow_urls"],
                    proxy_url=self._config.proxy_url,
                    timeout_seconds=self._config.request_timeout_seconds,
                    deadline_monotonic=deadline_monotonic,
                )
            return page_map, rep_pages, llm_result
        finally:
            metrics.fetch_pages_ms = primary_fetch_ms + overflow_fetch_ms

    def _extract_primary_representative(
        self,
        website: str,
        rep_pages: list,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> LlmExtractionResult:
        llm_result = self._time_call(
            metrics,
            "llm_extract_ms",
            lambda: _extract_with_llm_or_empty(
                llm_client=self._llm,
                homepage=website,
                rep_pages=rep_pages,
                deadline_monotonic=deadline_monotonic,
            ),
        )
        return _normalize_llm_result(llm_result, rep_pages)

    def _merge_ai_emails(
        self,
        website: str,
        rule_emails: list[str],
        email_rule_pages: list[tuple[str, str]],
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> list[str]:
        # 规则已经命中邮箱时不再跑 AI；AI 只补救规则没有识别出的拆分/隐写邮箱。
        if rule_emails or not email_rule_pages:
            return rule_emails
        ai_emails = self._time_call(
            metrics,
            "ai_email_ms",
            lambda: _extract_ai_emails_or_empty(
                llm_client=self._llm,
                homepage=website,
                email_rule_pages=email_rule_pages,
                deadline_monotonic=deadline_monotonic,
                ai_email_concurrency=getattr(self._config, "ai_email_concurrency", _DEFAULT_AI_EMAIL_CONCURRENCY),
                ai_email_timeout_seconds=getattr(
                    self._config,
                    "ai_email_timeout_seconds",
                    _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS,
                ),
            ),
        )
        if not ai_emails:
            return rule_emails
        return merge_ai_emails_for_website(website, rule_emails, ai_emails, email_rule_pages)

    def _fetch_email_overflow_pages_if_needed(
        self,
        protocol: SiteProtocolClient,
        website: str,
        fetch_plan: dict[str, list[str]],
        page_map: dict[str, object],
    ) -> tuple[list, int]:
        if not _should_fetch_email_overflow_after_primary_fetch(
            website,
            _collect_primary_email_rule_pages(page_map, fetch_plan),
            fetch_plan["email_overflow_urls"],
            email_stop_same_domain_count=_get_email_stop_same_domain_count(self._config),
        ):
            return [], 0
        return _fetch_email_overflow_pages(
            protocol,
            fetch_plan,
            page_concurrency=self._config.page_concurrency,
            page_pool=self._page_pool,
        )

    def _fetch_remaining_primary_pages_if_needed(
        self,
        protocol: SiteProtocolClient,
        website: str,
        fetch_plan: dict[str, list[str]],
        page_map: dict[str, object],
        *,
        cascade_email_primary: bool,
    ) -> tuple[list, int]:
        if not cascade_email_primary:
            return [], 0
        remaining_urls = _select_unfetched_primary_urls(fetch_plan, page_map)
        if not _should_fetch_email_overflow_after_primary_fetch(
            website,
            _collect_primary_email_rule_pages(page_map, fetch_plan),
            remaining_urls,
            email_stop_same_domain_count=_get_email_stop_same_domain_count(self._config),
        ):
            return [], 0
        collected_pages: list = []
        elapsed_ms = 0
        for url in remaining_urls:
            if not _should_fetch_email_overflow_after_primary_fetch(
                website,
                _collect_primary_email_rule_pages(page_map, fetch_plan),
                [url],
                email_stop_same_domain_count=_get_email_stop_same_domain_count(self._config),
            ):
                break
            pages, fetch_ms = _fetch_primary_pages(
                protocol,
                [url],
                page_concurrency=1,
                page_pool=self._page_pool,
            )
            elapsed_ms += fetch_ms
            _merge_pages_into_map(page_map, pages)
            collected_pages.extend(pages)
        return collected_pages, elapsed_ms

    def _time_call(self, metrics: SiteStageMetrics, field_name: str, func):
        started = time.monotonic()
        try:
            return func()
        finally:
            elapsed_ms = int(round((time.monotonic() - started) * 1000))
            setattr(metrics, field_name, int(getattr(metrics, field_name, 0) or 0) + elapsed_ms)


def _merge_page_targets(rep_urls: list[str], email_urls: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*rep_urls, *email_urls]:
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result


def _start_active_representative_search(
    searcher,
    *,
    company_name: str,
    website: str,
    deadline_monotonic: float | None,
) -> Future | None:
    if searcher is None or not str(company_name or "").strip():
        return None
    submit = getattr(searcher, "submit", None)
    if not callable(submit):
        return None
    try:
        return submit(
            company_name=company_name,
            website=website,
            deadline_monotonic=deadline_monotonic,
        )
    except Exception:  # noqa: BLE001
        return None


def _finish_active_representative_search(
    future: Future | None,
    metrics: SiteStageMetrics,
    started_monotonic: float,
    deadline_monotonic: float | None,
):
    try:
        if future is None:
            metrics.search_rep_ms = 0
            return ActiveRepresentativeSearchResult()
        timeout = _remaining_deadline_seconds(deadline_monotonic)
        if timeout is not None and timeout <= 0:
            future.cancel()
            return ActiveRepresentativeSearchResult()
        try:
            result = future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            return ActiveRepresentativeSearchResult()
        except Exception:  # noqa: BLE001
            return ActiveRepresentativeSearchResult()
        return result or ActiveRepresentativeSearchResult()
    finally:
        if future is not None:
            metrics.search_rep_ms = int(round((time.monotonic() - started_monotonic) * 1000))
def _remaining_deadline_seconds(deadline_monotonic: float | None) -> float | None:
    if deadline_monotonic is None:
        return None
    return max(deadline_monotonic - time.monotonic(), 0.0)
def _collect_contact_details(
    website: str,
    email_rule_pages: list[tuple[str, str]],
    *,
    collect_email_enabled: bool = True,
    collect_phone_enabled: bool = True,
) -> tuple[list[str], dict[str, list[str]], list[str], dict[str, list[str]]]:
    if collect_email_enabled:
        emails, email_sources = collect_emails_for_pages(website, email_rule_pages)
    else:
        emails, email_sources = [], {}
    if collect_phone_enabled:
        phones, phone_sources = collect_phones_for_pages(email_rule_pages)
    else:
        phones, phone_sources = [], {}
    return emails, email_sources, phones, phone_sources
def _extract_ai_emails_or_empty(
    *,
    llm_client: WebsiteLlmClient,
    homepage: str,
    email_rule_pages: list[tuple[str, str]],
    deadline_monotonic: float | None,
    ai_email_concurrency: int = _DEFAULT_AI_EMAIL_CONCURRENCY,
    ai_email_timeout_seconds: float = _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS,
) -> list[str]:
    if not email_rule_pages:
        return []
    semaphore = _get_ai_email_semaphore(ai_email_concurrency)
    effective_deadline = _resolve_ai_email_deadline(
        deadline_monotonic,
        ai_email_timeout_seconds=ai_email_timeout_seconds,
    )
    timeout = _remaining_deadline_seconds(effective_deadline)
    if timeout is not None and timeout <= 0:
        return []
    acquired = False
    try:
        acquired = semaphore.acquire(timeout=timeout)
        if not acquired:
            return []
        return llm_client.extract_emails_from_pages(
            homepage=homepage,
            pages=[{"url": url, "html": html_text} for url, html_text in email_rule_pages],
            deadline_monotonic=effective_deadline,
        )
    except LlmConfigurationError:
        # 配额 / Key 类故障要冒泡出去触发统一的 Key 恢复，不在这里吞掉。
        raise
    except LlmTemporaryError:
        # AI 邮箱只是补充路径，上游临时不可用时不能让整站重跑。
        return []
    except Exception:  # noqa: BLE001
        # AI 邮箱只是增量，解析异常等就退回规则结果，别因此拖垮整站。
        return []
    finally:
        if acquired:
            semaphore.release()


def _start_ai_email_future(
    *,
    llm_client: WebsiteLlmClient,
    homepage: str,
    email_rule_pages: list[tuple[str, str]],
    deadline_monotonic: float | None,
    ai_email_concurrency: int = _DEFAULT_AI_EMAIL_CONCURRENCY,
    ai_email_timeout_seconds: float = _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS,
) -> AiEmailFuture | None:
    if not email_rule_pages:
        return None
    executor = _get_ai_email_executor(ai_email_concurrency)
    started = time.monotonic()
    try:
        future = executor.submit(
            _extract_ai_emails_or_empty,
            llm_client=llm_client,
            homepage=homepage,
            email_rule_pages=email_rule_pages,
            deadline_monotonic=deadline_monotonic,
            ai_email_concurrency=ai_email_concurrency,
            ai_email_timeout_seconds=ai_email_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        return None
    return AiEmailFuture(future=future, started_monotonic=started)


def _merge_ai_email_future(
    *,
    website: str,
    rule_emails: list[str],
    email_rule_pages: list[tuple[str, str]],
    ai_email_future: AiEmailFuture | None,
    metrics: SiteStageMetrics,
    deadline_monotonic: float | None,
) -> list[str]:
    if rule_emails:
        return rule_emails
    if ai_email_future is None:
        return rule_emails
    wait_started = time.monotonic()
    try:
        timeout = _remaining_deadline_seconds(deadline_monotonic)
        if timeout is not None and timeout <= 0:
            ai_email_future.future.cancel()
            return rule_emails
        ai_emails = ai_email_future.future.result(timeout=timeout)
    except FutureTimeoutError:
        ai_email_future.future.cancel()
        return rule_emails
    except LlmConfigurationError:
        raise
    except Exception:  # noqa: BLE001
        return rule_emails
    finally:
        metrics.ai_email_ms += int(round((time.monotonic() - wait_started) * 1000))
    if not ai_emails:
        return rule_emails
    return merge_ai_emails_for_website(website, rule_emails, ai_emails, email_rule_pages)


def _get_ai_email_semaphore(limit: int) -> threading.Semaphore:
    global _AI_EMAIL_SEMAPHORE, _AI_EMAIL_SEMAPHORE_LIMIT
    normalized_limit = min(max(int(limit or _DEFAULT_AI_EMAIL_CONCURRENCY), 1), 32)
    with _AI_EMAIL_SEMAPHORE_LOCK:
        if _AI_EMAIL_SEMAPHORE is None or _AI_EMAIL_SEMAPHORE_LIMIT != normalized_limit:
            _AI_EMAIL_SEMAPHORE = threading.Semaphore(normalized_limit)
            _AI_EMAIL_SEMAPHORE_LIMIT = normalized_limit
        return _AI_EMAIL_SEMAPHORE


def _get_ai_email_executor(limit: int) -> DaemonProbeExecutor:
    global _AI_EMAIL_EXECUTOR, _AI_EMAIL_EXECUTOR_LIMIT
    normalized_limit = min(max(int(limit or _DEFAULT_AI_EMAIL_CONCURRENCY), 1), 32)
    old_executor: DaemonProbeExecutor | None = None
    with _AI_EMAIL_EXECUTOR_LOCK:
        if _AI_EMAIL_EXECUTOR is not None and _AI_EMAIL_EXECUTOR_LIMIT == normalized_limit:
            return _AI_EMAIL_EXECUTOR
        old_executor = _AI_EMAIL_EXECUTOR
        _AI_EMAIL_EXECUTOR = DaemonProbeExecutor(max_workers=normalized_limit)
        _AI_EMAIL_EXECUTOR_LIMIT = normalized_limit
        executor = _AI_EMAIL_EXECUTOR
    if old_executor is not None:
        old_executor.shutdown(wait=False, cancel_futures=False)
    return executor


def _resolve_ai_email_deadline(
    deadline_monotonic: float | None,
    *,
    ai_email_timeout_seconds: float,
) -> float:
    timeout_seconds = min(max(float(ai_email_timeout_seconds or _DEFAULT_AI_EMAIL_TIMEOUT_SECONDS), 3.0), 45.0)
    ai_deadline = time.monotonic() + timeout_seconds
    if deadline_monotonic is None:
        return ai_deadline
    return min(deadline_monotonic, ai_deadline)


def _build_site_protocol_config(config: AppConfig, deadline_monotonic: float | None) -> SiteProtocolConfig:
    page_concurrency = max(int(getattr(config, "page_concurrency", 1) or 1), 1)
    page_worker_count = max(int(getattr(config, "page_worker_count", page_concurrency) or page_concurrency), 1)
    return SiteProtocolConfig(
        timeout_seconds=config.request_timeout_seconds,
        proxy_url=config.proxy_url,
        capsolver_api_key=config.capsolver_api_key,
        capsolver_api_base_url=config.capsolver_api_base_url,
        capsolver_proxy=config.capsolver_proxy,
        capsolver_poll_seconds=config.capsolver_poll_seconds,
        capsolver_max_wait_seconds=config.capsolver_max_wait_seconds,
        cloudflare_proxy_url=config.cloudflare_proxy_url,
        deadline_monotonic=deadline_monotonic,
        page_batch_timeout_seconds=_resolve_page_batch_timeout_seconds(config),
        common_probe_concurrency=page_concurrency,
        probe_worker_count=page_worker_count,
        request_slot_limit=page_worker_count,
    )


def _resolve_page_batch_timeout_seconds(config: AppConfig) -> float:
    """限制单轮目标页批抓时长，避免整站预算都耗在同一批卡住的子页上。"""

    request_timeout = max(float(getattr(config, "request_timeout_seconds", 10.0) or 10.0), 1.0)
    site_budget = max(
        float(getattr(config, "total_wait_seconds", request_timeout * 2) or 0.0),
        request_timeout * 2,
    )
    batch_ceiling = min(max(request_timeout * 2, 12.0), 20.0)
    return max(min(site_budget, batch_ceiling), min(request_timeout * 2, batch_ceiling))


def build_learning_feedback(
    *,
    representative: str,
    evidence_url: str,
    rep_urls: list[str],
    rep_fetched_urls: list[str],
    emails: str,
    email_sources: list[str],
    email_urls: list[str],
    email_fetched_urls: list[str],
) -> LearningFeedback:
    rep_positive_tokens = _collect_positive_rep_tokens(representative, evidence_url, rep_fetched_urls)
    rep_negative_tokens = _collect_failed_rep_negative_tokens(
        representative,
        evidence_url,
        rep_positive_tokens,
        rep_fetched_urls,
    )
    email_positive_tokens = _collect_positive_email_tokens(emails, email_sources, email_fetched_urls)
    email_negative_tokens = _collect_failed_email_negative_tokens(
        emails,
        email_positive_tokens,
        email_fetched_urls,
    )
    return LearningFeedback(
        rep_positive_tokens=rep_positive_tokens,
        rep_negative_tokens=rep_negative_tokens,
        email_positive_tokens=email_positive_tokens,
        email_negative_tokens=email_negative_tokens,
    )


def _merge_learning_tokens(urls: list[str]) -> list[str]:
    tokens: list[str] = []
    for url in urls:
        for token in extract_learning_tokens(url):
            if token not in tokens:
                tokens.append(token)
    return tokens


def _collect_failed_rep_negative_tokens(
    representative: str,
    evidence_url: str,
    positive_tokens: list[str],
    rep_fetched_urls: list[str],
) -> list[str]:
    return []


def _collect_failed_email_negative_tokens(
    emails: str,
    positive_tokens: list[str],
    email_fetched_urls: list[str],
) -> list[str]:
    return []


def _collect_positive_rep_tokens(representative: str, evidence_url: str, rep_fetched_urls: list[str]) -> list[str]:
    if not representative or not evidence_url:
        return []
    if evidence_url not in rep_fetched_urls:
        return []
    return extract_learning_tokens(evidence_url)


def _collect_positive_email_tokens(emails: str, email_sources: list[str], email_fetched_urls: list[str]) -> list[str]:
    if not emails:
        return []
    kept_sources = [url for url in email_sources if url in email_fetched_urls]
    return _merge_learning_tokens(kept_sources)


def _extract_with_llm_or_empty(
    *,
    llm_client: WebsiteLlmClient,
    homepage: str,
    rep_pages: list,
    deadline_monotonic: float | None,
) -> LlmExtractionResult:
    if not rep_pages:
        return LlmExtractionResult(company_name="", representative="", evidence_url="", evidence_quote="")
    return llm_client.extract_company_and_representative(
        homepage=homepage,
        pages=[{"url": page.url, "html": page.html} for page in rep_pages],
        deadline_monotonic=deadline_monotonic,
    )


def _normalize_llm_result(llm_result: LlmExtractionResult, rep_pages: list) -> LlmExtractionResult:
    available_urls = {
        canonicalize_target_url(page.url): page.url
        for page in rep_pages
        if str(page.url or "").strip()
    }
    raw_evidence_url = str(llm_result.evidence_url or "").strip()
    evidence_url = available_urls.get(canonicalize_target_url(raw_evidence_url), "")
    representative = str(llm_result.representative or "").strip() if evidence_url else ""
    evidence_quote = str(llm_result.evidence_quote or "").strip() if representative else ""
    return LlmExtractionResult(
        company_name=str(llm_result.company_name or "").strip() if rep_pages else "",
        representative=representative,
        evidence_url=evidence_url,
        evidence_quote=evidence_quote,
    )
