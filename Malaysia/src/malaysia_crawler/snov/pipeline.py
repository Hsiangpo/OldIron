"""CTOS + BusinessList + Snov 串联管道。"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from datetime import timezone
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.common.io_utils import CsvAppender
from malaysia_crawler.common.io_utils import append_jsonl
from malaysia_crawler.common.io_utils import ensure_dir
from malaysia_crawler.ctos_directory.crawler import CTOSDirectoryCrawler
from malaysia_crawler.snov.client import SnovClient
from malaysia_crawler.snov.client import extract_domain


def _normalize_company_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _merge_contact_emails(contact_email: str, snov_emails: list[str]) -> list[str]:
    merged: list[str] = []
    if contact_email.strip():
        merged.append(contact_email.strip().lower())
    for email in snov_emails:
        value = email.strip().lower()
        if value:
            merged.append(value)
    # 中文注释：去重并保持顺序。
    return list(dict.fromkeys(merged))


class _BusinessListSource(Protocol):
    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None: ...


class _ManagerAgentSource(Protocol):
    def enrich_manager(
        self,
        *,
        company_name: str,
        domain: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ): ...


@dataclass(slots=True)
class _PipelineCandidate:
    company: BusinessListCompany
    domain: str


@dataclass(slots=True)
class _ManagerStats:
    from_businesslist: int = 0
    from_firecrawl_llm: int = 0
    from_cache_success: int = 0
    missing_after_fallback: int = 0
    fallback_attempted: int = 0
    from_cache_miss: int = 0


class _ManagerDomainCache:
    """域名级缓存：成功长期缓存，失败按 TTL 缓存。"""

    def __init__(self, *, cache_file: Path, miss_ttl_seconds: int = 24 * 3600) -> None:
        self.cache_file = cache_file
        self.miss_ttl_seconds = max(miss_ttl_seconds, 60)
        self._records: dict[str, dict[str, object]] = {}
        self._load()

    def _load(self) -> None:
        if not self.cache_file.exists():
            return
        for raw in self.cache_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            text = raw.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain", "")).strip().lower()
            if not domain:
                continue
            self._records[domain] = item

    def get(self, domain: str) -> dict[str, object] | None:
        key = domain.strip().lower()
        if not key:
            return None
        row = self._records.get(key)
        if row is None:
            return None
        status = str(row.get("status", "")).strip()
        if status == "success":
            return row
        if status == "miss":
            updated_at = self._parse_time(str(row.get("updated_at", "")).strip())
            if updated_at is None:
                return None
            age = datetime.now(timezone.utc).timestamp() - updated_at
            if age <= self.miss_ttl_seconds:
                return row
        return None

    def put_success(self, *, domain: str, manager_name: str, manager_role: str) -> None:
        payload: dict[str, object] = {
            "domain": domain.strip().lower(),
            "status": "success",
            "manager_name": manager_name.strip(),
            "manager_role": manager_role.strip(),
            "updated_at": self._now_iso(),
        }
        self._upsert(payload)

    def put_miss(self, *, domain: str, error_code: str) -> None:
        payload: dict[str, object] = {
            "domain": domain.strip().lower(),
            "status": "miss",
            "error_code": error_code.strip(),
            "updated_at": self._now_iso(),
        }
        self._upsert(payload)

    def _upsert(self, payload: dict[str, object]) -> None:
        domain = str(payload.get("domain", "")).strip().lower()
        if not domain:
            return
        self._records[domain] = payload
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        with self.cache_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_time(self, text: str) -> float | None:
        if not text:
            return None
        try:
            dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return None
        return dt.replace(tzinfo=timezone.utc).timestamp()


def _build_output_row(
    *,
    company: BusinessListCompany,
    domain: str,
    snov_emails: list[str],
) -> dict[str, str]:
    contact_emails = _merge_contact_emails(company.contact_email, snov_emails)
    return {
        "company_name": company.company_name,
        "domain": domain,
        "contact_eamils": json.dumps(contact_emails, ensure_ascii=False),
        "company_manager": company.company_manager,
    }


class CtosBusinessListSnovPipeline:
    """用 CTOS 公司名过滤 BusinessList，然后调用 Snov 补邮箱。"""

    def __init__(
        self,
        *,
        ctos_crawler: CTOSDirectoryCrawler,
        businesslist_crawler: _BusinessListSource,
        snov_client: SnovClient,
        manager_agent: _ManagerAgentSource | None = None,
        manager_enrich_max_rounds: int = 3,
        manager_enrich_workers: int = 8,
        manager_cache_ttl_seconds: int = 24 * 3600,
    ) -> None:
        self.ctos_crawler = ctos_crawler
        self.businesslist_crawler = businesslist_crawler
        self.snov_client = snov_client
        self.manager_agent = manager_agent
        self.manager_enrich_max_rounds = max(manager_enrich_max_rounds, 1)
        self.manager_enrich_workers = max(manager_enrich_workers, 1)
        self.manager_cache_ttl_seconds = max(manager_cache_ttl_seconds, 60)

    def _stage_a_candidates(self, domain: str) -> list[str]:
        base = f"https://{domain}"
        return [base + "/about", base + "/team"]

    def _stage_b_candidates(self, domain: str) -> list[str]:
        base = f"https://{domain}"
        paths = [
            "/management",
            "/leadership",
            "/directors",
            "/board",
            "/company",
        ]
        return [base + path for path in paths]

    def _extract_error_code(self, exc: Exception) -> str:
        code = getattr(exc, "code", "")
        if isinstance(code, str) and code.strip():
            return code.strip()
        text = str(exc)
        if "firecrawl_429" in text:
            return "firecrawl_429"
        if "firecrawl_402" in text:
            return "firecrawl_402"
        if "firecrawl_401" in text:
            return "firecrawl_401"
        return ""

    def _run_manager_stage(
        self,
        *,
        company_name: str,
        domain: str,
        candidates: list[str],
        rounds: int,
    ) -> tuple[str, str]:
        tried_urls: list[str] = []
        for _ in range(max(rounds, 1)):
            try:
                result = self.manager_agent.enrich_manager(
                    company_name=company_name,
                    domain=domain,
                    candidate_pool=candidates,
                    tried_urls=tried_urls,
                )
            except Exception as exc:  # noqa: BLE001
                code = self._extract_error_code(exc)
                if code in {"firecrawl_429", "firecrawl_5xx"}:
                    continue
                return "", code or "unknown_error"
            tried_urls = list(result.tried_urls)
            if result.success and result.manager_name.strip():
                return result.manager_name.strip(), "firecrawl_llm"
            if result.error_code in {"firecrawl_429", "firecrawl_5xx"}:
                continue
            if result.error_code in {"firecrawl_401", "firecrawl_402"}:
                return "", result.error_code
            if result.error_code and result.error_code != "manager_not_found":
                return "", result.error_code
        return "", "manager_not_found"

    def _fill_manager(
        self,
        *,
        company_name: str,
        domain: str,
        manager: str,
        cache: _ManagerDomainCache,
    ) -> tuple[str, str]:
        if manager.strip():
            return manager.strip(), "businesslist"
        if self.manager_agent is None:
            return "", "missing"
        cached = cache.get(domain)
        if cached is not None:
            if str(cached.get("status", "")) == "success":
                cached_name = str(cached.get("manager_name", "")).strip()
                if cached_name:
                    return cached_name, "cache_success"
            return "", "cache_miss"
        stage_a_rounds = 1
        stage_b_rounds = 1
        manager_name, status = self._run_manager_stage(
            company_name=company_name,
            domain=domain,
            candidates=self._stage_a_candidates(domain),
            rounds=stage_a_rounds,
        )
        if manager_name:
            cache.put_success(domain=domain, manager_name=manager_name, manager_role="Manager")
            return manager_name, "firecrawl_llm"
        if status in {"firecrawl_401", "firecrawl_402"}:
            cache.put_miss(domain=domain, error_code=status)
            return "", "missing"
        manager_name, status = self._run_manager_stage(
            company_name=company_name,
            domain=domain,
            candidates=self._stage_b_candidates(domain),
            rounds=stage_b_rounds,
        )
        if manager_name:
            cache.put_success(domain=domain, manager_name=manager_name, manager_role="Manager")
            return manager_name, "firecrawl_llm"
        cache.put_miss(domain=domain, error_code=status)
        return "", "missing"

    def _load_ctos_names(self, *, prefixes: str, max_pages_per_prefix: int) -> set[str]:
        names: set[str] = set()
        for prefix in prefixes:
            if not prefix.isalnum():
                continue
            for page in range(1, max_pages_per_prefix + 1):
                page_data = self.ctos_crawler.fetch_list_page(prefix.lower(), page)
                if not page_data.companies:
                    break
                for item in page_data.companies:
                    normalized = _normalize_company_name(item.company_name)
                    if normalized:
                        names.add(normalized)
        return names

    def _collect_candidates(
        self,
        *,
        target_companies: int,
        businesslist_start_id: int,
        businesslist_end_id: int,
        require_ctos_match: bool,
        ctos_names: set[str],
    ) -> tuple[list[_PipelineCandidate], int]:
        scanned_ids = 0
        candidates: list[_PipelineCandidate] = []
        for company_id in range(businesslist_start_id, businesslist_end_id + 1):
            if len(candidates) >= target_companies:
                break
            scanned_ids += 1
            company = self.businesslist_crawler.fetch_company_profile(company_id)
            if company is None:
                continue
            normalized = _normalize_company_name(company.company_name)
            ctos_name_matched = bool(normalized and normalized in ctos_names)
            if require_ctos_match and not ctos_name_matched:
                continue
            domain = extract_domain(company.website_url)
            if not domain:
                continue
            candidates.append(_PipelineCandidate(company=company, domain=domain))
        return candidates, scanned_ids

    def _enrich_and_export_rows(
        self,
        *,
        candidates: list[_PipelineCandidate],
        cache: _ManagerDomainCache,
        jsonl_path: Path,
        csv_writer: CsvAppender,
    ) -> tuple[_ManagerStats, int, int]:
        stats = _ManagerStats()
        missing_indices = [idx for idx, item in enumerate(candidates) if not item.company.company_manager.strip()]
        for item in candidates:
            if item.company.company_manager.strip():
                stats.from_businesslist += 1
        snov_results: list[list[str] | None] = [None] * len(candidates)
        futures: dict[object, int] = {}
        manager_pool: ThreadPoolExecutor | None = None
        if self.manager_agent is not None and missing_indices:
            stats.fallback_attempted = len(missing_indices)
            worker_count = min(self.manager_enrich_workers, len(missing_indices))
            manager_pool = ThreadPoolExecutor(max_workers=max(worker_count, 1))
            futures = {
                manager_pool.submit(
                    self._fill_manager,
                    company_name=candidates[idx].company.company_name,
                    domain=candidates[idx].domain,
                    manager="",
                    cache=cache,
                ): idx
                for idx in missing_indices
            }
        if not missing_indices:
            stats.fallback_attempted = 0
        elif self.manager_agent is None:
            stats.missing_after_fallback = len(missing_indices)
            stats.fallback_attempted = len(missing_indices)
        # 中文注释：先跑 Snov，期间 manager 线程池并行执行。
        for idx, item in enumerate(candidates):
            try:
                snov_results[idx] = self.snov_client.get_domain_emails(item.domain)
            except Exception:  # noqa: BLE001
                snov_results[idx] = None
        if manager_pool is not None:
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    manager, source = future.result()
                except Exception:  # noqa: BLE001
                    manager, source = "", "missing"
                candidates[idx].company.company_manager = manager
                if source == "firecrawl_llm":
                    stats.from_firecrawl_llm += 1
                elif source == "cache_success":
                    stats.from_cache_success += 1
                elif source == "cache_miss":
                    stats.from_cache_miss += 1
                    stats.missing_after_fallback += 1
                else:
                    stats.missing_after_fallback += 1
            manager_pool.shutdown(wait=True)
        matched_companies = 0
        snov_enriched = 0
        for idx, item in enumerate(candidates):
            snov_emails = snov_results[idx]
            if snov_emails is None:
                continue
            row = _build_output_row(
                company=item.company,
                domain=item.domain,
                snov_emails=snov_emails,
            )
            append_jsonl(jsonl_path, row)
            csv_writer.write_row(row)
            matched_companies += 1
            snov_enriched += 1
        return stats, matched_companies, snov_enriched

    def run(
        self,
        *,
        output_dir: str | Path,
        target_companies: int,
        ctos_prefixes: str,
        ctos_max_pages_per_prefix: int,
        businesslist_start_id: int,
        businesslist_end_id: int,
        require_ctos_match: bool = False,
    ) -> dict[str, float | int]:
        if target_companies <= 0:
            raise ValueError("target_companies 必须大于 0。")
        if businesslist_end_id < businesslist_start_id:
            raise ValueError("BusinessList company_id 区间非法。")

        started = time.perf_counter()
        output = ensure_dir(output_dir)
        jsonl_path = output / "ctos_businesslist_snov.jsonl"
        cache = _ManagerDomainCache(
            cache_file=output / "manager_cache.jsonl",
            miss_ttl_seconds=self.manager_cache_ttl_seconds,
        )
        csv_writer = CsvAppender(
            output / "ctos_businesslist_snov.csv",
            ["company_name", "domain", "contact_eamils", "company_manager"],
        )
        try:
            ctos_names = self._load_ctos_names(
                prefixes=ctos_prefixes,
                max_pages_per_prefix=ctos_max_pages_per_prefix,
            )
            candidates, scanned_ids = self._collect_candidates(
                target_companies=target_companies,
                businesslist_start_id=businesslist_start_id,
                businesslist_end_id=businesslist_end_id,
                require_ctos_match=require_ctos_match,
                ctos_names=ctos_names,
            )
            manager_stats, matched_companies, snov_enriched = self._enrich_and_export_rows(
                candidates=candidates,
                cache=cache,
                jsonl_path=jsonl_path,
                csv_writer=csv_writer,
            )
        finally:
            closer = getattr(self.businesslist_crawler, "close", None)
            if callable(closer):
                closer()
            csv_writer.close()

        elapsed_seconds = round(time.perf_counter() - started, 3)
        return {
            "ctos_name_pool": len(ctos_names),
            "scanned_ids": scanned_ids,
            "matched_companies": matched_companies,
            "snov_enriched": snov_enriched,
            "manager_from_businesslist": manager_stats.from_businesslist,
            "manager_from_firecrawl_llm": manager_stats.from_firecrawl_llm,
            "manager_from_cache_success": manager_stats.from_cache_success,
            "manager_from_cache_miss": manager_stats.from_cache_miss,
            "manager_missing_after_fallback": manager_stats.missing_after_fallback,
            "manager_fallback_attempted": manager_stats.fallback_attempted,
            "elapsed_seconds": elapsed_seconds,
        }
