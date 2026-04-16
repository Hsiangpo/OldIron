"""Snov 业务封装：域名邮箱 + 人员筛选。"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oldiron_core.delivery.engine import extract_domain
from oldiron_core.fc_email.email_service import DEFAULT_LLM_API_STYLE
from oldiron_core.fc_email.email_service import DEFAULT_LLM_BASE_URL
from oldiron_core.fc_email.email_service import DEFAULT_LLM_MODEL
from oldiron_core.fc_email.email_service import DEFAULT_LLM_REASONING_EFFORT
from oldiron_core.fc_email.llm_client import EmailUrlLlmClient
from oldiron_core.fc_email.normalization import normalize_email_candidate

from .client import SnovClient
from .client import SnovClientConfig
from .client import SnovProspect


LOGGER = logging.getLogger("oldiron_core.snov.service")
_LEADER_PATTERNS = (
    (r"\bchief executive officer\b|\bceo\b", 300),
    (r"\bmanaging director\b", 260),
    (r"\bpresident\b", 240),
    (r"\bchairman\b", 230),
    (r"\bfounder\b", 220),
    (r"\bowner\b", 210),
    (r"\bpartner\b", 200),
    (r"\bdirector\b", 150),
    (r"\bhead\b", 120),
)
_FINANCE_PATTERNS = (
    (r"\bcfo\b|\bchief financial officer\b", 260),
    (r"\bfinance director\b", 240),
    (r"\bhead of finance\b", 220),
    (r"\bfinance manager\b", 200),
    (r"\bcontroller\b", 180),
    (r"\bvp finance\b|\bvice president finance\b", 170),
)
_ACCOUNTING_PATTERNS = (
    (r"\bchief accountant\b", 250),
    (r"\baccounting manager\b", 230),
    (r"\bhead of accounting\b", 220),
    (r"\baccountant\b", 180),
    (r"\baccounts manager\b", 170),
    (r"\bfinance and accounts\b", 170),
)


@dataclass(slots=True)
class SnovServiceSettings:
    """Snov 业务配置。"""

    client_config: SnovClientConfig
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_reasoning_effort: str
    llm_api_style: str
    llm_timeout_seconds: float

    @classmethod
    def from_env(cls) -> SnovServiceSettings:
        return cls(
            client_config=SnovClientConfig.from_env(),
            llm_api_key=str(os.getenv("LLM_API_KEY", "") or "").strip(),
            llm_base_url=str(os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL) or DEFAULT_LLM_BASE_URL).strip(),
            llm_model=str(os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL) or DEFAULT_LLM_MODEL).strip(),
            llm_reasoning_effort=str(os.getenv("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT) or "").strip(),
            llm_api_style=str(os.getenv("LLM_API_STYLE", DEFAULT_LLM_API_STYLE) or DEFAULT_LLM_API_STYLE).strip(),
            llm_timeout_seconds=max(float(os.getenv("LLM_TIMEOUT_SECONDS", "120") or 120), 30.0),
        )

    def validate(self, *, require_llm: bool = True) -> None:
        self.client_config.validate()
        if require_llm and not self.llm_api_key:
            raise RuntimeError("缺少 LLM_API_KEY，Snov 联系人筛选无法运行。")


@dataclass(slots=True)
class SnovContact:
    """最终落盘的人物。"""

    name: str
    title_raw: str
    title_zh: str
    emails: list[str]


@dataclass(slots=True)
class SnovDiscoveryResult:
    """单公司 Snov 结果。"""

    website: str
    domain_emails: list[str]
    people: list[SnovContact]

    @property
    def representative_names(self) -> str:
        return ";".join(contact.name for contact in self.people if contact.name)

    @property
    def people_json(self) -> str:
        payload = [
            {
                "name": contact.name,
                "title_zh": contact.title_zh,
                "emails": list(contact.emails),
            }
            for contact in self.people
        ]
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


@dataclass(slots=True)
class _Candidate:
    """LLM 候选。"""

    name: str
    title_raw: str
    lookup_key: str
    leader_score: int
    finance_score: int
    accounting_score: int


class SnovService:
    """组合 Snov API 与 LLM。"""

    def __init__(
        self,
        settings: SnovServiceSettings,
        *,
        client: SnovClient | None = None,
        llm_client: EmailUrlLlmClient | None = None,
    ) -> None:
        settings.validate(require_llm=True)
        self._settings = settings
        self._client = client or SnovClient(settings.client_config)
        self._owns_client = client is None
        self._llm = llm_client or EmailUrlLlmClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
            reasoning_effort=settings.llm_reasoning_effort,
            api_style=settings.llm_api_style,
            timeout_seconds=settings.llm_timeout_seconds,
        )
        self._owns_llm = llm_client is None

    def close(self) -> None:
        if self._owns_llm:
            self._llm.close()
        if self._owns_client:
            self._client.close()

    def discover_company(self, *, company_name: str, homepage: str) -> SnovDiscoveryResult:
        website = self._resolve_website(company_name=company_name, homepage=homepage)
        domain = extract_domain(website)
        if not domain:
            return SnovDiscoveryResult(website="", domain_emails=[], people=[])
        count = self._client.get_domain_emails_count(domain)
        LOGGER.info("Snov 域名邮箱计数：company=%s domain=%s count=%s", company_name, domain, count)
        prospects = self._client.fetch_prospects(domain)
        selected_people = self._hydrate_selected_people(company_name=company_name, prospects=prospects)
        domain_emails = _merge_emails(
            self._client.fetch_domain_emails(domain),
            self._client.fetch_generic_contacts(domain),
            [email for person in selected_people for email in person.emails],
        )
        return SnovDiscoveryResult(website=website, domain_emails=domain_emails, people=selected_people)

    def _resolve_website(self, *, company_name: str, homepage: str) -> str:
        domain = extract_domain(homepage)
        if domain:
            return _ensure_https_url(homepage, domain)
        fallback_domain = self._client.company_domain_by_name(company_name)
        if not fallback_domain:
            return ""
        return f"https://{fallback_domain}"

    def _hydrate_selected_people(self, *, company_name: str, prospects: list[SnovProspect]) -> list[SnovContact]:
        selected = self._select_contacts(company_name=company_name, prospects=prospects)
        results: list[SnovContact] = []
        for candidate in selected:
            emails = []
            if candidate.lookup_key:
                prospect = next((item for item in prospects if _prospect_lookup_key(item) == candidate.lookup_key), None)
                if prospect is not None:
                    emails = self._client.fetch_prospect_emails(prospect)
            results.append(
                SnovContact(
                    name=candidate.name,
                    title_raw=candidate.title_raw,
                    title_zh=candidate.title_zh,
                    emails=emails,
                )
            )
        return results

    def _select_contacts(self, *, company_name: str, prospects: list[SnovProspect]) -> list[_SelectedCandidate]:
        candidates = _build_candidates(prospects)
        if not candidates:
            return []
        prompt = _build_selector_prompt(company_name=company_name, candidates=candidates)
        try:
            payload = self._llm._call_json(prompt)  # noqa: SLF001
            selected = _parse_selected_candidates(payload, candidates)
            if selected:
                return selected
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Snov 联系人 LLM 选择失败，回退规则：company=%s error=%s", company_name, exc)
        return _fallback_selected_candidates(candidates)


@dataclass(slots=True)
class _SelectedCandidate:
    """LLM 选中的人。"""

    name: str
    title_raw: str
    title_zh: str
    lookup_key: str


def _build_candidates(prospects: list[SnovProspect]) -> list[_Candidate]:
    results: list[_Candidate] = []
    seen: set[tuple[str, str]] = set()
    for prospect in prospects:
        name = str(prospect.name or "").strip()
        title = str(prospect.title or "").strip()
        if not name or not title:
            continue
        lookup_key = _prospect_lookup_key(prospect)
        leader_score = _score_patterns(title, _LEADER_PATTERNS)
        finance_score = _score_patterns(title, _FINANCE_PATTERNS)
        accounting_score = _score_patterns(title, _ACCOUNTING_PATTERNS)
        if max(leader_score, finance_score, accounting_score) <= 0:
            continue
        key = (name.lower(), title.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append(
            _Candidate(
                name=name,
                title_raw=title,
                lookup_key=lookup_key,
                leader_score=leader_score,
                finance_score=finance_score,
                accounting_score=accounting_score,
            )
        )
    results.sort(key=lambda item: (-max(item.leader_score, item.finance_score, item.accounting_score), item.name.lower()))
    return results[:200]


def _score_patterns(title: str, patterns: tuple[tuple[str, int], ...]) -> int:
    lowered = str(title or "").strip().lower()
    best = 0
    for pattern, score in patterns:
        if re.search(pattern, lowered, flags=re.I):
            best = max(best, score)
    return best


def _build_selector_prompt(*, company_name: str, candidates: list[_Candidate]) -> str:
    payload = [
        {
            "name": item.name,
            "raw_title": item.title_raw,
            "leader_score": item.leader_score,
            "finance_score": item.finance_score,
            "accounting_score": item.accounting_score,
        }
        for item in candidates
    ]
    return (
        "你是企业联系人筛选器。\n"
        "任务：只从候选名单里选出最重要的 4 个高层角色，再单独选财务负责人和会计负责人。\n"
        "leaders 最多 4 人，按权重从高到低排序。\n"
        "finance 只选最像财务负责人（CFO/Finance Director/Head of Finance/Finance Manager/Controller 等）。\n"
        "accounting 只选最像会计负责人（Chief Accountant/Accounting Manager/Accountant/Finance & Accounts 等）。\n"
        "必须原样返回 name 和 raw_title，不可编造；同时把职位翻译成中文 title_zh。\n"
        "如果没有合适人选，该字段返回空对象或空数组。\n"
        '返回 JSON：{"leaders":[{"name":"","raw_title":"","title_zh":""}],"finance":{"name":"","raw_title":"","title_zh":""},"accounting":{"name":"","raw_title":"","title_zh":""}}\n'
        f"公司：{company_name}\n"
        f"候选：{json.dumps(payload, ensure_ascii=False)}"
    )


def _parse_selected_candidates(payload: dict[str, Any], candidates: list[_Candidate]) -> list[_SelectedCandidate]:
    lookup = {(item.name, item.title_raw): item for item in candidates}
    results: list[_SelectedCandidate] = []
    seen: set[str] = set()
    for item in _extract_selected_items(payload):
        name = str(item.get("name") or "").strip()
        raw_title = str(item.get("raw_title") or "").strip()
        title_zh = str(item.get("title_zh") or "").strip()
        candidate = lookup.get((name, raw_title))
        if candidate is None or candidate.lookup_key in seen:
            continue
        seen.add(candidate.lookup_key)
        results.append(
            _SelectedCandidate(
                name=candidate.name,
                title_raw=candidate.title_raw,
                title_zh=title_zh or candidate.title_raw,
                lookup_key=candidate.lookup_key,
            )
        )
    return results


def _extract_selected_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    leaders = payload.get("leaders")
    if isinstance(leaders, list):
        results.extend(item for item in leaders if isinstance(item, dict))
    for key in ("finance", "accounting"):
        item = payload.get(key)
        if isinstance(item, dict):
            results.append(item)
    return results


def _fallback_selected_candidates(candidates: list[_Candidate]) -> list[_SelectedCandidate]:
    leaders = sorted(candidates, key=lambda item: (-item.leader_score, item.name.lower()))
    finance = sorted(candidates, key=lambda item: (-item.finance_score, item.name.lower()))
    accounting = sorted(candidates, key=lambda item: (-item.accounting_score, item.name.lower()))
    picked: list[_Candidate] = []
    seen: set[str] = set()
    for candidate in leaders[:4]:
        if candidate.leader_score <= 0 or candidate.lookup_key in seen:
            continue
        seen.add(candidate.lookup_key)
        picked.append(candidate)
    for candidate in [finance[0] if finance else None, accounting[0] if accounting else None]:
        if candidate is None or max(candidate.finance_score, candidate.accounting_score) <= 0:
            continue
        if candidate.lookup_key in seen:
            continue
        seen.add(candidate.lookup_key)
        picked.append(candidate)
    return [
        _SelectedCandidate(
            name=item.name,
            title_raw=item.title_raw,
            title_zh=_fallback_title_zh(item.title_raw),
            lookup_key=item.lookup_key,
        )
        for item in picked
    ]


def _fallback_title_zh(title: str) -> str:
    lowered = str(title or "").lower()
    if "chief executive officer" in lowered or re.search(r"\bceo\b", lowered):
        return "首席执行官"
    if "managing director" in lowered:
        return "总经理"
    if "chairman" in lowered:
        return "董事长"
    if "president" in lowered:
        return "总裁"
    if "founder" in lowered:
        return "创始人"
    if "owner" in lowered:
        return "所有者"
    if "chief financial officer" in lowered or re.search(r"\bcfo\b", lowered):
        return "首席财务官"
    if "finance director" in lowered:
        return "财务总监"
    if "finance manager" in lowered:
        return "财务经理"
    if "chief accountant" in lowered:
        return "总会计师"
    if "accounting manager" in lowered:
        return "会计经理"
    if "accountant" in lowered:
        return "会计"
    return title


def _merge_emails(*groups: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group:
            clean = normalize_email_candidate(item)
            lowered = clean.lower()
            if not clean or lowered in seen:
                continue
            seen.add(lowered)
            merged.append(clean)
    return merged


def _ensure_https_url(homepage: str, domain: str) -> str:
    raw = str(homepage or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return f"https://{domain}"


def _prospect_lookup_key(prospect: SnovProspect) -> str:
    if prospect.prospect_hash:
        return f"hash:{prospect.prospect_hash}"
    return f"name:{prospect.name.lower()}|{prospect.title.lower()}"
