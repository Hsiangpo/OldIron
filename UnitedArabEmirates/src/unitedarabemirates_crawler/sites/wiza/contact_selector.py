"""Wiza 联系人代表人选择。"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oldiron_core.fc_email.llm_client import EmailUrlLlmClient

from ..common.enrich import build_email_settings
from ..common.enrich import normalize_person_name


_DIRECT_CEO_PATTERNS = (
    r"\bchief executive officer\b",
    r"\bceo\b",
    r"\bceo of\b",
    r"\bchief executive\b",
    r"\bco[- ]chief executive officer\b",
    r"\bco[- ]ceo\b",
    r"\bacting chief executive officer\b",
    r"\bcountry chief executive officer\b",
    r"président directeur général",
    r"president directeur general",
    r"\bpresident and chief executive officer\b",
    r"\bpresident\b.*\bceo\b",
    r"\bchairman\b.*\bchief executive\b",
    r"\bchairman\b.*\bchief executive officer\b",
    r"\bchairman\b.*\bgroup chief executive\b",
    r"\bchairman\b.*\bceo\b",
    r"\bmanaging director\b.*\bceo\b",
    r"\bmanaging director\b.*\bchief executive officer\b",
    r"\bchief executive officer\b.*\bmanaging director\b",
    r"\bchief executive and managing director\b",
    r"\bgroup ceo\b",
    r"\bgroup chief executive\b",
    r"\bgroup chief executive officer\b",
)
_BLOCK_TITLE_PATTERNS = (
    r"\bformer\b",
    r"\bdeputy\b",
    r"\bassistant to\b",
    r"\bassistance\b",
    r"\bmanager to\b",
    r"chief executive officer[’']?s office",
    r"chief executive officer office",
    r"\bceo office\b",
    r"chief executive director",
    r"\bexecutive office\b",
    r"\bcfo\b",
    r"\bcoo\b",
    r"\bcto\b",
    r"\bcio\b",
    r"\bcmo\b",
    r"\bcro\b",
    r"\bcao\b",
    r"\bcso\b",
    r"\bcgo\b",
    r"\bccoo\b",
    r"\bcsso\b",
    r"\bciso\b",
    r"\bchro\b",
    r"chief financial",
    r"chief operating",
    r"chief technology",
    r"chief information",
    r"chief marketing",
    r"chief revenue",
    r"chief sales",
    r"chief product",
    r"chief people",
    r"chief human",
    r"chief security",
    r"chief compliance",
    r"chief legal",
    r"chief commercial",
    r"chief experience",
    r"chief consumer",
    r"chief cargo",
    r"chief customer",
    r"chief communications",
    r"chief digital",
    r"chief data",
    r"chief transformation",
    r"chief procurement",
    r"chief sustainability",
    r"chief strategy",
    r"chief audit",
    r"chief credit",
    r"chief platforms",
    r"chief voice",
    r"chief government",
    r"chief guest",
    r"chief operations",
    r"chief operations officer",
    r"chief people",
    r"chief wellness",
    r"chief architect",
    r"chief engineer",
    r"\bchief of\b.*\bsecurity\b",
    r"\bchief of staff\b",
)
_FALLBACK_TITLE_PATTERNS = (
    (r"\bmanaging director\b", 180),
    (r"\bfounder\b", 175),
    (r"\bowner\b", 170),
    (r"\bchairman\b", 150),
    (r"\bpresident\b", 145),
    (r"\bpartner\b", 140),
    (r"\bdirector\b", 90),
)
_UAE_LOCATION_PATTERNS = (
    "united arab emirates",
    "abu dhabi",
    "dubai",
    "sharjah",
    "ajman",
    "ras al khaimah",
    "fujairah",
    "umm al quwain",
    "umm-al-quwain",
    "al ain",
)
ENV_WIZA_SELECTOR_ENABLE_LLM = "WIZA_SELECTOR_ENABLE_LLM"


@dataclass(slots=True)
class ContactCandidate:
    """联系人候选。"""

    name: str
    title: str
    score: int


class WizaContactSelector:
    """规则优先，歧义时才走 LLM。"""

    def __init__(self, output_dir: Path) -> None:
        settings = build_email_settings(output_dir)
        self._llm: EmailUrlLlmClient | None = None
        if settings.llm_api_key and _wiza_selector_llm_enabled():
            self._llm = EmailUrlLlmClient(
                api_key=settings.llm_api_key,
                base_url=settings.llm_base_url,
                model=settings.llm_model,
                reasoning_effort=settings.llm_reasoning_effort,
                api_style=settings.llm_api_style,
                timeout_seconds=settings.llm_timeout_seconds,
            )

    def close(self) -> None:
        if self._llm is not None:
            self._llm.close()

    def pick_representative(self, company_name: str, contacts: list[dict[str, Any]]) -> str:
        candidates = self._build_candidates(company_name, contacts)
        if not candidates:
            return ""
        if len(candidates) == 1 and candidates[0].score >= 140:
            return candidates[0].name
        if len(candidates) >= 2 and candidates[0].score >= 300 and candidates[0].score - candidates[1].score >= 15:
            return candidates[0].name
        if len(candidates) >= 2 and candidates[0].score - candidates[1].score >= 40:
            return candidates[0].name
        if len(candidates) >= 2 and candidates[0].score >= 300:
            return self._pick_with_llm(company_name, candidates[:8])
        selected = self._pick_with_llm(company_name, candidates[:8])
        if selected:
            return selected
        return candidates[0].name if candidates[0].score >= 180 else ""

    def _build_candidates(self, company_name: str, contacts: list[dict[str, Any]]) -> list[ContactCandidate]:
        results: list[ContactCandidate] = []
        seen: set[str] = set()
        for contact in contacts:
            name = normalize_person_name(str(contact.get("full_name") or ""), company_name)
            if not name:
                continue
            title = str(contact.get("job_title") or "").strip()
            score = _score_title(title, contact)
            if score <= 0 or name in seen:
                continue
            seen.add(name)
            results.append(ContactCandidate(name=name, title=title, score=score))
        results.sort(key=lambda item: (-item.score, len(item.title), item.name.lower()))
        return results

    def _pick_with_llm(self, company_name: str, candidates: list[ContactCandidate]) -> str:
        if self._llm is None or len(candidates) < 2:
            return ""
        prompt = (
            "你是企业高管判定器。\n"
            "任务：只从候选名单里选出最像公司一把手的人。\n"
            "优先级：CEO / Chief Executive > Chairman&CEO > Managing Director > Owner / Founder > Chairman / President / Partner。\n"
            "明确排除：CFO / COO / CTO / CIO / CMO / CRO / CAO / CISO / CHRO 这类非一把手。\n"
            "只能返回候选名单中的原样 full_name；如果没有把握，就返回空字符串。\n"
            '返回 JSON：{"representative":"","reason":""}\n'
            f"公司：{company_name}\n"
            f"候选：{json.dumps([_candidate_payload(candidate) for candidate in candidates], ensure_ascii=False)}"
        )
        try:
            data = self._llm._call_json(prompt)  # noqa: SLF001
        except Exception:
            self._llm.close()
            self._llm = None
            return ""
        selected = normalize_person_name(str(data.get("representative") or ""), company_name)
        allowed = {candidate.name for candidate in candidates}
        return selected if selected in allowed else ""


def _score_title(title: str, contact: dict[str, Any]) -> int:
    text = str(title or "").strip().lower()
    if not text:
        return 0
    if _matches_any(text, _BLOCK_TITLE_PATTERNS):
        return 0
    direct_score = _score_direct_ceo_title(text, contact)
    if direct_score > 0:
        return direct_score
    levels = {str(item or "").strip().lower() for item in list(contact.get("job_title_levels") or [])}
    best = 0
    for pattern, score in _FALLBACK_TITLE_PATTERNS:
        if re.search(pattern, text, flags=re.I):
            best = max(best, score)
    if "cxo" in levels:
        best += 30
    return best


def _matches_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, flags=re.I) for pattern in patterns)


def _score_direct_ceo_title(text: str, contact: dict[str, Any]) -> int:
    if not _matches_any(text, _DIRECT_CEO_PATTERNS):
        return 0
    score = 320
    if text == "chief executive officer":
        score += 5
    if "chairman and chief executive" in text:
        score += 28
    if "managing director and group chief executive officer" in text:
        score += 24
    if "managing director and chief executive officer" in text:
        score += 20
    if "president and chief executive officer" in text:
        score += 16
    if "co chief executive officer" in text or "co-chief executive officer" in text:
        score -= 8
    if "chairman" in text:
        score += 20
    if "managing director" in text:
        score += 10
    if "acting" in text or "country" in text or "regional" in text:
        score -= 25
    if _matches_any(text, ("\\busa\\b", "\\bjordan\\b", "\\bindonesia\\b", "\\bmalaysia\\b", "\\baustralia\\b", "\\beurope\\b", "\\bapac\\b", "\\begypt\\b", "\\bcanada\\b", "kazakhstan", "travel group", "airport operations")):
        score -= 35
    if "group" in text:
        score += 10
    if "|" in text or " of " in text or "," in text:
        score -= 20
    if text.startswith("ceo of "):
        score -= 16
    if " at " in text:
        score -= 35
    if " travel " in text or " asset management " in text or " office " in text or " in house " in text:
        score -= 45
    score += _score_contact_location(contact)
    return score


def _score_contact_location(contact: dict[str, Any]) -> int:
    text = _build_location_text(contact)
    if not text:
        return 0
    if any(keyword in text for keyword in _UAE_LOCATION_PATTERNS):
        return 16
    return -16


def _build_location_text(contact: dict[str, Any]) -> str:
    parts = [
        contact.get("location_name"),
        contact.get("location"),
        contact.get("city"),
        contact.get("state"),
        contact.get("country"),
    ]
    return " | ".join(str(part or "").strip().lower() for part in parts if str(part or "").strip())


def _candidate_payload(candidate: ContactCandidate) -> dict[str, str | int]:
    return {
        "name": candidate.name,
        "title": candidate.title,
        "score": candidate.score,
    }


def _wiza_selector_llm_enabled() -> bool:
    value = str(os.getenv(ENV_WIZA_SELECTOR_ENABLE_LLM, "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}
