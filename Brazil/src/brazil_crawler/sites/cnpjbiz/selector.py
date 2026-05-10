"""CNPJ Biz 多候选代表人选择。"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

from oldiron_core.fc_email.llm_client import EmailUrlLlmClient

from .config import CnpjBizConfig


LOGGER = logging.getLogger(__name__)
_ROLE_PATTERNS: tuple[tuple[str, int], ...] = (
    (r"\bsócio[-\s]*administrador\b", 100),
    (r"\badministrador\b", 92),
    (r"\bpresidente\b", 88),
    (r"\bdiretor\b", 84),
    (r"\btitular\b", 80),
    (r"\bsócio\b", 72),
    (r"\bproprietário\b", 70),
    (r"\bproprietario\b", 70),
    (r"\bgerente\b", 40),
)


@dataclass(slots=True)
class RepresentativeCandidate:
    name: str
    role: str


class CnpjBizRepresentativeSelector:
    """优先用 LLM，失败时回退规则。"""

    def __init__(
        self,
        config: CnpjBizConfig,
        *,
        llm_client: EmailUrlLlmClient | None = None,
    ) -> None:
        self._config = config
        self._llm = llm_client
        self._owns_llm = False
        self._llm_disabled = False

    def close(self) -> None:
        if self._owns_llm and self._llm is not None:
            self._llm.close()

    def choose(self, *, company_name: str, cnpj: str, candidates: list[RepresentativeCandidate]) -> str:
        normalized = _normalize_candidates(candidates)
        if not normalized:
            return ""
        if len(normalized) == 1:
            return normalized[0].name
        picked = self._choose_via_llm(company_name=company_name, cnpj=cnpj, candidates=normalized)
        if picked:
            return picked
        return _fallback_candidate(normalized).name

    def _choose_via_llm(
        self,
        *,
        company_name: str,
        cnpj: str,
        candidates: list[RepresentativeCandidate],
    ) -> str:
        llm = self._ensure_llm()
        if llm is None:
            return ""
        prompt = _build_prompt(company_name=company_name, cnpj=cnpj, candidates=candidates)
        try:
            payload = llm._call_json(prompt)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz 代表人 LLM 选择失败：company=%s error=%s", company_name, exc)
            if _is_auth_error(exc):
                self._llm_disabled = True
            return ""
        name = str(payload.get("representative", "") or "").strip()
        role = str(payload.get("role", "") or "").strip()
        if not name:
            return ""
        for candidate in candidates:
            if candidate.name.lower() != name.lower():
                continue
            if role and candidate.role.lower() != role.lower():
                continue
            return candidate.name
        return ""

    def _ensure_llm(self) -> EmailUrlLlmClient | None:
        if self._llm_disabled:
            return None
        if self._llm is not None:
            return self._llm
        if not self._config.llm_api_key or not self._config.llm_model:
            return None
        self._llm = EmailUrlLlmClient(
            api_key=self._config.llm_api_key,
            base_url=self._config.llm_base_url,
            model=self._config.llm_model,
            reasoning_effort=self._config.llm_reasoning_effort,
            api_style=self._config.llm_api_style,
            timeout_seconds=self._config.llm_timeout_seconds,
        )
        self._owns_llm = True
        return self._llm


def _normalize_candidates(candidates: list[RepresentativeCandidate]) -> list[RepresentativeCandidate]:
    seen: set[tuple[str, str]] = set()
    normalized: list[RepresentativeCandidate] = []
    for candidate in candidates:
        name = str(candidate.name or "").strip()
        role = str(candidate.role or "").strip()
        if not name or not role:
            continue
        key = (name.lower(), role.lower())
        if key in seen:
            continue
        seen.add(key)
        normalized.append(RepresentativeCandidate(name=name, role=role))
    return normalized


def _fallback_candidate(candidates: list[RepresentativeCandidate]) -> RepresentativeCandidate:
    return sorted(
        candidates,
        key=lambda item: (-_score_role(item.role), item.name.lower()),
    )[0]


def _score_role(role: str) -> int:
    lowered = str(role or "").strip().lower()
    best = 0
    for pattern, score in _ROLE_PATTERNS:
        if re.search(pattern, lowered, flags=re.I):
            best = max(best, score)
    return best


def _build_prompt(*, company_name: str, cnpj: str, candidates: list[RepresentativeCandidate]) -> str:
    payload = [{"name": item.name, "role": item.role} for item in candidates]
    return (
        "你是巴西企业代表人选择器。\n"
        "任务：只从候选名单里选出一个最像公司最高负责人的自然人。\n"
        "优先级通常是：Sócio-Administrador > Administrador > Presidente > Diretor > Titular > Sócio。\n"
        "不能编造新名字，只能从候选里选。\n"
        "返回 JSON：{\"representative\":\"\",\"role\":\"\"}\n"
        f"公司：{company_name}\n"
        f"CNPJ：{cnpj}\n"
        f"候选：{json.dumps(payload, ensure_ascii=False)}"
    )


def _is_auth_error(exc: Exception) -> bool:
    lowered = str(exc or "").strip().lower()
    if not lowered:
        return False
    return "401" in lowered or "无效的令牌" in lowered or "invalid" in lowered
