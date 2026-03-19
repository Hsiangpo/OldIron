"""Proff Firecrawl 外部 LLM 客户端。"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
LOGGER = logging.getLogger(__name__)


def _parse_json_text(raw: str) -> dict[str, object]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass
    match = _JSON_BLOCK_RE.search(text)
    if match is None:
        return {}
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


@dataclass(slots=True)
class HtmlContactExtraction:
    company_name: str
    representative: str
    emails: list[str]
    evidence_url: str
    evidence_quote: str


class EmailUrlLlmClient:
    """负责选链和 HTML 抽取。"""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        reasoning_effort: str,
        timeout_seconds: float,
        fallback_model: str = "gpt-5.1-codex-mini",
    ) -> None:
        from openai import OpenAI

        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url or None,
            timeout=max(timeout_seconds, 20.0),
        )
        self._model = model
        self._fallback_model = fallback_model
        self._reasoning_effort = reasoning_effort

    def pick_candidate_urls(
        self,
        *,
        company_name: str,
        domain: str,
        homepage: str,
        candidate_urls: list[str],
        target_count: int,
    ) -> list[str]:
        prompt = (
            "你是企业官网信息页选择器。\n"
            "任务：从候选 URL 中，按最可能出现公开邮箱或代表人的概率排序，并返回前 N 个。\n"
            "优先页面：contact, about, team, leadership, management, board, imprint, legal, privacy, careers, press。\n"
            "首页也可以入选。\n"
            "不允许编造 URL，只能从给定列表里选。\n"
            '返回 JSON：{"selected_urls": ["..."]}\n\n'
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"首页: {homepage}\n"
            f"最多返回: {max(int(target_count), 1)}\n"
            f"候选 URL(JSON): {json.dumps(candidate_urls, ensure_ascii=False)}"
        )
        data = self._call_json(prompt)
        selected = data.get("selected_urls")
        if not isinstance(selected, list):
            return []
        allowed = set(candidate_urls)
        picked: list[str] = []
        for item in selected:
            value = str(item or "").strip()
            if value and value in allowed and value not in picked:
                picked.append(value)
        return picked[: max(int(target_count), 1)]

    def extract_contacts_from_html(
        self,
        *,
        company_name: str,
        homepage: str,
        pages: list[dict[str, str]],
    ) -> HtmlContactExtraction:
        prompt = (
            "你是企业官网联系人抽取器。\n"
            "目标：从给定网页 HTML 中抽取公司名、最大的代表人、所有公开邮箱。\n"
            "规则：\n"
            "1. 代表人只保留一个最大的，优先 CEO / Adm. Direktør / Managing Director / Director / Founder / Owner。\n"
            "2. 邮箱只保留真实公开邮箱，不要占位符、不要社媒账号。\n"
            "3. company_name 如果网页明确显示公司名称，就返回官网上的名称，否则返回输入公司名。\n"
            "4. evidence_url 返回最能证明代表人或邮箱的页面。\n"
            "5. evidence_quote 返回最短直接证据。\n"
            '返回 JSON：{"company_name":"","representative":"","emails":[],"evidence_url":"","evidence_quote":""}\n\n'
            f"输入公司名: {company_name}\n"
            f"首页: {homepage}\n"
            f"页面(JSON): {json.dumps(pages, ensure_ascii=False)}"
        )
        data = self._call_json(prompt)
        emails = self._normalize_emails(data.get("emails"))
        return HtmlContactExtraction(
            company_name=str(data.get("company_name", "") or company_name).strip(),
            representative=str(data.get("representative", "") or "").strip(),
            emails=emails,
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
            evidence_quote=str(data.get("evidence_quote", "") or "").strip(),
        )

    def _normalize_emails(self, values: object) -> list[str]:
        if not isinstance(values, list):
            return []
        emails: list[str] = []
        for item in values:
            text = str(item or "").strip().lower()
            if text and "@" in text and text not in emails:
                emails.append(text)
        return emails

    def _call_json(self, prompt: str) -> dict[str, Any]:
        last_exc: Exception | None = None
        for model in self._candidate_models():
            try:
                return self._call_json_with_model(model, prompt)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                LOGGER.warning("LLM 调用失败，模型=%s，错误=%s", model, exc)
                continue
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("LLM 调用失败：无可用模型")

    def _candidate_models(self) -> list[str]:
        values: list[str] = []
        for model in (self._model, self._fallback_model):
            text = str(model or "").strip()
            if text and text not in values:
                values.append(text)
        return values

    def _call_json_with_model(self, model: str, prompt: str) -> dict[str, Any]:
        base_kwargs: dict[str, Any] = {"model": model, "input": prompt}
        if self._reasoning_effort:
            base_kwargs["reasoning"] = {"effort": self._reasoning_effort}
        plans = ((False, True), (False, False), (True, True), (True, False))
        last_exc: Exception | None = None
        for use_list_input, use_response_format in plans:
            kwargs = dict(base_kwargs)
            if use_list_input:
                kwargs["input"] = self._build_list_input(prompt)
            if use_response_format:
                kwargs["response_format"] = {"type": "json_object"}
            try:
                resp = self._client.responses.create(**kwargs)
            except TypeError as exc:
                if use_response_format and "response_format" in str(exc):
                    last_exc = exc
                    continue
                raise
            except Exception as exc:  # noqa: BLE001
                if (not use_list_input) and "Input must be a list" in str(exc):
                    last_exc = exc
                    continue
                raise
            output_text = str(getattr(resp, "output_text", "") or "")
            return _parse_json_text(output_text)
        if last_exc is not None:
            raise last_exc
        return {}

    def _build_list_input(self, prompt: str) -> list[dict[str, object]]:
        return [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }
        ]
