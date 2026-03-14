"""site 阶段公司名抽取 LLM。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


THAI_PATTERN = re.compile(r"[\u0E00-\u0E7F]")
_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def contains_thai_text(value: str) -> bool:
    return bool(THAI_PATTERN.search(str(value or "")))


def resolve_company_name(company_name_en: str, company_name_th: str) -> str:
    thai = str(company_name_th or "").strip()
    if contains_thai_text(thai):
        return thai
    return str(company_name_en or "").strip()


def _parse_json_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass
    matched = _JSON_BLOCK_RE.search(text)
    if matched is None:
        return {}
    try:
        parsed = json.loads(matched.group(0))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


@dataclass(slots=True)
class SiteNameExtractResult:
    company_name_th: str
    evidence_url: str
    evidence_quote: str
    confidence: float


class SiteNameLlmClient:
    """官网首页泰文公司名抽取。"""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        reasoning_effort: str,
        timeout_seconds: float,
    ) -> None:
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("缺少 openai 依赖，请先安装 openai 包。") from exc
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url or None,
            timeout=max(timeout_seconds, 20.0),
        )
        self._model = model
        self._reasoning_effort = reasoning_effort

    def extract_company_name(
        self,
        *,
        company_name_en: str,
        website: str,
        markdown: str,
        raw_html: str,
    ) -> SiteNameExtractResult:
        prompt = (
            "你是企业官网首页信息抽取器。目标是只提取官网首页上明确出现的泰文公司名称。\n"
            "硬性要求：\n"
            "1. 只返回泰文公司名。\n"
            "2. 如果页面没有明确泰文公司名，company_name_th 返回空字符串。\n"
            "3. 不要把英文名、品牌口号、栏目标题当成公司名。\n"
            "4. 不要猜测。\n\n"
            f"D&B 英文公司名: {company_name_en}\n"
            f"官网: {website}\n"
            f"Markdown: {markdown[:12000]}\n\n"
            f"HTML: {raw_html[:12000]}\n\n"
            "输出 JSON："
            '{"company_name_th":"","evidence_url":"","evidence_quote":"","confidence":0.0}'
        )
        data = self._call_json(prompt)
        company_name_th = str(data.get("company_name_th", "") or "").strip()
        if not contains_thai_text(company_name_th):
            company_name_th = ""
        return SiteNameExtractResult(
            company_name_th=company_name_th,
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
            evidence_quote=str(data.get("evidence_quote", "") or "").strip(),
            confidence=max(0.0, min(_as_float(data.get("confidence")), 1.0)),
        )

    def _call_json(self, prompt: str) -> dict[str, Any]:
        base_kwargs: dict[str, Any] = {"model": self._model, "input": prompt}
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
                response = self._client.responses.create(**kwargs)
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
            return _parse_json_text(str(getattr(response, "output_text", "") or ""))
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
