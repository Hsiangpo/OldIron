"""管理人抽取 LLM 客户端。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from openai import OpenAI


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


@dataclass(slots=True)
class ManagerExtractResult:
    manager_name: str
    manager_role: str
    evidence_url: str
    evidence_quote: str
    confidence: float


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


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


class ManagerLlmClient:
    """封装链接选择与管理人抽取。"""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        reasoning_effort: str,
        timeout_seconds: float,
    ) -> None:
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url or None,
            timeout=max(timeout_seconds, 20.0),
        )
        self._model = model
        self._reasoning_effort = reasoning_effort

    def pick_urls(
        self,
        *,
        company_name: str,
        domain: str,
        round_index: int,
        pick_count: int,
        candidate_urls: list[str],
        tried_urls: list[str],
    ) -> list[str]:
        prompt = (
            "你是企业官网导航选择器。目标是找公司管理层姓名。\n"
            "可接受角色：Manager、Managing Director、Director、CEO、Founder、Owner、President、Partner。\n"
            "优先考虑后缀模式：/about, /about-us, /aboutus, /company, /corporate, /team, /leadership, "
            "/management, /directors, /board, /who-we-are, /contact。\n"
            "只允许从候选URL中选择，返回 JSON。\n\n"
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"轮次: {round_index}\n"
            f"目标数量: {pick_count}\n"
            f"已尝试URL(JSON): {json.dumps(tried_urls, ensure_ascii=False)}\n"
            f"候选URL(JSON): {json.dumps(candidate_urls, ensure_ascii=False)}\n\n"
            "输出格式：{\"selected_urls\": [\"...\"]}"
        )
        data = self._call_json(prompt)
        picked = data.get("selected_urls")
        if not isinstance(picked, list):
            return []
        allowed = set(candidate_urls)
        results: list[str] = []
        for item in picked:
            if not isinstance(item, str):
                continue
            value = item.strip()
            if not value or value not in allowed:
                continue
            results.append(value)
        return list(dict.fromkeys(results))[: max(pick_count, 1)]

    def extract_manager(
        self,
        *,
        company_name: str,
        domain: str,
        pages: list[dict[str, str]],
    ) -> ManagerExtractResult:
        prompt = (
            "你是企业信息抽取器。仅当证据明确显示属于公司管理层时才返回姓名。\n"
            "可接受角色：Manager、Managing Director、Director、CEO、Founder、Owner、President、Partner。\n"
            "若没有明确证据，manager_name 返回空字符串。\n"
            "不要猜测。\n\n"
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"页面内容(JSON): {json.dumps(pages, ensure_ascii=False)}\n\n"
            "输出 JSON："
            "{\"manager_name\":\"\",\"manager_role\":\"\",\"evidence_url\":\"\","
            "\"evidence_quote\":\"\",\"confidence\":0.0}"
        )
        data = self._call_json(prompt)
        return ManagerExtractResult(
            manager_name=str(data.get("manager_name", "") or "").strip(),
            manager_role=str(data.get("manager_role", "") or "").strip(),
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
            evidence_quote=str(data.get("evidence_quote", "") or "").strip(),
            confidence=max(0.0, min(_as_float(data.get("confidence")), 1.0)),
        )

    def _call_json(self, prompt: str) -> dict[str, object]:
        base_kwargs: dict[str, object] = {
            "model": self._model,
            "input": prompt,
        }
        if self._reasoning_effort:
            base_kwargs["reasoning"] = {"effort": self._reasoning_effort}
        plans = (
            (False, True),
            (False, False),
            (True, True),
            (True, False),
        )
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
                # 中文注释：兼容不支持 response_format 的 OpenAI 兼容网关。
                if use_response_format and "response_format" in str(exc):
                    last_exc = exc
                    continue
                raise
            except Exception as exc:  # noqa: BLE001
                # 中文注释：兼容要求 input 必须是数组的网关。
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
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt,
                    }
                ],
            }
        ]
