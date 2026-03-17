"""候选 URL 重排 LLM。"""

from __future__ import annotations

import json
import re
from typing import Any


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


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


class EmailUrlLlmClient:
    """候选 URL 重排客户端。"""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        reasoning_effort: str,
        timeout_seconds: float,
    ) -> None:
        from openai import OpenAI

        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url or None,
            timeout=max(timeout_seconds, 20.0),
        )
        self._model = model
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
            "你是企业官网联系页选择器。目标是从候选 URL 中选择最可能包含公开邮箱的页面。\n"
            "优先级：contact, support, about, team, leadership, management, privacy, legal, imprint, careers, press, media, terms, PDF。\n"
            "只允许从给定候选 URL 中选择，不要编造 URL。\n"
            "返回 JSON：{\"selected_urls\": [\"...\"]}\n\n"
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"首页: {homepage}\n"
            f"目标数量: {max(int(target_count), 1)}\n"
            f"候选 URL(JSON): {json.dumps(candidate_urls, ensure_ascii=False)}"
        )
        data = self._call_json(prompt)
        selected = data.get("selected_urls")
        if not isinstance(selected, list):
            return []
        allowed = set(candidate_urls)
        picked: list[str] = []
        for item in selected:
            if not isinstance(item, str):
                continue
            value = item.strip()
            if value and value in allowed and value not in picked:
                picked.append(value)
        return picked[: max(int(target_count), 1)]

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

