"""LLM 邮箱抽取客户端。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from openai import OpenAI

_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


@dataclass(slots=True)
class EmailExtractResult:
    emails: list[str]
    evidence_url: str
    confidence: float


def _parse_json_text(raw: str) -> dict:
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


class EmailLlmClient:
    """封装 LLM 邮箱抽取。"""

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
        candidate_urls: list[str],
        pick_count: int = 5,
    ) -> list[str]:
        """让 LLM 从候选 URL 中选出最可能包含邮箱的页面。"""
        prompt = (
            "你是企业官网导航选择器。目标是找包含联系邮箱的页面。\n"
            "优先考虑后缀模式：/contact, /contact-us, /about, /about-us, /company, /footer, /support。\n"
            "只允许从候选URL中选择，返回 JSON。\n\n"
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"目标数量: {pick_count}\n"
            f"候选URL(JSON): {json.dumps(candidate_urls[:200], ensure_ascii=False)}\n\n"
            '输出格式：{"selected_urls": ["..."]}'
        )
        data = self._call_json(prompt)
        picked = data.get("selected_urls")
        if not isinstance(picked, list):
            return []
        allowed = set(candidate_urls)
        results: list[str] = []
        for item in picked:
            if isinstance(item, str) and item.strip() in allowed:
                results.append(item.strip())
        return list(dict.fromkeys(results))[:max(pick_count, 1)]

    def extract_emails(
        self,
        *,
        company_name: str,
        domain: str,
        pages: list[dict[str, str]],
    ) -> EmailExtractResult:
        """从页面内容中提取邮箱。"""
        # 先用正则预提取，加速 LLM 处理
        regex_emails: set[str] = set()
        for page in pages:
            found = _EMAIL_RE.findall(page.get("markdown", ""))
            regex_emails.update(e.lower() for e in found)

        prompt = (
            "你是企业信息抽取器。从公司官网页面中提取所有联系邮箱。\n"
            "排除以下类型邮箱：noreply@, no-reply@, mailer-daemon@, postmaster@\n"
            "如果页面中没有明确的邮箱，返回空列表。\n\n"
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"正则预提取邮箱: {json.dumps(sorted(regex_emails), ensure_ascii=False)}\n"
            f"页面内容(JSON): {json.dumps(pages, ensure_ascii=False)[:8000]}\n\n"
            '输出 JSON：{"emails": ["email1@...", "email2@..."], "evidence_url": "...", "confidence": 0.9}'
        )
        data = self._call_json(prompt)
        raw_emails = data.get("emails", [])
        if not isinstance(raw_emails, list):
            raw_emails = []

        # 合并 LLM 结果和正则结果
        all_emails: list[str] = []
        seen: set[str] = set()
        for e in raw_emails:
            val = str(e).strip().lower()
            if val and _EMAIL_RE.fullmatch(val) and val not in seen:
                seen.add(val)
                all_emails.append(val)
        for e in sorted(regex_emails):
            if e not in seen:
                seen.add(e)
                all_emails.append(e)

        # 过滤无效邮箱
        filtered = [e for e in all_emails if not any(
            e.startswith(p) for p in ("noreply@", "no-reply@", "mailer-daemon@", "postmaster@")
        )]

        confidence = data.get("confidence", 0.0)
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.0

        return EmailExtractResult(
            emails=filtered,
            evidence_url=str(data.get("evidence_url", "") or ""),
            confidence=max(0.0, min(confidence, 1.0)),
        )

    def _call_json(self, prompt: str) -> dict:
        base_kwargs: dict = {
            "model": self._model,
            "input": prompt,
        }
        if self._reasoning_effort:
            base_kwargs["reasoning"] = {"effort": self._reasoning_effort}

        plans = [
            (False, True),
            (False, False),
            (True, True),
            (True, False),
        ]
        last_exc: Exception | None = None
        for use_list_input, use_response_format in plans:
            kwargs = dict(base_kwargs)
            if use_list_input:
                kwargs["input"] = [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}]
            if use_response_format:
                kwargs["response_format"] = {"type": "json_object"}
            try:
                resp = self._client.responses.create(**kwargs)
            except TypeError as exc:
                if use_response_format and "response_format" in str(exc):
                    last_exc = exc
                    continue
                raise
            except Exception as exc:
                if (not use_list_input) and "Input must be a list" in str(exc):
                    last_exc = exc
                    continue
                raise
            output_text = str(getattr(resp, "output_text", "") or "")
            return _parse_json_text(output_text)
        if last_exc is not None:
            raise last_exc
        return {}
