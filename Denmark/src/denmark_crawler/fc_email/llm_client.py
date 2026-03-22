"""官网爬虫邮箱补充 LLM 客户端。"""

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
        recommended_urls: list[str] | None = None,
    ) -> list[str]:
        # 构建带推荐标注的 URL 列表
        rec_set = set(recommended_urls or [])
        annotated = []
        for url in candidate_urls:
            annotated.append(f"★ {url}" if url in rec_set else url)
        prompt = (
            "你是企业官网信息页选择器。\n"
            "任务：从候选 URL 中，按最可能出现公开邮箱或代表人的概率排序，并返回前 N 个。\n"
            "优先页面：contact, about, team, leadership, management, board, imprint, legal, privacy, careers, press。\n"
            "首页也可以入选。\n"
            "标记 ★ 的 URL 是规则引擎推荐的，仅供参考。你可以自由选择任意 URL，不受 ★ 标记限制。\n"
            "不允许编造 URL，只能从给定列表里选（返回时去掉 ★ 前缀）。\n"
            '返回 JSON：{"selected_urls": ["..."]}\n\n'
            f"公司名: {company_name}\n"
            f"域名: {domain}\n"
            f"首页: {homepage}\n"
            f"最多返回: {max(int(target_count), 1)}\n"
            f"候选 URL(JSON): {json.dumps(annotated, ensure_ascii=False)}"
        )
        data = self._call_json(prompt)
        selected = data.get("selected_urls")
        if not isinstance(selected, list):
            return []
        allowed = set(candidate_urls)
        picked: list[str] = []
        for item in selected:
            # 兼容 LLM 可能带着 ★ 前缀返回
            value = str(item or "").strip().lstrip("★").strip()
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
            "目标：从给定网页 HTML 中抽取公司名、公司最高负责人（Director 级别以上）、所有公开邮箱。\n\n"
            "=== 代表人规则（极其严格）===\n"
            "1. 只接受以下级别的人作为代表人（从高到低）：\n"
            "   CEO / Managing Director / Director / Chairman / "
            "Founder / Owner / Partner（律所/会计所的合伙人）/ "
            "President / Vice President / Chief Officer。\n"
            "2. 【不接受】以下级别的人：Manager / Coordinator / "
            "Consultant / Advisor / Employee / Assistant / Secretary / "
            "Accountant / Receptionist / Clerk / Officer（无 Chief 前缀的）。这些职位太低，不是公司代表人。\n"
            "3. 【严禁推断】代表人姓名必须在网页 HTML 正文中原文出现过。\n"
            "   绝对禁止从输入的公司名中拆分或猜测人名。\n"
            "   例：公司名叫 'Smith & Johnson Limited'，\n"
            "   你不能直接返回 'Smith' 或 'Johnson'，除非 HTML 正文里也写了这个名字和对应的 Director 级别职位。\n"
            "4. 代表人必须是真实人名（名+姓），不能是职位名、公司名或占位符。\n"
            "5. evidence_quote 必须包含代表人姓名的原文片段（从 HTML 中复制）。\n"
            "   如果你无法提供包含该人名的 evidence_quote，说明你没有在 HTML 中找到，必须留空 representative。\n"
            "6. 如果页面上有多个人但无法确定谁是最高负责人，宁可留空也不要猜。\n\n"
            "=== 邮箱规则 ===\n"
            "7. 优先保留个人邮箱（如 firstname@domain），"
            "其次保留通用邮箱（如 info@, enquiries@, hello@, mail@）。\n"
            "8. 排除无效邮箱：noreply@, no-reply@, example@, test@, "
            "以及社媒账号、图片中的文字邮箱。\n\n"
            "=== 其他 ===\n"
            "9. company_name：如果网页明确显示公司法定名称就用官网的，否则用输入公司名。\n"
            "10. 找不到代表人就 representative 留空字符串，找不到邮箱就 emails 留空列表。绝对不要编造。\n\n"
            '返回 JSON：{"company_name":"","representative":"","emails":[],"evidence_url":"","evidence_quote":""}\n\n'
            f"输入公司名: {company_name}\n"
            f"首页: {homepage}\n"
            f"页面(JSON): {json.dumps(pages, ensure_ascii=False)}"
        )
        data = self._call_json(prompt)
        emails = self._normalize_emails(data.get("emails"))
        representative = str(data.get("representative", "") or "").strip()
        evidence_quote = str(data.get("evidence_quote", "") or "").strip()
        evidence_url = str(data.get("evidence_url", "") or "").strip()
        # 校验：代表人姓名必须出现在 evidence_quote 中，否则判定为编造
        if representative and evidence_quote:
            # 把代表人名字拆分成各个单词，至少有一半以上出现在 evidence_quote 中
            name_parts = representative.split()
            if len(name_parts) >= 2:
                matches = sum(1 for part in name_parts if part.lower() in evidence_quote.lower())
                if matches < len(name_parts) * 0.5:
                    LOGGER.warning(
                        "LLM 代表人被丢弃（evidence_quote 不匹配）：rep=%s quote=%s",
                        representative, evidence_quote[:80],
                    )
                    representative = ""
                    evidence_quote = ""
        elif representative and not evidence_quote:
            # 没有 evidence_quote，说明 LLM 找不到证据，丢弃代表人
            LOGGER.warning("LLM 代表人被丢弃（无 evidence_quote）：rep=%s", representative)
            representative = ""
        return HtmlContactExtraction(
            company_name=str(data.get("company_name", "") or company_name).strip(),
            representative=representative,
            emails=emails,
            evidence_url=evidence_url,
            evidence_quote=evidence_quote,
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
                output_text = self._call_api_with_retry(kwargs)
                return _parse_json_text(output_text)
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
        if last_exc is not None:
            raise last_exc
        return {}

    def _call_api_with_retry(self, kwargs: dict[str, Any], max_retries: int = 5) -> str:
        """带指数退避的 API 调用，处理网络错误、超时、429 限流和 5xx 错误。"""
        import time as _time
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                resp = self._client.responses.create(**kwargs)
                return str(getattr(resp, "output_text", "") or "")
            except TypeError:
                # 参数格式错误直接抛出，不重试
                raise
            except Exception as exc:  # noqa: BLE001
                err_str = str(exc)
                # 判断是否值得重试：网络错误、超时、429、5xx
                retryable = any(kw in err_str for kw in (
                    "Connection", "Timeout", "timeout", "429", "500", "502", "503",
                    "rate_limit", "Rate limit", "overloaded", "capacity",
                ))
                if not retryable:
                    raise
                last_exc = exc
                wait = min(2 ** (attempt + 1), 32)  # 2s, 4s, 8s, 16s, 32s
                LOGGER.warning(
                    "LLM API 重试 %d/%d，等待 %ds，错误: %s",
                    attempt + 1, max_retries, wait, exc,
                )
                _time.sleep(wait)
        raise last_exc

    def _build_list_input(self, prompt: str) -> list[dict[str, object]]:
        return [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }
        ]
