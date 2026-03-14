"""韩国 DNB 韩文名补全与决策。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from korea_crawler.email_agent.config import EmailAgentConfig
from korea_crawler.email_agent.firecrawl_client import FirecrawlClient
from korea_crawler.email_agent.firecrawl_client import FirecrawlClientConfig
from korea_crawler.email_agent.key_pool import FirecrawlKeyPool
from korea_crawler.email_agent.key_pool import KeyPoolConfig


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
_KOREAN_PATTERN = re.compile(r"[가-힣]")
_BLOCKED_KOREAN_NAME_PHRASES = (
    "휴업/폐업",
    "존재하지 않음 또는 중복으로 표시",
    "법적 문제 신고",
    "현재 게시가 사용 중지됨",
    "현재 이 유형의 장소에 대한 게시가 사용 중지됨",
    "비즈니스에 대한 소유권 주장",
    "이름, 위치, 영업시간 등 수정",
    "이름 또는 기타 세부정보 변경",
    "다른 사용자에게 도움이 될 후기를 공유해 주세요",
)


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


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def has_korean_company_name(value: str) -> bool:
    cleaned = _normalize_name(value)
    if not cleaned or not _KOREAN_PATTERN.search(cleaned):
        return False
    if any(phrase in cleaned for phrase in _BLOCKED_KOREAN_NAME_PHRASES):
        return False
    return True


def resolve_company_name(
    *,
    company_name_en_dnb: str,
    company_name_local_gmap: str,
    company_name_local_site: str,
) -> str:
    for value in (company_name_local_gmap, company_name_local_site):
        cleaned = _normalize_name(value)
        if has_korean_company_name(cleaned):
            return cleaned
    return _normalize_name(company_name_en_dnb)


@dataclass(slots=True)
class SiteNameExtractResult:
    company_name_local: str
    evidence_url: str
    evidence_quote: str
    confidence: float


class SiteNameLlmClient:
    """官网韩文公司名抽取。"""

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
        company_name_en_dnb: str,
        website: str,
        markdown: str,
    ) -> SiteNameExtractResult:
        prompt = (
            "你是企业官网首页信息抽取器。目标是只提取官网上明确出现的官方韩文公司名。\n"
            "硬性要求：\n"
            "1. 只返回韩文公司名。\n"
            "2. 如果页面没有明确官方韩文公司名，company_name_local 返回空字符串。\n"
            "3. 不要把品牌口号、栏目标题、产品名当成公司名。\n"
            "4. 优先返回页面顶部、页脚、copyright、about 文案中出现的正式韩文主体名。\n"
            "5. 不要猜测。\n\n"
            f"DNB 英文公司名: {company_name_en_dnb}\n"
            f"官网: {website}\n"
            f"Markdown: {markdown[:12000]}\n\n"
            "输出 JSON："
            '{"company_name_local":"","evidence_url":"","evidence_quote":"","confidence":0.0}'
        )
        data = self._call_json(prompt)
        company_name_local = _normalize_name(str(data.get("company_name_local", "") or ""))
        if not has_korean_company_name(company_name_local):
            company_name_local = ""
        confidence = _clamp_float(data.get("confidence"))
        return SiteNameExtractResult(
            company_name_local=company_name_local,
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
            evidence_quote=str(data.get("evidence_quote", "") or "").strip(),
            confidence=confidence,
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


def _clamp_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(number, 1.0))


@dataclass(slots=True)
class SiteNameService:
    firecrawl: FirecrawlClient
    llm: SiteNameLlmClient

    @classmethod
    def from_env(cls, project_root: Path) -> "SiteNameService":
        config = EmailAgentConfig.from_env(project_root)
        keys = FirecrawlKeyPool.load_keys(config.firecrawl_keys_file)
        key_pool = FirecrawlKeyPool(
            keys=keys,
            key_file=config.firecrawl_keys_file,
            db_path=config.firecrawl_pool_db,
            config=KeyPoolConfig(
                per_key_limit=config.firecrawl_key_per_limit,
                wait_seconds=config.firecrawl_key_wait_seconds,
                cooldown_seconds=config.firecrawl_key_cooldown_seconds,
                failure_threshold=config.firecrawl_key_failure_threshold,
            ),
        )
        firecrawl = FirecrawlClient(
            key_pool=key_pool,
            config=FirecrawlClientConfig(
                base_url=config.firecrawl_base_url,
                timeout_seconds=config.firecrawl_timeout_seconds,
                max_retries=config.firecrawl_max_retries,
                only_main_content=True,
                map_limit=config.map_limit,
            ),
        )
        llm = SiteNameLlmClient(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
            model=config.llm_model,
            reasoning_effort=config.llm_reasoning_effort,
            timeout_seconds=config.llm_timeout_seconds,
        )
        return cls(firecrawl=firecrawl, llm=llm)

    def extract_homepage_name(
        self,
        *,
        company_name_en_dnb: str,
        website: str,
    ) -> SiteNameExtractResult:
        page = self.firecrawl.scrape_page(website)
        markdown = str(page.get("markdown", "") or "").strip()
        if not markdown:
            return SiteNameExtractResult(
                company_name_local="",
                evidence_url=website,
                evidence_quote="",
                confidence=0.0,
            )
        result = self.llm.extract_company_name(
            company_name_en_dnb=company_name_en_dnb,
            website=website,
            markdown=markdown,
        )
        if not result.evidence_url:
            result.evidence_url = str(page.get("url", website) or website)
        return result
