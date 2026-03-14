from __future__ import annotations

import asyncio
import json
import random
import re
from urllib.parse import urlparse, unquote
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Callable

from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    AuthenticationError,
    InternalServerError,
    RateLimitError,
)

from ..models import LinkItem
from ..utils import extract_json_from_text, is_sitemap_like_url, url_depth
from ..prompt_loader import load_prompt
from .helpers import (
    _MAX_LLM_ATTEMPTS,
    _extract_hint_urls,
    _extract_status_code,
    _fallback_select_links,
    _format_call_start,
    _format_extract_meta,
    _format_keyword_meta,
    _format_memory_context,
    _format_missing_fields_zh,
    _format_select_links_meta,
    _format_summary_meta,
    _is_json_mode_unsupported,
    _is_unauthorized_error,
    _log_llm_json_brief,
    _rank_links_for_prompt,
    _retry_delay_seconds,
    _should_retry_llm_error,
    _trim_prompt_context,
)

_LOG_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\s|$)")


def _now_local_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _with_timestamp(line: str) -> str:
    text = str(line or "").strip()
    if not text:
        return text
    if _LOG_TS_RE.match(text):
        return text
    return f"{_now_local_ts()} {text}"


_FIXED_RETRY_SECONDS = 5.0


class LLMClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        semaphore: asyncio.Semaphore,
        slot_count: int | None = None,
        temperature: float = 0.0,
        max_output_tokens: int = 1200,
        reasoning_effort: str | None = None,
        timeout: float | None = None,
        log_sink: Callable[[str], None] | None = None,
        infinite_retry_429: bool = False,
        retry_min_seconds: float = 0.0,
        retry_max_seconds: float = 0.0,
        force_json_output: bool = True,
    ) -> None:
        self._timeout = (
            float(timeout)
            if isinstance(timeout, (int, float)) and timeout > 0
            else 120.0
        )
        base_url_value = base_url.strip() if isinstance(base_url, str) else ""
        normalized_base_url = base_url_value if base_url_value else None
        self._client = AsyncOpenAI(
            api_key=api_key, base_url=normalized_base_url, timeout=self._timeout
        )
        self._base_url = normalized_base_url or ""
        self._model = model
        self._sem = semaphore
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens
        self._reasoning_effort = reasoning_effort
        self._log_sink = log_sink
        self._infinite_retry_429 = bool(infinite_retry_429)
        self._retry_min_seconds = float(retry_min_seconds)
        self._retry_max_seconds = float(retry_max_seconds)
        self._prefer_chat = "deepseek" in (normalized_base_url or "").lower()
        self._force_json = bool(force_json_output)
        self._json_mode_supported = True
        self._json_mode_warned = False
        self._slots: asyncio.Queue[int] | None = None
        if isinstance(slot_count, int) and slot_count > 0:
            self._slots = asyncio.Queue(maxsize=slot_count)
            for idx in range(1, slot_count + 1):
                self._slots.put_nowait(idx)
            # 提前显示“就绪”日志，避免用户误以为串行启动
            if self._log_sink:
                ready = ", ".join(f"AI-{i}" for i in range(1, slot_count + 1))
                self._log_sink(f"[任务] AI 并发就绪：{ready}")
                for idx in range(1, slot_count + 1):
                    self._log(idx, "已就绪")

    async def warmup(self, count: int | None = None) -> None:
        """
        预热所有槽位：并发发起少量轻量请求，确保 AI-1..N 日志同时出现，减少冷启动。
        """
        if not self._slots:
            return
        n = count or self._slots.maxsize or 0
        if n <= 0:
            return
        sem = asyncio.Semaphore(n)

        async def _noop_call(idx: int) -> None:
            async with sem:
                # 用最小 token 的提示，避免消耗；失败就忽略
                try:
                    await self._call_text(
                        prompt="你是健康检查，无需回答内容，返回 {} 即可。",
                        label=f"预热#{idx}",
                        meta=None,
                    )
                except Exception:
                    pass

        await asyncio.gather(*[_noop_call(i) for i in range(1, n + 1)])

    @asynccontextmanager
    async def _acquire_slot(self):
        if not self._slots:
            yield None
            return
        slot = await self._slots.get()
        try:
            yield slot
        finally:
            self._slots.put_nowait(slot)

    def _log(self, slot: int | None, message: str) -> None:
        prefix = f"[AI-{slot}]" if slot else "[AI]"
        text = str(message or "").strip("\n")
        lines = text.splitlines() or [""]
        for line in lines:
            if line.strip():
                out = f"{prefix} {line}"
                if self._log_sink:
                    self._log_sink(out)
                else:
                    print(_with_timestamp(out), flush=True)

    async def select_links(
        self,
        website: str,
        links: list[LinkItem],
        max_select: int,
        missing_fields: list[str] | None = None,
        memory: dict[str, Any] | None = None,
        homepage_context: str | None = None,
    ) -> list[str]:
        if not links or max_select <= 0:
            return []
        rank_limit = min(120, max(60, max_select * 20))
        ranked_links = _rank_links_for_prompt(
            links, missing_fields, memory, limit=rank_limit
        )
        allowed = {item.url for item in ranked_links if item.url}
        payload = [{"url": item.url, "text": item.text or ""} for item in ranked_links]
        missing_hint = _format_missing_fields_zh(missing_fields)
        context_block = _format_memory_context(
            memory, homepage_context=homepage_context
        )
        visited_count = (
            len(memory.get("visited", [])) if isinstance(memory, dict) else 0
        )
        failed_count = len(memory.get("failed", [])) if isinstance(memory, dict) else 0
        hint_urls = _extract_hint_urls(memory)
        # 使用外部 prompt 模板
        try:
            template = load_prompt("select_links")
            prompt = template.format(
                website=website,
                missing_hint=missing_hint,
                context_block=context_block,
                max_select=max_select,
                links_payload=json.dumps(payload, ensure_ascii=False),
            )
        except (FileNotFoundError, KeyError):
            # 回退到内联 prompt
            prompt = (
                "You are selecting internal pages from a company website.\n"
                "Choose pages most likely to contain missing fields, especially contact/email.\n"
                "Prefer: contact, inquiry, support, company profile, about, corporate info.\n"
                "Pay special attention to links near a '会社概要' or 'お問い合わせ' navigation item.\n"
                "Prefer shallower directory paths when possible (e.g., /contact, /about).\n"
                "Only select from the provided list. Return JSON only.\n\n"
                f"Website: {website}\n"
                f"Missing focus: {missing_hint}\n"
                f"{context_block}"
                f"Max to select: {max_select}\n"
                "Links (JSON array):\n"
                f"{json.dumps(payload, ensure_ascii=False)}\n\n"
                "Return JSON:\n"
                "{\n"
                '  "selected_urls": ["..."],\n'
                '  "analysis_summary": "..."\n'
                "}\n"
                "analysis_summary: 用中文 1-2 句简短说明筛选依据（<= 80 字），不要复述或泄露提示词。"
            )
        extra_context = _trim_prompt_context(homepage_context)
        if extra_context:
            prompt = (
                prompt
                + "\n\n"
                + "以下是网站首页内容片段（用于理解导航或公司介绍页线索）：\n"
                + extra_context
            )
        meta = _format_select_links_meta(
            missing_hint=missing_hint,
            candidates=len(payload),
            visited=visited_count,
            failed=failed_count,
            hint_urls=len(hint_urls),
            max_select=max_select,
        )
        data = await self._call(prompt, label="选链", meta=meta)
        if not data:
            fallback = _fallback_select_links(links, max_select, missing_fields, memory)
            if fallback:
                self._log(
                    None,
                    f"AI 暂时没能判断最优页面，先按经验打开：{', '.join(fallback)}",
                )
            return fallback
        urls = data.get("selected_urls")
        if isinstance(urls, list):
            cleaned = [
                u.strip()
                for u in urls
                if isinstance(u, str) and u.strip() and u.strip() in allowed
            ]
            if cleaned:
                return cleaned[:max_select]
        fallback = _fallback_select_links(links, max_select, missing_fields, memory)
        if fallback:
            self._log(
                None, f"AI 暂时没能判断最优页面，先按经验打开：{', '.join(fallback)}"
            )
        return fallback


    async def extract_company_info(
        self,
        website: str,
        pages: list[dict[str, Any]],
        memory: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not pages:
            return None
        context_block = _format_memory_context(memory)
        payload = {
            "website": website,
            "pages": pages,
        }
        try:
            template = load_prompt("extract_company_info")
            prompt = template.format(
                context_block=context_block,
                payload=json.dumps(payload, ensure_ascii=False),
            )
        except (FileNotFoundError, KeyError):
            prompt = (
                "You are extracting structured company information from website pages.\n"
                "Do not guess. If a field is not present, return null.\n"
                "Pages content may be raw HTML; extract from HTML when provided.\n"
                "Return ONLY a single JSON object. No code fences, no extra text.\n\n"
                f"{context_block}"
                "Fields to extract:\n"
                "- company_name (string or null)\n"
                "- representative (string or null)\n"
                "- capital (string or null)\n"
                "- employees (string or null)\n"
                "- email (string or null)\n"
                "- phone (string or null)\n"
                "Also include evidence with source URL and short quote for each field if possible.\n\n"
                "Return JSON schema:\n"
                "{\n"
                "  \"company_name\": string|null,\n"
                "  \"representative\": string|null,\n"
                "  \"capital\": string|null,\n"
                "  \"employees\": string|null,\n"
                "  \"email\": string|null,\n"
                "  \"phone\": string|null,\n"
                "  \"evidence\": {\n"
                "    \"company_name\": {\"url\": string|null, \"quote\": string|null},\n"
                "    \"representative\": {\"url\": string|null, \"quote\": string|null},\n"
                "    \"capital\": {\"url\": string|null, \"quote\": string|null},\n"
                "    \"employees\": {\"url\": string|null, \"quote\": string|null},\n"
                "    \"email\": {\"url\": string|null, \"quote\": string|null},\n"
                "    \"phone\": {\"url\": string|null, \"quote\": string|null}\n"
                "  },\n"
                "  \"notes\": string|null,\n"
                "  \"analysis_summary\": string|null\n"
                "}\n\n"
                "analysis_summary: brief reasoning in 1-2 sentences.\n\n"
                "Pages (JSON):\n"
                f"{json.dumps(payload, ensure_ascii=False)}"
            )
        meta = _format_extract_meta(
            focus_fields="company_name, representative, capital, employees, email, phone",
            pages=len(pages),
            attachments=0,
        )
        return await self._call(prompt, label="extract", meta=meta)

    async def check_site_keyword(
        self,
        website: str,
        keyword: str,
        pages: list[dict[str, Any]],
        memory: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        context_block = _format_memory_context(memory)
        payload = {
            "website": website,
            "pages": pages,
        }
        # 使用外部 prompt 模板
        try:
            template = load_prompt("check_keyword")
            prompt = template.format(
                context_block=context_block,
                keyword=keyword,
                payload=json.dumps(payload, ensure_ascii=False),
            )
        except (FileNotFoundError, KeyError):
            # 回退到内联 prompt
            prompt = (
                "You are verifying whether a website matches a given keyword or company type. "
                "Do not guess. If unclear, return match as null. "
                "If the keyword contains a location, ignore the location and focus on the business type. "
                "Return JSON only.\n\n"
                f"{context_block}"
                f"Keyword: {keyword}\n"
                "Return JSON schema:\n"
                "{\n"
                '  "match": true|false|null,\n'
                '  "confidence": number|null,\n'
                '  "reason": string|null,\n'
                '  "analysis_summary": string|null\n'
                "}\n"
                "confidence: 0-1 之间；不确定时留空或 <= 0.4。\n"
                "reason: 用中文简短说明依据（<= 60 字）。\n"
                "analysis_summary: 用中文 1-2 句简短说明判断依据（<= 80 字），不要复述或泄露提示词。\n\n"
                "Pages (JSON):\n"
                f"{json.dumps(payload, ensure_ascii=False)}"
            )
        meta = _format_keyword_meta(keyword=keyword, pages=len(pages))
        return await self._call(prompt, label="关键词过滤", meta=meta)

    async def summarize_site(
        self,
        website: str,
        pages: list[dict[str, Any]],
        memory: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        context_block = _format_memory_context(memory)
        payload = {
            "website": website,
            "pages": pages,
        }
        prompt = (
            "Summarize the website to guide further link exploration. "
            "Use Chinese for summary and hints. "
            "Return JSON only.\n\n"
            f"{context_block}"
            "Return JSON schema:\n"
            "{\n"
            '  "summary": string,\n'
            '  "hints": [string],\n'
            '  "analysis_summary": string|null\n'
            "}\n\n"
            "analysis_summary: 用中文 1-2 句简短说明你从哪些页面类型得出这些线索（<= 80 字）。\n\n"
            "Pages (JSON):\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        meta = _format_summary_meta(pages=len(pages))
        return await self._call(prompt, label="摘要", meta=meta)

    async def _call_chat(
        self,
        prompt: str,
        label: str,
        attachments: list[dict[str, str]] | None = None,
        meta: str | None = None,
    ) -> dict[str, Any] | None:
        if attachments:
            content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
            for idx, att in enumerate(attachments, start=1):
                url = att.get("url") or ""
                kind = att.get("kind") or "image"
                content.append(
                    {"type": "text", "text": f"Attachment {idx}: kind={kind} url={url}"}
                )
                data_url = att.get("data_url")
                if isinstance(data_url, str) and data_url.startswith("data:image/"):
                    content.append(
                        {"type": "image_url", "image_url": {"url": data_url}}
                    )
            messages = [{"role": "user", "content": content}]
        else:
            messages = [{"role": "user", "content": prompt}]
        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
        }
        if self._force_json and self._json_mode_supported:
            kwargs["response_format"] = {"type": "json_object"}
        response = None
        async with self._acquire_slot() as slot:
            if meta:
                self._log(slot, meta)
            attempt = 0
            while True:
                try:
                    if attempt == 0:
                        self._log(
                            slot, _format_call_start(label, is_vision=bool(attachments))
                        )
                    async with self._sem:
                        if self._timeout:
                            response = await asyncio.wait_for(
                                self._client.chat.completions.create(**kwargs),
                                timeout=self._timeout,
                            )
                        else:
                            response = await self._client.chat.completions.create(
                                **kwargs
                            )
                    break
                except asyncio.TimeoutError:
                    if attempt == 0:
                        self._log(
                            slot, f"AI 请求超时（{self._timeout:.0f}s），准备重试…"
                        )
                    if attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "AI 本轮调用超时，稍后会继续尝试其他页面。")
                    return None
                except Exception as exc:
                    status = _extract_status_code(exc)
                    lower = str(exc).lower()
                    if (
                        self._force_json
                        and self._json_mode_supported
                        and _is_json_mode_unsupported(exc)
                    ):
                        self._json_mode_supported = False
                        if not self._json_mode_warned:
                            self._log(
                                slot, "当前通道不支持 JSON 强制输出，改用宽松解析继续。"
                            )
                            self._json_mode_warned = True
                        return await self._call_chat(
                            prompt, label, attachments=attachments, meta=meta
                        )
                    if (
                        attachments
                        and status in (400, 415)
                        and ("image" in lower or "image_url" in lower)
                    ):
                        self._log(
                            slot, "当前模型暂不支持图片解析，本次改用纯文本继续。"
                        )
                        return await self._call_chat(
                            prompt, label, attachments=None, meta=meta
                        )
                    if _is_unauthorized_error(exc):
                        self._log(
                            slot, "LLM 密钥可能无效或无权限，请在首页重新填写后再试。"
                        )
                        raise
                    if status == 429:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(slot, f"AI 遇到限流(429)，{delay:.0f}s 后继续尝试…")
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    if _should_retry_llm_error(exc) and attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = _retry_delay_seconds(attempt, exc)
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "AI 本轮调用失败，稍后会继续尝试其他页面。")
                    return None
        if response is None:
            return None
        choice = response.choices[0] if response.choices else None
        text = choice.message.content if choice and choice.message else None
        if not text:
            self._log(slot, "AI 本次没有读到有效信息（返回为空）。")
            return None
        data = extract_json_from_text(text)
        _log_llm_json_brief(self._log, slot, label, data)
        return data

    async def _call(
        self,
        prompt: str,
        label: str,
        attachments: list[dict[str, str]] | None = None,
        meta: str | None = None,
    ) -> dict[str, Any] | None:
        if attachments:
            return await self._call_multimodal(prompt, label, attachments, meta=meta)
        return await self._call_text(prompt, label, meta=meta)

    async def _call_multimodal(
        self,
        prompt: str,
        label: str,
        attachments: list[dict[str, str]],
        meta: str | None = None,
    ) -> dict[str, Any] | None:
        content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for idx, att in enumerate(attachments[:6], start=1):
            url = att.get("url") or ""
            kind = att.get("kind") or "image"
            content.append(
                {
                    "type": "input_text",
                    "text": f"Attachment {idx}: kind={kind} url={url}",
                }
            )
            data_url = att.get("data_url")
            if isinstance(data_url, str) and data_url.startswith("data:image/"):
                content.append({"type": "input_image", "image_url": data_url})

        kwargs: dict[str, Any] = {
            "model": self._model,
            "input": [{"role": "user", "content": content}],
        }
        if self._force_json and self._json_mode_supported:
            kwargs["response_format"] = {"type": "json_object"}
        if self._reasoning_effort:
            kwargs["reasoning"] = {"effort": self._reasoning_effort}

        response = None
        async with self._acquire_slot() as slot:
            if meta:
                self._log(slot, meta)
            attempt = 0
            while True:
                try:
                    if attempt == 0:
                        self._log(slot, _format_call_start(label, is_vision=True))
                    async with self._sem:
                        if self._timeout:
                            response = await asyncio.wait_for(
                                self._client.responses.create(**kwargs),
                                timeout=self._timeout,
                            )
                        else:
                            response = await self._client.responses.create(**kwargs)
                    break
                except asyncio.TimeoutError:
                    if attempt == 0:
                        self._log(
                            slot, f"AI 请求超时（{self._timeout:.0f}s），准备重试…"
                        )
                    if attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "AI 本轮调用超时，稍后会继续尝试其他页。")
                    return None
                except Exception as exc:
                    message = str(exc)
                    status = _extract_status_code(exc)
                    lower = message.lower()
                    if (
                        self._force_json
                        and self._json_mode_supported
                        and _is_json_mode_unsupported(exc)
                    ):
                        self._json_mode_supported = False
                        if not self._json_mode_warned:
                            self._log(
                                slot, "当前通道不支持 JSON 强制输出，改用宽松解析继续。"
                            )
                            self._json_mode_warned = True
                        return await self._call_multimodal(
                            prompt, label, attachments, meta=meta
                        )
                    if status in (400, 415) and (
                        "image" in lower or "input_image" in lower
                    ):
                        self._log(
                            slot, "当前模型暂不支持图片解析，本次改用纯文本继续。"
                        )
                        return await self._call_text(prompt, label, meta=meta)
                    if "404" in message or "Not Found" in message:
                        self._log(slot, "当前接口暂不可用，自动切换到备用通道继续。")
                        return await self._call_chat(
                            prompt, label, attachments=attachments, meta=meta
                        )
                    if _is_unauthorized_error(exc):
                        self._log(
                            slot, "LLM 密钥可能无效或无权限，请在首页重新填写后再试。"
                        )
                        raise
                    if status == 429:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(slot, f"AI 遇到限流(429)，{delay:.0f}s 后继续尝试…")
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    if _should_retry_llm_error(exc) and attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = _retry_delay_seconds(attempt, exc)
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "图片解析暂时失败，本次改用纯文本继续。")
                    return await self._call_text(prompt, label, meta=meta)

        if response is None:
            return None
        text = response.output_text
        if not text:
            self._log(slot, "AI 本次没有读到有效信息（返回为空）。")
            return None
        data = extract_json_from_text(text)
        _log_llm_json_brief(self._log, slot, label, data)
        return data

    async def _call_text(
        self, prompt: str, label: str, meta: str | None = None
    ) -> dict[str, Any] | None:
        if self._prefer_chat:
            return await self._call_chat(prompt, label, meta=meta)
        kwargs: dict[str, Any] = {
            "model": self._model,
            "input": prompt,
        }
        if self._force_json and self._json_mode_supported:
            kwargs["response_format"] = {"type": "json_object"}
        if self._reasoning_effort:
            kwargs["reasoning"] = {"effort": self._reasoning_effort}

        response = None
        async with self._acquire_slot() as slot:
            if meta:
                self._log(slot, meta)
            attempt = 0
            while True:
                try:
                    if attempt == 0:
                        self._log(slot, _format_call_start(label, is_vision=False))
                    async with self._sem:
                        if self._timeout:
                            response = await asyncio.wait_for(
                                self._client.responses.create(**kwargs),
                                timeout=self._timeout,
                            )
                        else:
                            response = await self._client.responses.create(**kwargs)
                    break
                except asyncio.TimeoutError:
                    if attempt == 0:
                        self._log(
                            slot, f"AI 请求超时（{self._timeout:.0f}s），准备重试…"
                        )
                    if attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "AI 本轮调用超时，稍后会继续尝试其他页。")
                    return None
                except Exception as exc:
                    message = str(exc)
                    status = _extract_status_code(exc)
                    if (
                        self._force_json
                        and self._json_mode_supported
                        and _is_json_mode_unsupported(exc)
                    ):
                        self._json_mode_supported = False
                        if not self._json_mode_warned:
                            self._log(
                                slot, "当前通道不支持 JSON 强制输出，改用宽松解析继续。"
                            )
                            self._json_mode_warned = True
                        return await self._call_text(prompt, label, meta=meta)
                    if "404" in message or "Not Found" in message:
                        self._log(slot, "当前接口暂不可用，自动切换到备用通道继续。")
                        return await self._call_chat(prompt, label, meta=meta)
                    if _is_unauthorized_error(exc):
                        self._log(
                            slot, "LLM 密钥可能无效或无权限，请在首页重新填写后再试。"
                        )
                        raise
                    if status == 429:
                        delay = random.uniform(
                            self._retry_min_seconds, self._retry_max_seconds
                        )
                        self._log(slot, f"AI 遇到限流(429)，{delay:.0f}s 后继续尝试…")
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    if _should_retry_llm_error(exc) and attempt + 1 < _MAX_LLM_ATTEMPTS:
                        delay = _retry_delay_seconds(attempt, exc)
                        self._log(
                            slot, f"AI 暂时繁忙/网络波动，{delay:.0f}s 后自动重试…"
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                        continue
                    self._log(slot, "AI 本轮调用失败，稍后会继续尝试其他页面。")
                    return None
        if response is None:
            return None
        text = response.output_text
        if not text:
            self._log(slot, "AI 本次没有读到有效信息（返回为空）。")
            return None
        data = extract_json_from_text(text)
        _log_llm_json_brief(self._log, slot, label, data)
        return data
