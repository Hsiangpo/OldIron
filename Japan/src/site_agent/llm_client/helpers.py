from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import unquote, urlparse

from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    InternalServerError,
    RateLimitError,
)

from ..models import LinkItem
from ..utils import is_sitemap_like_url, url_depth

def _log_llm_json_brief(
    logger, slot: int | None, label: str, data: dict[str, Any] | None
) -> None:
    if not isinstance(data, dict):
        logger(slot, "AI 本次输出格式异常（已自动继续下一步）。")
        return
    if label == "选链":
        urls = data.get("selected_urls")
        if isinstance(urls, list):
            cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
            preview = cleaned[:6]
            if cleaned:
                logger(slot, f"已选中 {len(cleaned)} 个页面：{', '.join(preview)}")
            else:
                logger(slot, "AI 暂时没有选中页面（将按经验策略继续）。")
            _log_analysis_summary(logger, slot, data)
            return
    if label in ("抽取", "抽取(vision)", "extract", "extract(vision)"):
        fields = {
            "company_name": data.get("company_name"),
            "representative": data.get("representative"),
            "email": data.get("email"),
            "phone": data.get("phone"),
        }
        present = [k for k, v in fields.items() if isinstance(v, str) and v.strip()]
        missing = [k for k in ("company_name", "representative", "email", "phone") if k not in present]
        found_zh = "、".join(_field_name_zh(k) for k in present) if present else ""
        miss_zh = "、".join(_field_name_zh(k) for k in missing) if missing else ""
        if found_zh:
            if miss_zh:
                logger(slot, f"提取完成：已找到 {found_zh}；{miss_zh} 暂未公开。")
            else:
                logger(slot, f"提取完成：已找到 {found_zh}。")
        else:
            logger(slot, "AI 暂未在已打开页面中找到关键信息（会继续尝试）。")
        _log_analysis_summary(logger, slot, data)
        return
    if label == "摘要":
        hints = data.get("hints")
        preview = hints[:3] if isinstance(hints, list) else []
        cleaned = [x for x in preview if isinstance(x, str) and x.strip()]
        if cleaned:
            logger(slot, f"我建议优先查看：{', '.join(cleaned)}")
        _log_analysis_summary(logger, slot, data)
        return
    if label == "关键词过滤":
        match = data.get("match")
        confidence = data.get("confidence")
        reason = data.get("reason")
        if match is True:
            text = "关键词匹配：是"
        elif match is False:
            text = "关键词匹配：否"
        else:
            text = "关键词匹配：不确定"
        if isinstance(confidence, (int, float)):
            text = f"{text}（置信度 {confidence:.2f}）"
        logger(slot, text)
        if isinstance(reason, str) and reason.strip():
            logger(slot, f"判断依据：{reason.strip()}")
        _log_analysis_summary(logger, slot, data)
        return
    if label == "邮箱筛选":
        email = data.get("email")
        if isinstance(email, str) and email.strip():
            logger(slot, f"邮箱筛选完成：{email.strip()}")
        else:
            logger(slot, "邮箱筛选未命中合适结果。")
        _log_analysis_summary(logger, slot, data)
        return
    _log_analysis_summary(logger, slot, data)


def _log_analysis_summary(logger, slot: int | None, data: dict[str, Any]) -> None:
    summary = data.get("analysis_summary")
    if not (isinstance(summary, str) and summary.strip()):
        return
    text = summary.strip()
    if len(text) > 160:
        text = text[:160] + "…"
    logger(slot, f"AI 思路：{text}")


def _field_name_zh(field: str) -> str:
    return {
        "company_name": "公司名称",
        "representative": "代表人",
        "email": "邮箱",
        "phone": "座机",
    }.get(field, field)


def _format_missing_fields_zh(fields: list[str] | None) -> str:
    if not (isinstance(fields, list) and fields):
        return "公司名称、代表人"
    cleaned = [f for f in fields if isinstance(f, str) and f.strip()]
    if not cleaned:
        return "公司名称、代表人"
    return "、".join(_field_name_zh(f) for f in cleaned)


def _format_select_links_meta(
    *,
    missing_hint: str,
    candidates: int,
    visited: int,
    failed: int,
    hint_urls: int,
    max_select: int,
) -> str:
    lines = [
        f"正在从 {candidates} 个页面里挑选 {max_select} 个重点页面（目标：{missing_hint}）…",
    ]
    if hint_urls:
        lines.append(f"站点线索：已注意到 {hint_urls} 条可能相关的内部链接。")
    if visited or failed:
        lines.append(f"已看过 {visited} 个页面，跳过失败 {failed} 个页面。")
    return "\n".join(lines)


def _format_extract_meta(*, focus_fields: str, pages: int, attachments: int) -> str:
    lines = [
        f"正在从 {pages} 个页面中提取 {focus_fields}…",
    ]
    if attachments:
        lines.append(f"同时查看 {attachments} 张图片/PDF 截图，防止信息写在图片里。")
    return "\n".join(lines)


def _format_summary_meta(*, pages: int) -> str:
    return f"AI 正在快速梳理网站结构（已打开 {pages} 个页面），给出下一步优先访问的页面建议…"


def _format_keyword_meta(*, keyword: str, pages: int) -> str:
    return f"正在判断站点是否匹配关键词“{keyword}”（已打开 {pages} 个页面）…"


def _format_call_start(label: str, *, is_vision: bool) -> str:
    if label == "选链":
        return "正在做页面筛选…"
    if label in ("抽取", "抽取(vision)", "extract", "extract(vision)"):
        return "正在提取公司信息…"
    if label == "摘要":
        return "正在整理网站结构与下一步线索…"
    if label == "关键词过滤":
        return "正在判断站点是否匹配关键词…"
    return "AI 正在处理…"

def _trim_prompt_context(text: str | None, max_chars: int = 2000) -> str | None:
    if not (isinstance(text, str) and text.strip()):
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    if max_chars > 0 and len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars] + "…"
    return cleaned


def _format_memory_context(
    memory: dict[str, Any] | None, *, homepage_context: str | None = None
) -> str:
    if not memory:
        return ""
    summary = memory.get("summary") if isinstance(memory.get("summary"), str) else None
    hints = memory.get("hints") if isinstance(memory.get("hints"), list) else None
    visited = memory.get("visited") if isinstance(memory.get("visited"), list) else None
    failed = memory.get("failed") if isinstance(memory.get("failed"), list) else None
    selected = (
        memory.get("selected") if isinstance(memory.get("selected"), list) else None
    )
    last_missing = (
        memory.get("last_missing")
        if isinstance(memory.get("last_missing"), list)
        else None
    )
    pool_size = (
        memory.get("link_pool_size")
        if isinstance(memory.get("link_pool_size"), int)
        else None
    )
    found = memory.get("found") if isinstance(memory.get("found"), dict) else None
    parts = []
    if summary:
        parts.append(f"Context summary: {summary}\n")
    if hints:
        parts.append(f"Hints: {json.dumps(hints[:10], ensure_ascii=False)}\n")
    if found:
        parts.append(f"Already found: {json.dumps(found, ensure_ascii=False)}\n")
    if last_missing:
        parts.append(f"Missing now: {json.dumps(last_missing, ensure_ascii=False)}\n")
    if pool_size is not None:
        parts.append(f"Link pool size: {pool_size}\n")
    if selected:
        parts.append(
            f"Selected URLs: {json.dumps(selected[-20:], ensure_ascii=False)}\n"
        )
    if visited:
        parts.append(f"Visited URLs: {json.dumps(visited[:50], ensure_ascii=False)}\n")
    if failed:
        parts.append(f"Failed URLs: {json.dumps(failed[:50], ensure_ascii=False)}\n")
    if isinstance(homepage_context, str) and homepage_context.strip():
        parts.append(f"Homepage context: {homepage_context.strip()}\n")
    return "".join(parts)


def _fallback_select_links(
    links: list[LinkItem],
    max_select: int,
    missing_fields: list[str] | None,
    memory: dict[str, Any] | None,
) -> list[str]:
    if not links or max_select <= 0:
        return []
    visited = set(memory.get("visited", [])) if isinstance(memory, dict) else set()
    failed = set(memory.get("failed", [])) if isinstance(memory, dict) else set()
    hint_urls = _extract_hint_urls(memory)
    link_map = {item.url: item for item in links if item.url}
    picked: list[str] = []
    for url in hint_urls:
        if url in link_map and url not in visited and url not in failed:
            picked.append(url)
        if len(picked) >= max_select:
            return picked

    keywords = _build_keywords(missing_fields, memory)

    def score(item: LinkItem) -> int:
        url = (item.url or "").lower()
        text = (item.text or "").lower()
        if not url or url in visited or url in failed:
            return -1
        score_value = 0
        for kw, weight in keywords:
            if kw in url:
                score_value += weight
            if text and kw in text:
                score_value += max(1, weight // 2)
        score_value += _company_text_boost(item.text or "")
        score_value += _company_path_boost(item.url or "")
        score_value += _noise_path_penalty(item.url or "")
        # 目录层级权重：越浅越优先（常见关键页都比较浅）。
        depth = url_depth(item.url or "")
        score_value += max(0, 4 - depth)
        # sitemap 页面通常是链接列表，本身不包含关键信息，优先级应更低。
        if is_sitemap_like_url(item.url or ""):
            score_value -= 10
        return score_value

    scored = []
    for item in links:
        s = score(item)
        if s > 0:
            scored.append((s, item.url))
    scored.sort(key=lambda x: (-x[0], len(x[1])))
    for _, url in scored:
        if url not in picked:
            picked.append(url)
        if len(picked) >= max_select:
            return picked

    # If still nothing, pick the shortest unvisited internal links to backtrack.
    candidates = []
    for item in links:
        url = item.url
        if not url or url in visited or url in failed:
            continue
        depth = url.count("/")
        candidates.append((depth, len(url), url))
    candidates.sort()
    for _, _, url in candidates:
        if url not in picked:
            picked.append(url)
        if len(picked) >= max_select:
            break
    return picked


def _build_keywords(
    missing_fields: list[str] | None, memory: dict[str, Any] | None
) -> list[tuple[str, int]]:
    base = [
        ("会社概要", 8),
        ("会社案内", 7),
        ("会社情報", 7),
        ("企業情報", 7),
        ("企業概要", 7),
        ("会社紹介", 6),
        ("企業紹介", 6),
        ("企業案内", 6),
        ("会社データ", 6),
        ("企業データ", 6),
        ("会社プロフィール", 6),
        ("企業プロフィール", 6),
        ("企業情报", 6),
        ("企业信息", 6),
        ("企业情报", 6),
        ("会社", 4),
        ("代表者", 7),
        ("代表取締役", 7),
        ("代表", 6),
        ("社長", 6),
        ("会長", 5),
        ("取締役", 5),
        ("役員", 5),
        ("役員紹介", 5),
        ("役員一覧", 5),
        ("経営陣", 5),
        ("代表挨拶", 5),
        ("代表メッセージ", 5),
        ("社長挨拶", 5),
        ("社長メッセージ", 5),
        ("トップメッセージ", 5),
        ("ご挨拶", 4),
        ("挨拶", 4),
        ("沿革", 4),
        ("アクセス", 3),
        ("所在地", 3),
        ("about", 5),
        ("about-us", 5),
        ("aboutus", 5),
        ("company", 6),
        ("company-information", 6),
        ("company-info", 6),
        ("company-profile", 6),
        ("companyprofile", 6),
        ("company-outline", 6),
        ("companyoverview", 6),
        ("corporate", 5),
        ("corporate-information", 6),
        ("corporate-info", 6),
        ("corporate-profile", 6),
        ("corporate-outline", 6),
        ("corporate-overview", 6),
        ("profile", 6),
        ("outline", 6),
        ("overview", 5),
        ("info", 4),
        ("information", 4),
        ("message", 4),
        ("greeting", 4),
        ("chairman", 4),
    ]
    hints = memory.get("hints") if isinstance(memory, dict) else None
    hint_keywords: list[tuple[str, int]] = []
    if isinstance(hints, list):
        for hint in hints:
            if not isinstance(hint, str) or "http" in hint:
                continue
            for word in _extract_hint_words(hint):
                hint_keywords.append((word, 3))
    if not missing_fields:
        return hint_keywords + base
    extra = []
    if "representative" in missing_fields:
        extra.extend(
            [
                ("代表者", 8),
                ("代表取締役", 8),
                ("代表", 7),
                ("社長", 7),
                ("役員", 6),
                ("役員紹介", 6),
                ("トップメッセージ", 6),
                ("message", 5),
                ("greeting", 5),
            ]
        )
    if "company_name" in missing_fields:
        extra.extend(
            [
                ("company", 7),
                ("corporate", 6),
                ("profile", 6),
                ("outline", 6),
                ("会社", 6),
            ]
        )
    return extra + hint_keywords + base


def _extract_hint_urls(memory: dict[str, Any] | None) -> list[str]:
    if not isinstance(memory, dict):
        return []
    hints = memory.get("hints")
    if not isinstance(hints, list):
        return []
    urls: list[str] = []
    for hint in hints:
        if isinstance(hint, str) and hint.startswith("http"):
            urls.append(hint.strip())
    return urls


def _extract_hint_words(text: str) -> list[str]:
    words = []
    current = []
    for ch in text:
        if "a" <= ch.lower() <= "z":
            current.append(ch.lower())
        else:
            if len(current) >= 4:
                words.append("".join(current))
            current = []
    if len(current) >= 4:
        words.append("".join(current))
    return words


def _rank_links_for_prompt(
    links: list[LinkItem],
    missing_fields: list[str] | None,
    memory: dict[str, Any] | None,
    limit: int,
) -> list[LinkItem]:
    if not links:
        return []
    visited = set(memory.get("visited", [])) if isinstance(memory, dict) else set()
    failed = set(memory.get("failed", [])) if isinstance(memory, dict) else set()
    hint_urls = set(_extract_hint_urls(memory))
    keywords = _build_keywords(missing_fields, memory)

    def score(item: LinkItem) -> tuple[int, int, int]:
        url = (item.url or "").strip()
        if not url:
            return (-1, 999, 999999)
        if url in visited or url in failed:
            return (-1, 999, 999999)
        base_score = 0
        lower_url = url.lower()
        lower_text = (item.text or "").lower()
        for kw, weight in keywords:
            if kw in lower_url:
                base_score += weight
            if lower_text and kw in lower_text:
                base_score += max(1, weight // 2)
        base_score += _company_text_boost(item.text or "")
        if getattr(item, "is_nav", False):
            base_score += 6
        base_score += _company_path_boost(url)
        base_score += _noise_path_penalty(url)
        depth = url_depth(url)
        base_score += max(0, 4 - depth)
        if is_sitemap_like_url(url):
            base_score -= 10
        if url in hint_urls:
            base_score += 20
        return (base_score, depth, len(url))

    scored: list[tuple[tuple[int, int, int], LinkItem]] = []
    for item in links:
        scored.append((score(item), item))
    scored.sort(key=lambda x: (-x[0][0], x[0][1], x[0][2]))
    picked: list[LinkItem] = []
    for (s, _, _), item in scored:
        if s <= 0:
            continue
        picked.append(item)
        if len(picked) >= max(1, limit):
            break
    if picked:
        return picked
    # 没有明显关键词时，给 LLM 一些“最浅路径”的候选，便于回溯。
    shallow = sorted(
        [
            item
            for item in links
            if item.url and item.url not in visited and item.url not in failed
        ],
        key=lambda item: (url_depth(item.url or ""), len(item.url or "")),
    )
    return shallow[: max(1, limit)]


_COMPANY_PATH_PARTS = {
    "company",
    "about",
    "info",
    "profile",
    "outline",
    "overview",
    "corporate",
    "company-info",
    "company-information",
    "company-profile",
    "companyprofile",
    "company-outline",
    "companyoverview",
    "corporate-info",
    "corporate-information",
    "corporate-profile",
    "corporate-outline",
    "corporate-overview",
    "corporateinfo",
    "corp",
    "corp-info",
    "corpinfo",
    "company-data",
    "companydata",
    "about-us",
    "aboutus",
    "gaiyou",
    "kaisya",
    "kaisha",
}

_NOISE_PATH_PARTS = {
    "news",
    "topics",
    "press",
    "press-release",
    "blog",
    "column",
    "event",
    "events",
    "seminar",
    "search",
    "case",
    "cases",
    "works",
    "work",
    "product",
    "products",
    "service",
    "services",
    "solution",
    "solutions",
    "recruit",
    "recruitment",
    "career",
    "jobs",
    "job",
    "ir",
    "csr",
    "sustainability",
    "faq",
    "support",
    "shop",
    "store",
    "cart",
    "login",
    "signup",
    "お知らせ",
    "ニュース",
    "トピックス",
    "新着",
    "更新",
    "プレス",
    "リリース",
    "ブログ",
    "コラム",
    "イベント",
    "セミナー",
    "採用",
    "求人",
    "リクルート",
    "導入事例",
    "事例",
    "実績",
    "製品",
    "商品",
    "サービス",
    "ソリューション",
    "サポート",
    "faq",
    "qanda",
    "qa",
    "検索",
}

_GREETING_TOKENS = (
    "挨拶",
    "メッセージ",
    "message",
    "greeting",
)

_REP_ANCHOR_TOKENS = (
    "代表",
    "社長",
    "会長",
    "役員",
    "top-message",
    "topmessage",
    "greeting",
    "president",
    "ceo",
    "chairman",
)


def _noise_path_penalty(url: str) -> int:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower().strip("/")
    if path:
        path = unquote(path)
    if not path:
        return 0
    parts = [p for p in path.split("/") if p]
    if not parts:
        return 0
    text_blob = "/".join(parts)
    penalty = 0
    for part in parts:
        for token in _NOISE_PATH_PARTS:
            if token in part:
                penalty = min(penalty, -6)
                break
    return penalty


def _company_path_boost(url: str) -> int:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower().strip("/")
    if not path:
        return 0
    parts = [p for p in path.split("/") if p]
    if not parts:
        return 0
    if len(parts) <= 3 and parts[-1] in _COMPANY_PATH_PARTS:
        return 5
    return 0


_COMPANY_TEXT_BOOST_TOKENS = (
    "会社概要",
    "会社案内",
    "会社情報",
    "企業情報",
    "企業概要",
    "会社紹介",
    "企業紹介",
    "Company Profile",
    "Company Information",
    "About Us",
    "About",
    "Corporate",
)


def _company_text_boost(text: str) -> int:
    if not text:
        return 0
    lowered = text.lower()
    for token in _COMPANY_TEXT_BOOST_TOKENS:
        if token.isascii():
            if token.lower() in lowered:
                return 4
        elif token in text:
            return 5
    return 0


_MAX_LLM_ATTEMPTS = 10


def _is_json_mode_unsupported(exc: Exception) -> bool:
    status = _extract_status_code(exc)
    message = str(exc).lower()
    if status is None:
        if "response_format" in message or "unexpected keyword" in message:
            return True
    if status in (400, 415, 422):
        if (
            "response_format" in message
            or "json_object" in message
            or "json_schema" in message
            or "json mode" in message
            or ("unsupported" in message and "json" in message)
        ):
            return True
        # 某些网关会隐藏具体字段错误，遇到 400/415/422 也先降级一次。
        return True
    if status == 404 and (
        "response_format" in message
        or "json_object" in message
        or "json_schema" in message
        or "json mode" in message
    ):
        return True
    return False


def _should_retry_llm_error(exc: Exception) -> bool:
    # 用户要求：只要不是 401 状态，都可以重试（仅限于“LLM/HTTP 类错误”，避免对代码 bug 无限重试）。
    if _is_unauthorized_error(exc):
        return False
    if isinstance(
        exc,
        (
            AuthenticationError,
            APIStatusError,
            RateLimitError,
            APITimeoutError,
            APIConnectionError,
            InternalServerError,
        ),
    ):
        return True
    status = _extract_status_code(exc)
    if status is not None and status != 401:
        return True
    message = str(exc)
    if "upstream_account_rate_limited" in message:
        return True
    # 兜底：如果能从错误字符串中读到状态码，也按“非 401 都重试”。
    status = _extract_status_code_from_message(message)
    return status is not None and status != 401


def _is_unauthorized_error(exc: Exception) -> bool:
    status = _extract_status_code(exc)
    if status == 401:
        return True
    message = str(exc)
    if "Error code: 401" in message or "Unauthorized" in message:
        return True
    return False


def _extract_status_code(exc: Exception) -> int | None:
    if isinstance(exc, APIStatusError):
        return exc.status_code
    message = str(exc)
    return _extract_status_code_from_message(message)


def _extract_status_code_from_message(message: str) -> int | None:
    if not message:
        return None
    patterns = [
        r"Error code:\s*(\d{3})",
        r"status[_ -]?code[:=]\s*(\d{3})",
    ]
    for pat in patterns:
        m = re.search(pat, message)
        if not m:
            continue
        try:
            code = int(m.group(1))
        except ValueError:
            continue
        if 100 <= code <= 599:
            return code
    return None


def _retry_delay_seconds(attempt: int, exc: Exception) -> float:
    return 0.0


def _get_retry_after_seconds(exc: Exception) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    try:
        value = headers.get("Retry-After") or headers.get("retry-after")
    except Exception:
        return None
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
