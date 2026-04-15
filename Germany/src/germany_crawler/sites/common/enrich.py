"""德国代表人合并与官网补充配置。"""

from __future__ import annotations

import html
import os
import re
from pathlib import Path
from urllib.parse import urlparse

from oldiron_core.fc_email.email_service import DEFAULT_LLM_API_STYLE
from oldiron_core.fc_email.email_service import DEFAULT_LLM_BASE_URL
from oldiron_core.fc_email.email_service import DEFAULT_LLM_MODEL
from oldiron_core.fc_email.email_service import DEFAULT_LLM_REASONING_EFFORT
from oldiron_core.fc_email.email_service import FirecrawlEmailSettings


_REP_TITLE_PREFIX = re.compile(
    r"^(?:contact(?:\s+name|\s+person)?|representative|rep|name|mr|mrs|ms|dr|eng|er|sheikh|shaikh|sir)\.?\s*[:：-]?\s*",
    re.IGNORECASE,
)
_REP_TITLE_SUFFIX = re.compile(
    r"\b(?:ceo|coo|cfo|cto|owner|founder|manager|director|managing director|area manager|sales manager|general manager|partner)$",
    re.IGNORECASE,
)
_REP_INLINE_ROLE_SPLIT = re.compile(
    r"\s+\b(?:"
    r"executive director|managing director|general manager|deputy manager|"
    r"manager|director|chairman|chief executive officer|chief executive|"
    r"ceo|founder|owner|partner|supervisor"
    r")\b.*$",
    re.IGNORECASE,
)
_REP_CORP_HINTS = (
    "llc",
    "l.l.c",
    "ltd",
    "fze",
    "fzco",
    "fzc",
    "company",
    "trading",
    "services",
    "service",
    "industries",
    "industrial",
    "technology",
    "tech",
    "group",
    "est",
    "branch",
    "bank",
    "clinic",
    "hospital",
    "restaurant",
    "hotel",
    "construction",
    "contracting",
    "properties",
    "real estate",
    "development",
    "solutions",
    "international",
    "middle east",
    "auditors",
    "auditing",
    "dmcc",
)
_REP_NON_NAME_WORDS = {
    "space",
    "team",
    "staff",
    "admin",
    "office",
    "support",
    "sales",
    "marketing",
    "info",
    "contact",
    "publicidade",
    "dubai",
    "uae",
    "pk",
}
_REP_EMPTY_VALUES = {"", "-", "n/a", "na", "none", "null", "unknown"}
_BAD_WEBSITE_HOSTS = {
    "share.google",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "linkedin.com",
    "www.linkedin.com",
    "maps.app.goo.gl",
}


def build_email_settings(output_dir: Path) -> FirecrawlEmailSettings:
    """构建官网邮箱/代表人补充配置。"""
    return FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL),
        llm_model=os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT),
        llm_api_style=os.getenv("LLM_API_STYLE", DEFAULT_LLM_API_STYLE),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "12")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "5")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "5")),
    )


def merge_representatives(p1_value: str, p3_value: str, company_name: str) -> str:
    """按 P1;P3 顺序合并代表人，并去重。"""
    merged: list[str] = []
    seen: set[str] = set()
    for raw in (p1_value, p3_value):
        clean = normalize_person_name(raw, company_name)
        if not clean:
            continue
        key = _normalize_rep_key(clean)
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(clean)
    return ";".join(merged)


def normalize_person_name(value: str, company_name: str = "") -> str:
    """尽量把代表人字段收敛成纯人名。"""
    text = html.unescape(str(value or "")).replace("\u3000", " ").strip(" \t\r\n|/;,:：")
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = _REP_TITLE_PREFIX.sub("", text).strip(" \t\r\n|/;,:：")
    text = _REP_TITLE_SUFFIX.sub("", text).strip(" \t\r\n|/;,:：")
    text = _REP_INLINE_ROLE_SPLIT.sub("", text).strip(" \t\r\n|/;,:：")
    if text.lower() in _REP_EMPTY_VALUES:
        return ""
    if any(token in text.lower() for token in ("http", "www.", "@")):
        return ""
    if company_name and _normalize_rep_key(text) == _normalize_rep_key(company_name):
        return ""
    if sum(ch.isdigit() for ch in text) >= 3:
        return ""
    if len(text) < 2 or len(text) > 80:
        return ""
    lowered = text.lower()
    if any(token in _REP_NON_NAME_WORDS for token in lowered.split()):
        return ""
    if any(token in lowered for token in _REP_CORP_HINTS):
        return ""
    if len(re.findall(r"[A-Za-z\u0600-\u06FF]", text)) < 2:
        return ""
    if _looks_like_gibberish_person_name(text):
        return ""
    return _normalize_latin_person_casing(text)


def normalize_website_url(value: str) -> str:
    """把站点里的官网字段收敛成真实官网 URL。"""
    text = html.unescape(str(value or "")).strip(" \t\r\n,;|<>[](){}'\"")
    if not text:
        return ""
    matched = re.search(r"https?://[^\s<>'\"]+", text, flags=re.I)
    if matched is not None:
        text = matched.group(0)
    text = text.rstrip(".,;:)")
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = str(parsed.netloc or "").strip().lower()
    if not host or "+" in host or "." not in host:
        return ""
    if host in _BAD_WEBSITE_HOSTS:
        return ""
    suffix = host.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z]{2,24}", suffix):
        return ""
    path = parsed.path or ""
    normalized = f"{parsed.scheme}://{host}{path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def _normalize_rep_key(value: str) -> str:
    return re.sub(r"[^a-z0-9\u0600-\u06FF]+", "", str(value or "").lower())


def _looks_like_gibberish_person_name(value: str) -> bool:
    tokens = [token for token in re.split(r"\s+", str(value or "").strip()) if token]
    if not tokens:
        return True
    if len(tokens) == 2 and len(tokens[0]) == 1:
        second = re.sub(r"[^A-Za-z]", "", tokens[1])
        if len(second) >= 5 and not re.search(r"[aeiouAEIOU]", second):
            return True
    for token in tokens:
        plain = re.sub(r"[^A-Za-z]", "", token)
        if len(plain) >= 5 and not re.search(r"[aeiouAEIOU]", plain):
            return True
    return False


def _normalize_latin_person_casing(value: str) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z .'\-]+", text):
        return text
    return " ".join(_title_case_person_token(token) for token in text.split())


def _title_case_person_token(token: str) -> str:
    parts = re.split(r"([\-'])", token)
    normalized: list[str] = []
    for part in parts:
        if part in {"-", "'"}:
            normalized.append(part)
            continue
        normalized.append(part[:1].upper() + part[1:].lower() if part else "")
    return "".join(normalized)
