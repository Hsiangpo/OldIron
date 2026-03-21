from __future__ import annotations

import ast
import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse, urlunparse


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def normalize_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if not value.startswith("http://") and not value.startswith("https://"):
        value = "https://" + value
    parsed = urlparse(value)
    if not parsed.netloc:
        return ""
    cleaned = parsed._replace(fragment="")
    return urlunparse(cleaned).rstrip("/")


def canonical_site_key(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if not value.startswith("http://") and not value.startswith("https://"):
        value = "https://" + value
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def extract_domain_from_url(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host.strip(".")


def is_same_domain(base_url: str, candidate: str) -> bool:
    base = (urlparse(base_url).netloc or "").lower()
    target = (urlparse(candidate).netloc or "").lower()
    if base.startswith("www."):
        base = base[4:]
    if target.startswith("www."):
        target = target[4:]
    return base == target


def url_depth(url: str) -> int:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/")
    if not path:
        return 0
    return len([seg for seg in path.split("/") if seg])


def is_sitemap_like_url(url: str) -> bool:
    value = (url or "").strip()
    if not value:
        return False
    lower = value.lower()
    if "sitemap" in lower or "site-map" in lower or "site_map" in lower:
        return True
    if "サイトマップ" in value:
        return True
    return False


_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.I | re.S)


def extract_json_from_text(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = text.strip()
    # Prefer fenced json blocks first.
    for match in _JSON_BLOCK_RE.finditer(cleaned):
        candidate = match.group(1)
        parsed = _parse_json_candidate(candidate)
        if isinstance(parsed, dict):
            return parsed
    parsed = _parse_json_candidate(cleaned)
    if isinstance(parsed, dict):
        return parsed
    snippet = _extract_balanced_object(cleaned)
    if snippet:
        parsed = _parse_json_candidate(snippet)
        if isinstance(parsed, dict):
            return parsed
    parsed = _parse_key_value_fallback(cleaned)
    if isinstance(parsed, dict):
        return parsed
    return None


_KV_BOOL_KEYS = {"is_valid", "match"}
_KV_LIST_KEYS = {"selected_urls", "hints"}
_KV_KEYS = _KV_BOOL_KEYS | _KV_LIST_KEYS | {
    "company_name",
    "representative",
    "analysis_summary",
    "notes",
    "email",
    "emails",
    "evidence_url",
    "cleaned_name",
    "confidence",
    "reason",
    "summary",
}


def _parse_key_value_fallback(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    result: dict[str, Any] = {}
    current_list_key: str | None = None
    list_buffer: list[str] = []

    def flush_list() -> None:
        nonlocal current_list_key, list_buffer
        if current_list_key and list_buffer:
            cleaned = [_strip_wrapping_quotes(_strip_bullets(v)) for v in list_buffer if v.strip()]
            if cleaned:
                result[current_list_key] = cleaned
        current_list_key = None
        list_buffer = []

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if current_list_key and _looks_like_list_item(line):
            list_buffer.append(line)
            continue
        match = re.match(r"^[\\-\\*\\s]*['\"]?(?P<key>[A-Za-z_]+)['\"]?\\s*[:=]\\s*(?P<val>.+)$", line)
        if not match:
            continue
        key = match.group("key").strip().lower()
        if key not in _KV_KEYS:
            continue
        val = match.group("val").strip()
        flush_list()
        if key in _KV_BOOL_KEYS:
            parsed_bool = _parse_bool(val)
            if parsed_bool is not None:
                result[key] = parsed_bool
            continue
        if key in _KV_LIST_KEYS:
            parsed_list = _parse_list_value(val)
            if parsed_list:
                result[key] = parsed_list
            else:
                current_list_key = key
            continue
        if key == "confidence":
            parsed_num = _parse_float(val)
            if parsed_num is not None:
                result[key] = parsed_num
            continue
        result[key] = _strip_wrapping_quotes(val)
    flush_list()
    return result or None


def _parse_bool(value: str) -> bool | None:
    lowered = value.strip().lower()
    if lowered in {"true", "yes", "y", "1"}:
        return True
    if lowered in {"false", "no", "n", "0"}:
        return False
    return None


def _parse_float(value: str) -> float | None:
    try:
        return float(re.sub(r"[^0-9.+-]", "", value))
    except ValueError:
        return None


def _parse_list_value(value: str) -> list[str]:
    cleaned = value.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    parts = [p.strip() for p in re.split(r"[,\u3001]", cleaned) if p.strip()]
    return [_strip_wrapping_quotes(p) for p in parts if p]


def _strip_bullets(value: str) -> str:
    return re.sub(r"^[\\-\\*\\s]+", "", value).strip()


def _strip_wrapping_quotes(value: str) -> str:
    stripped = value.strip()
    if (stripped.startswith("\"") and stripped.endswith("\"")) or (stripped.startswith("'") and stripped.endswith("'")):
        return stripped[1:-1].strip()
    return stripped


def _looks_like_list_item(value: str) -> bool:
    if value.startswith(("-", "*")):
        return True
    return "http://" in value or "https://" in value


def _parse_json_candidate(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = text.strip()
    # Remove ```json fences if they leaked in.
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json", "", 1).strip()
    for candidate in (cleaned, _strip_trailing_commas(cleaned)):
        try:
            data = json.loads(candidate)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass
    # Fallback: tolerate Python-like dicts produced by LLM.
    python_like = _replace_json_literals(cleaned)
    try:
        data = ast.literal_eval(python_like)
    except (SyntaxError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _strip_trailing_commas(text: str) -> str:
    return re.sub(r",\s*([}\]])", r"\1", text)


def _replace_json_literals(text: str) -> str:
    replacements = {"true": "True", "false": "False", "null": "None"}
    out: list[str] = []
    in_string: str | None = None
    escape = False
    i = 0
    while i < len(text):
        ch = text[i]
        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == in_string:
                in_string = None
            i += 1
            continue
        if ch in ("'", '"'):
            in_string = ch
            out.append(ch)
            i += 1
            continue
        replaced = False
        for token, replacement in replacements.items():
            if text[i : i + len(token)].lower() == token and _is_boundary(text, i, len(token)):
                out.append(replacement)
                i += len(token)
                replaced = True
                break
        if replaced:
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _is_boundary(text: str, start: int, length: int) -> bool:
    before = text[start - 1] if start > 0 else ""
    after = text[start + length] if start + length < len(text) else ""
    return not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_")


def _extract_balanced_object(text: str) -> str | None:
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_string: str | None = None
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == in_string:
                in_string = None
            continue
        if ch in ("'", '"'):
            in_string = ch
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def safe_slug(value: str) -> str:
    allowed = []
    for ch in value:
        if ch.isalnum() or ch in ("-", "_", "."):
            allowed.append(ch)
        elif ch == "/":
            allowed.append("-")
    slug = "".join(allowed).strip("-")
    return slug[:80] if slug else "page"
