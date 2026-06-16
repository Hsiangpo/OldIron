from __future__ import annotations

import html
import re
from urllib.parse import urljoin, urlparse

from oldironcrawler.extractor.protocol.errors import ProtocolPermanentError

CHARSET_RE = re.compile(r"charset\s*=\s*[\"']?\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
HTML_META_CHARSET_RE = re.compile(br"<meta[^>]+charset=[\"']?\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
XML_ENCODING_RE = re.compile(br"<\?xml[^>]+encoding=[\"']\s*([a-zA-Z0-9._-]+)", re.IGNORECASE)
EMAIL_SIGNAL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
ANCHOR_TAG_RE = re.compile(r"<a\b[^>]*>.*?</a>", re.IGNORECASE | re.DOTALL)
META_REFRESH_RE = re.compile(
    r"<meta\b[^>]*http-equiv\s*=\s*['\"]?\s*refresh\s*['\"]?[^>]*>",
    re.IGNORECASE | re.DOTALL,
)
META_REFRESH_URL_RE = re.compile(r"\burl\s*=\s*['\"]?([^'\";>]+)", re.IGNORECASE)
SCRIPT_LOCATION_RE = re.compile(
    r"(?:window\.)?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]|"
    r"(?:window\.)?location\.replace\(\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)
HTML_SIGNAL_PATTERNS = (
    re.compile(r"<h[1-4][^>]*>.*?</h[1-4]>", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"(founder|co-founder|owner|chairman|chief executive|managing director|group chief executive|president|principal solicitor|director|lead guide|leadership|executive team)",
        re.IGNORECASE,
    ),
    EMAIL_SIGNAL_RE,
)
CLOUDFLARE_CHALLENGE_HINTS = (
    "just a moment...",
    "cf-browser-verification",
    "challenge-platform",
    "cf-challenge",
    "attention required! | cloudflare",
)
SOFT_CHALLENGE_HINTS = (
    ".well-known/sgcaptcha",
    "sgcaptcha",
)
INCAPSULA_CHALLENGE_HINTS = (
    "_incapsula_resource",
    "incapsula incident id",
    "imperva",
)
TEXT_HINTS = ("text/html", "application/xhtml+xml", "application/xml", "text/xml", "text/plain")


def truncate_html(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    value_links = _collect_value_link_tags(text, max(max_chars // 4, 1))
    head_keep = max(max_chars // 3, 1)
    tail_keep = max(max_chars // 3, 1)
    middle_budget = max(max_chars - len(value_links) - head_keep - tail_keep - 96, 0)
    middle = _collect_signal_html_windows(text, middle_budget)
    parts = [value_links] if value_links else []
    if middle:
        parts.extend([middle, "\n<!-- 页面内容过长已截断，已保留中部重点片段 -->\n", text[:head_keep]])
    else:
        parts.append(text[:head_keep])
    parts.extend(["\n<!-- 页面内容过长已截断 -->\n", text[-tail_keep:]])
    return "".join(parts)[:max_chars]


def decode_response_text(response: object) -> str:
    content = getattr(response, "content", b"")
    headers = getattr(response, "headers", {})
    return decode_bytes(bytes(content or b""), str(headers.get("Content-Type", "") or ""))


def decode_bytes(content: bytes, content_type: str) -> str:
    if not content:
        return ""
    encodings = _candidate_encodings(content_type, content)
    for encoding in encodings:
        try:
            return content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return content.decode("utf-8", errors="replace")


def raise_if_challenge_page(url: str, html_text: str) -> None:
    challenge_kind = detect_challenge_kind(html_text)
    if not challenge_kind:
        return
    raise ProtocolPermanentError(f"{challenge_kind}: {url}")


def detect_challenge_kind(html_text: str) -> str:
    lowered = str(html_text or "").lower()
    if not lowered:
        return ""
    if any(hint in lowered for hint in CLOUDFLARE_CHALLENGE_HINTS):
        return "cloudflare_challenge"
    if any(hint in lowered for hint in SOFT_CHALLENGE_HINTS):
        return "sgcaptcha_challenge"
    if any(hint in lowered for hint in INCAPSULA_CHALLENGE_HINTS):
        return "imperva_challenge"
    return ""


def extract_same_site_meta_refresh_url(html_text: str, source_url: str) -> str:
    return extract_meta_refresh_url(html_text, source_url, allow_cross_site=False)


def extract_meta_refresh_url(html_text: str, source_url: str, *, allow_cross_site: bool = False) -> str:
    if detect_challenge_kind(html_text):
        return ""
    match = META_REFRESH_RE.search(str(html_text or ""))
    if match is None:
        return ""
    target_match = META_REFRESH_URL_RE.search(match.group(0))
    if target_match is None:
        return ""
    target_url = urljoin(source_url, html.unescape(str(target_match.group(1) or "").strip()))
    if allow_cross_site:
        return target_url if _is_supported_refresh_target(target_url) else ""
    if not _is_same_site_refresh_target(source_url, target_url):
        return ""
    return target_url


def extract_same_site_html_redirect_url(html_text: str, source_url: str) -> str:
    target_url = extract_same_site_meta_refresh_url(html_text, source_url)
    if target_url:
        return target_url
    return extract_same_site_script_location_url(html_text, source_url)


def extract_same_site_script_location_url(html_text: str, source_url: str) -> str:
    if detect_challenge_kind(html_text):
        return ""
    for match in SCRIPT_LOCATION_RE.finditer(str(html_text or "")):
        raw_target = next((group for group in match.groups() if group), "")
        target = html.unescape(str(raw_target or "").strip())
        if not target or target.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        target_url = urljoin(source_url, target)
        if _is_same_site_refresh_target(source_url, target_url):
            return target_url
    return ""


def _is_same_site_refresh_target(source_url: str, target_url: str) -> bool:
    source = urlparse(str(source_url or ""))
    target = urlparse(str(target_url or ""))
    if not _is_supported_refresh_target(target_url):
        return False
    source_host = _normalize_refresh_host(source.netloc)
    target_host = _normalize_refresh_host(target.netloc)
    return bool(source_host and source_host == target_host)


def _is_supported_refresh_target(target_url: str) -> bool:
    target = urlparse(str(target_url or ""))
    return target.scheme in {"http", "https"} and bool(target.netloc)


def _normalize_refresh_host(host: str) -> str:
    value = str(host or "").strip().lower().split("@")[-1].split(":", 1)[0]
    if value.startswith("www."):
        return value[4:]
    return value


def _collect_signal_html_windows(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    windows = _merge_html_signal_windows(_find_html_signal_windows(text))
    if not windows:
        return ""
    lines: list[str] = []
    for start, end in windows:
        fragment_lines = _extract_signal_lines(text[start:end])
        if not fragment_lines:
            continue
        for line in fragment_lines:
            if line not in lines:
                lines.append(line)
        summary = _render_signal_summary(lines)
        if len(summary) >= max_chars:
            return summary[:max_chars]
    return _render_signal_summary(lines)[:max_chars]


def _find_html_signal_windows(text: str) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for pattern in HTML_SIGNAL_PATTERNS:
        if pattern is EMAIL_SIGNAL_RE and "@" not in text:
            continue
        for match in pattern.finditer(text):
            start = max(match.start() - 900, 0)
            end = min(match.end() + 1600, len(text))
            windows.append((start, end))
            if len(windows) >= 24:
                return windows
    return windows


def _collect_value_link_tags(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    tags: list[str] = []
    used = 0
    for match in ANCHOR_TAG_RE.finditer(str(text or "")):
        tag = match.group(0)
        if not _is_value_link_tag(tag):
            continue
        clean = tag.strip()
        if not clean or clean in tags:
            continue
        next_used = used + len(clean) + 1
        if next_used > max_chars:
            break
        tags.append(clean)
        used = next_used
        if len(tags) >= 24:
            break
    if not tags:
        return ""
    return "<nav data-oldiron-value-links='1'>" + "\n".join(tags) + "</nav>\n"


def _is_value_link_tag(tag: str) -> bool:
    value = html.unescape(str(tag or "")).lower()
    value = value.replace("%e3%81%8a%e5%95%8f%e5%90%88%e3%81%9b", "お問合せ")
    return any(
        token in value
        for token in (
            "shop-pro.jp",
            "contact",
            "inquiry",
            "mailform",
            "privacy",
            "recruit",
            "tokutei",
            "お問い合わせ",
            "お問合せ",
            "問い合わせ",
            "問合せ",
            "特定商取引",
            "通信販売",
            "個人情報",
            "プライバシー",
            "採用",
            "メール",
        )
    )


def _merge_html_signal_windows(windows: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not windows:
        return []
    ordered = sorted(windows)
    merged: list[tuple[int, int]] = []
    start, end = ordered[0]
    for next_start, next_end in ordered[1:]:
        if next_start <= end + 256:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return merged[:8]


def _extract_signal_lines(fragment: str) -> list[str]:
    text = html.unescape(re.sub(r"<[^>]+>", "\n", str(fragment or "")))
    raw_lines = []
    for raw_line in text.splitlines():
        clean = re.sub(r"\s+", " ", raw_line).strip()
        if len(clean) >= 2:
            raw_lines.append(clean)
    if not raw_lines:
        return []
    picked: list[str] = []
    for index, line in enumerate(raw_lines):
        lowered = line.lower()
        if not (EMAIL_SIGNAL_RE.search(line) or any(token in lowered for token in (
            "founder", "co-founder", "owner", "chairman", "chief executive", "managing director",
            "group chief executive", "president", "principal solicitor", "director", "lead guide",
            "leadership", "executive team",
        ))):
            continue
        start = max(index - 1, 0)
        end = min(index + 3, len(raw_lines))
        for candidate in raw_lines[start:end]:
            if candidate not in picked:
                picked.append(candidate)
    if picked:
        return picked[:24]
    fallback: list[str] = []
    for line in raw_lines:
        if line not in fallback:
            fallback.append(line)
        if len(fallback) >= 12:
            break
    return fallback


def _render_signal_summary(lines: list[str]) -> str:
    if not lines:
        return ""
    body = "".join(f"<p>{html.escape(line)}</p>" for line in lines)
    return f"<section data-oldiron-signal='1'><h2>重点正文片段</h2>{body}</section>"


def _candidate_encodings(content_type: str, content: bytes) -> list[str]:
    values: list[str] = []
    match = CHARSET_RE.search(str(content_type or ""))
    if match is not None:
        values.append(str(match.group(1) or "").strip().lower())
    head = bytes(content[:4096])
    for pattern in (HTML_META_CHARSET_RE, XML_ENCODING_RE):
        match = pattern.search(head)
        if match is None:
            continue
        values.append(match.group(1).decode("ascii", errors="ignore").strip().lower())
    for fallback in ("utf-8", "utf-8-sig", "cp932", "shift_jis", "euc_jp", "latin-1"):
        if fallback not in values:
            values.append(fallback)
    return values
