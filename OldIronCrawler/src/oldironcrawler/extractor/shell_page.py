from __future__ import annotations

import html
import hashlib
import queue
import re
import threading
import time
import unicodedata
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from oldironcrawler.extractor.email_rules import email_matches_website, extract_registrable_domain, normalize_email_candidate

_ROOT_IDS = {"root", "app", "__next", "__nuxt", "svelte"}
_ASSET_SUFFIXES = (".js", ".mjs", ".json")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_DOUBLE_QUOTED_RE = re.compile(r'"((?:\\.|[^"\\]){3,180})"')
_SINGLE_QUOTED_RE = re.compile(r"'((?:\\\\.|[^'\\\\]){3,180})'")
_ASSET_ANCHOR_RE = re.compile(
    r"mailto:|tel:|impressum|imprint|contact|gmbh|geschäft|director|founder|ceo|leadership|management|partner|owner|phone|president|telefon|telephone",
    re.IGNORECASE,
)
_PHONE_SIGNAL_RE = re.compile(r"(?:\+?\d[\d()./\-\s]{5,}\d)")
_PERSON_NAME_RE = re.compile(r"[A-Z][A-Za-z.-]+(?: [A-Z][A-Za-z.-]+){1,3}")
_ROLE_KEYWORDS = {
    "ceo",
    "chief executive",
    "contact",
    "director",
    "founder",
    "geschaftsfuhrer",
    "geschaftsfuhrender",
    "impressum",
    "imprint",
    "legal",
    "managing director",
    "owner",
    "partner",
    "president",
}
_COMPANY_KEYWORDS = {
    " inc",
    " llc",
    " ltd",
    " ag",
    " bv",
    " corp",
    " gmbh",
    " limited",
}
_HEAD_META_FIELDS = ("author", "description", "og:title", "og:description", "twitter:title", "twitter:description")
_SHELL_EMAIL_KEEP_HINTS = {
    "contact",
    "director",
    "email",
    "founder",
    "geschaftsfuhrer",
    "geschaftsfuhrender",
    "impressum",
    "kontakt",
    "legal",
    "mail",
    "mailto",
    "managing director",
    "owner",
    "partner",
    "phone",
    "president",
    "representative",
    "telefon",
}
_SHELL_EMAIL_DROP_HINTS = {
    "aufsicht",
    "authority",
    "beispiel",
    "compliance",
    "datenschutz",
    "example",
    "placeholder",
    "privacy",
    "regulator",
    "regierung",
    "terms",
}
_SHELL_ASSET_FETCH_BUDGET_SECONDS = 5.0
_SHELL_ASSET_REQUEST_TIMEOUT_SECONDS = 2.0
_SHELL_ASSET_MAX_BYTES = 512_000
_SHELL_REPLACE_BUDGET_SECONDS = 3.0
_SHELL_REPLACE_MIN_REMAINING_SECONDS = 0.005
_SHELL_PAGE_PARSE_MAX_CHARS = 250_000
_SCRIPT_SRC_RE = re.compile(r"<script\b[^>]{0,800}\bsrc\s*=", re.IGNORECASE)
_ROOT_MARKER_RE = re.compile(
    r"(?:data-reactroot|id\s*=\s*['\"]?(?:root|app|__next|__nuxt|svelte)['\"]?)",
    re.IGNORECASE,
)


def looks_like_shell_page(page_html: str) -> bool:
    raw_html = str(page_html or "")
    if not _has_script_src_signal(raw_html):
        return False
    if len(raw_html) > _SHELL_PAGE_PARSE_MAX_CHARS and not _has_root_marker_signal(raw_html):
        return False
    soup = BeautifulSoup(_shell_parse_sample(raw_html), "lxml")
    body = soup.body or soup
    text = re.sub(r"\s+", " ", body.get_text(" ", strip=True)).strip()
    if _has_root_container(soup):
        return True
    if _has_root_marker_signal(raw_html):
        return True
    if len(text) > 160:
        return False
    return False


def extract_first_party_asset_urls(page_url: str, page_html: str, *, limit: int = 6) -> list[str]:
    soup = BeautifulSoup(_shell_parse_sample(str(page_html or "")), "lxml")
    asset_base_url = _select_asset_base_url(page_url, soup)
    page_domain = extract_registrable_domain(asset_base_url)
    urls: list[str] = []
    seen: set[str] = set()
    for script in soup.find_all("script", src=True):
        src = str(script.get("src") or "").strip()
        if not src:
            continue
        resolved = urljoin(asset_base_url, src)
        if resolved in seen:
            continue
        if not _is_first_party_asset(page_domain, resolved):
            continue
        seen.add(resolved)
        urls.append(resolved)
        if len(urls) >= limit:
            break
    return urls


def _select_asset_base_url(page_url: str, soup: BeautifulSoup) -> str:
    for link in soup.find_all("link", href=True):
        rel_values = link.get("rel") or []
        if isinstance(rel_values, str):
            rel_values = [rel_values]
        if "canonical" not in {str(value).strip().lower() for value in rel_values}:
            continue
        href = str(link.get("href") or "").strip()
        resolved = urljoin(page_url, href)
        parsed = urlparse(resolved)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return resolved
    return page_url


def build_shell_fingerprint(page_url: str, page_html: str) -> str:
    if not looks_like_shell_page(page_html):
        return ""
    asset_urls = extract_first_party_asset_urls(page_url, page_html, limit=12)
    fingerprint_parts = [_normalize_fingerprint_asset_url(asset_url) for asset_url in asset_urls]
    fingerprint_parts.extend(_collect_head_lines(page_html)[:3])
    if not any(fingerprint_parts):
        return ""
    payload = "\n".join(part for part in fingerprint_parts if part)
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def fetch_first_party_asset_texts(
    asset_urls: list[str],
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None = None,
) -> dict[str, str]:
    if not asset_urls:
        return {}
    effective_deadline = _resolve_shell_asset_deadline(deadline_monotonic)
    client_kwargs: dict[str, object] = {
        "follow_redirects": True,
        "timeout": _build_shell_asset_timeout(timeout_seconds),
        "trust_env": False,
        "verify": False,
        "headers": {"User-Agent": "Mozilla/5.0", "Accept": "*/*"},
    }
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    result: dict[str, str] = {}
    for asset_url in asset_urls:
        request_timeout = _resolve_deadline_timeout(
            timeout_seconds=min(max(float(timeout_seconds or 0), 1.0), _SHELL_ASSET_REQUEST_TIMEOUT_SECONDS),
            deadline_monotonic=effective_deadline,
        )
        if request_timeout is None:
            break
        text, timed_out = _fetch_text_asset_with_timeout(
            client_kwargs,
            asset_url,
            timeout_seconds=request_timeout,
            max_bytes=_SHELL_ASSET_MAX_BYTES,
            deadline_monotonic=effective_deadline,
        )
        if text:
            result[asset_url] = text
        if timed_out:
            break
    return result


def _build_shell_asset_timeout(timeout_seconds: float) -> httpx.Timeout:
    base_timeout = min(max(float(timeout_seconds or 0), 1.0), _SHELL_ASSET_REQUEST_TIMEOUT_SECONDS)
    return httpx.Timeout(connect=base_timeout, read=base_timeout, write=base_timeout, pool=1.0)


def _resolve_shell_asset_deadline(deadline_monotonic: float | None) -> float:
    budget_deadline = time.monotonic() + _SHELL_ASSET_FETCH_BUDGET_SECONDS
    if deadline_monotonic is None:
        return budget_deadline
    return min(deadline_monotonic, budget_deadline)


def _fetch_text_asset_stream(client: httpx.Client, asset_url: str, *, timeout_seconds: float, max_bytes: int) -> str:
    chunks: list[bytes] = []
    read_size = 0
    with client.stream("GET", asset_url, timeout=timeout_seconds) as response:
        if response.status_code != 200:
            return ""
        if not _looks_like_text_asset(asset_url, str(response.headers.get("Content-Type", "") or "")):
            return ""
        for chunk in response.iter_bytes():
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8", errors="ignore")
            if not chunk:
                continue
            chunks.append(bytes(chunk))
            read_size += len(chunk)
            if read_size >= max_bytes:
                break
    if not chunks:
        return ""
    return b"".join(chunks)[:max_bytes].decode("utf-8", errors="replace").strip()


def _fetch_text_asset_with_timeout(
    client_kwargs: dict[str, object],
    asset_url: str,
    *,
    timeout_seconds: float,
    max_bytes: int,
    deadline_monotonic: float,
) -> tuple[str, bool]:
    remaining = _resolve_deadline_timeout(timeout_seconds=timeout_seconds, deadline_monotonic=deadline_monotonic)
    if remaining is None:
        return "", True
    result_queue: queue.Queue[str] = queue.Queue(maxsize=1)

    def worker() -> None:
        try:
            with httpx.Client(**client_kwargs) as client:
                text = _fetch_text_asset_stream(
                    client,
                    asset_url,
                    timeout_seconds=timeout_seconds,
                    max_bytes=max_bytes,
                )
        except Exception:  # noqa: BLE001
            text = ""
        try:
            result_queue.put_nowait(text)
        except queue.Full:
            return

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=max(remaining, 0.01))
    if thread.is_alive():
        return "", True
    try:
        return result_queue.get_nowait(), False
    except queue.Empty:
        return "", False


def build_shell_evidence_html(page_url: str, page_html: str, asset_texts: dict[str, str]) -> str:
    evidence_lines = _collect_head_lines(page_html)
    for asset_url, asset_text in asset_texts.items():
        signals = _extract_asset_signal_lines(asset_text, page_url=page_url)
        if not signals:
            continue
        evidence_lines.append(f"Source Asset: {asset_url}")
        evidence_lines.extend(signals)
    deduped = _dedupe_lines(evidence_lines)
    if not deduped:
        return str(page_html or "")
    parts = [
        "<html><body><section data-oldiron-shell='1'>",
        f"<h1>Recovered shell evidence for {html.escape(page_url)}</h1>",
    ]
    for line in deduped[:180]:
        parts.append(f"<p>{html.escape(line)}</p>")
    parts.append("</section></body></html>")
    return "".join(parts)


def enrich_shell_page_html(
    page_url: str,
    page_html: str,
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None = None,
) -> str:
    if not looks_like_shell_page(page_html):
        return str(page_html or "")
    asset_urls = extract_first_party_asset_urls(page_url, page_html)
    asset_texts = fetch_first_party_asset_texts(
        asset_urls,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        deadline_monotonic=deadline_monotonic,
    )
    return build_shell_evidence_html(page_url, page_html, asset_texts)


def replace_shell_pages_with_evidence(
    page_map: dict[str, object],
    target_urls: list[str],
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None,
) -> int:
    started = time.monotonic()
    shell_deadline = _resolve_shell_replace_deadline(deadline_monotonic)
    attempted_count = 0
    for url in target_urls:
        remaining = shell_deadline - time.monotonic()
        if remaining <= 0:
            break
        remaining_floor = max(_SHELL_REPLACE_MIN_REMAINING_SECONDS, _SHELL_REPLACE_MIN_REMAINING_SECONDS * 2)
        if attempted_count > 0 and _SHELL_REPLACE_BUDGET_SECONDS <= remaining_floor:
            break
        if attempted_count > 0 and remaining <= remaining_floor:
            break
        page = page_map.get(url)
        if page is None or not looks_like_shell_page(getattr(page, "html", "")):
            continue
        attempted_count += 1
        enriched_html = _enrich_shell_page_html_with_deadline(
            page.url,
            page.html,
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            deadline_monotonic=shell_deadline,
        )
        if enriched_html.strip() and enriched_html != page.html:
            page_map[url] = type(page)(url=page.url, html=_merge_shell_evidence(page.html, enriched_html))
    return int(round((time.monotonic() - started) * 1000))


def _enrich_shell_page_html_with_deadline(
    page_url: str,
    page_html: str,
    *,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float,
) -> str:
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        return str(page_html or "")
    result_queue: queue.Queue[str] = queue.Queue(maxsize=1)

    def worker() -> None:
        try:
            enriched_html = enrich_shell_page_html(
                page_url,
                page_html,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                deadline_monotonic=deadline_monotonic,
            )
        except Exception:  # noqa: BLE001
            enriched_html = str(page_html or "")
        try:
            result_queue.put_nowait(str(enriched_html or ""))
        except queue.Full:
            return

    # 外层预算必须兜住底层网络库或解析器偶发卡住的情况。
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=max(remaining, _SHELL_REPLACE_MIN_REMAINING_SECONDS))
    if thread.is_alive():
        return str(page_html or "")
    try:
        return result_queue.get_nowait()
    except queue.Empty:
        return str(page_html or "")


def _resolve_shell_replace_deadline(deadline_monotonic: float | None) -> float:
    budget_deadline = time.monotonic() + _SHELL_REPLACE_BUDGET_SECONDS
    if deadline_monotonic is None:
        return budget_deadline
    return min(deadline_monotonic, budget_deadline)


def _merge_shell_evidence(original_html: str, enriched_html: str) -> str:
    original = str(original_html or "")
    enriched = str(enriched_html or "")
    if not original.strip():
        return enriched
    if not enriched.strip() or enriched == original:
        return original
    return f"{original}\n{enriched}"


def build_shell_alias_map(
    *,
    start_url: str,
    page_map: dict[str, object],
    target_urls: list[str],
) -> dict[str, str]:
    ordered_urls = _merge_unique_urls([start_url], target_urls, limit=max(len(target_urls) + 1, 1))
    fingerprint_to_urls: dict[str, list[str]] = {}
    homepage_fingerprint = _page_shell_fingerprint(page_map.get(start_url))
    for url in ordered_urls:
        fingerprint = _page_shell_fingerprint(page_map.get(url))
        if not fingerprint:
            continue
        fingerprint_to_urls.setdefault(fingerprint, []).append(url)
    alias_map: dict[str, str] = {}
    for fingerprint, urls in fingerprint_to_urls.items():
        if len(urls) <= 1:
            continue
        canonical_url = _pick_shell_canonical_url(
            start_url=start_url,
            homepage_fingerprint=homepage_fingerprint,
            fingerprint=fingerprint,
            urls=urls,
        )
        for url in urls:
            alias_map[url] = canonical_url
    return alias_map


def canonicalize_shell_target_urls(urls: list[str], alias_map: dict[str, str]) -> list[str]:
    if not alias_map:
        return [str(url or "").strip() for url in urls if str(url or "").strip()]
    result: list[str] = []
    seen: set[str] = set()
    for raw_url in urls:
        url = str(raw_url or "").strip()
        if not url:
            continue
        canonical = str(alias_map.get(url, url) or "").strip()
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        result.append(canonical)
    return result


def _resolve_deadline_timeout(*, timeout_seconds: float, deadline_monotonic: float | None) -> float | None:
    base_timeout = max(float(timeout_seconds or 0), 1.0)
    if deadline_monotonic is None:
        return base_timeout
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        return None
    return max(min(base_timeout, remaining), 0.01)


def _has_root_container(soup: BeautifulSoup) -> bool:
    body = soup.body or soup
    for element in body.find_all(True, recursive=False):
        if str(element.get("id") or "").strip().lower() in _ROOT_IDS:
            return True
    return False


def _has_script_src_signal(page_html: str) -> bool:
    return bool(_SCRIPT_SRC_RE.search(str(page_html or "")))


def _has_root_marker_signal(page_html: str) -> bool:
    return bool(_ROOT_MARKER_RE.search(str(page_html or "")))


def _shell_parse_sample(page_html: str) -> str:
    raw_html = str(page_html or "")
    if len(raw_html) <= _SHELL_PAGE_PARSE_MAX_CHARS:
        return raw_html
    half = max(_SHELL_PAGE_PARSE_MAX_CHARS // 2, 1)
    return raw_html[:half] + raw_html[-half:]


def _is_first_party_asset(page_domain: str, asset_url: str) -> bool:
    if not page_domain:
        return False
    parsed = urlparse(asset_url)
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        return False
    asset_domain = extract_registrable_domain(host)
    if asset_domain != page_domain:
        return False
    lowered_path = str(parsed.path or "").strip().lower()
    return lowered_path.endswith(_ASSET_SUFFIXES) or "/assets/" in lowered_path


def _looks_like_text_asset(asset_url: str, content_type: str) -> bool:
    lowered = str(content_type or "").lower()
    if any(token in lowered for token in ("javascript", "json", "text/plain", "ecmascript")):
        return True
    return str(urlparse(asset_url).path or "").lower().endswith(_ASSET_SUFFIXES)


def _collect_head_lines(page_html: str) -> list[str]:
    soup = BeautifulSoup(_shell_parse_sample(str(page_html or "")), "lxml")
    lines: list[str] = []
    if soup.title and soup.title.string:
        title = re.sub(r"\s+", " ", str(soup.title.string or "")).strip()
        if title:
            lines.append(title)
    for meta in soup.find_all("meta"):
        key = str(meta.get("property") or meta.get("name") or "").strip().lower()
        if key not in _HEAD_META_FIELDS:
            continue
        content = re.sub(r"\s+", " ", str(meta.get("content") or "")).strip()
        if content:
            lines.append(content)
    return _dedupe_lines(lines)


def _extract_asset_signal_lines(asset_text: str, *, page_url: str) -> list[str]:
    scoped_text = _slice_asset_signal_windows(str(asset_text or ""))
    general_candidates: list[tuple[int, str]] = []
    person_candidates: list[tuple[int, str]] = []
    role_candidates: list[tuple[int, str]] = []
    company_candidates: list[tuple[int, str]] = []
    email_candidates: list[tuple[int, str]] = []
    for match in _EMAIL_RE.finditer(scoped_text):
        email = normalize_email_candidate(match.group(0))
        if not email:
            continue
        context = _extract_email_context_fragment(scoped_text, match.start(), match.end())
        if not _should_keep_shell_email(page_url, email, context):
            continue
        score = 90 if email_matches_website(page_url, email) else 48
        email_candidates.append((score, email.lower()))
    for name in _PERSON_NAME_RE.findall(scoped_text):
        if _looks_like_person_name(name):
            person_candidates.append((78, name))
    for fragment in _extract_context_fragments(scoped_text):
        cleaned_fragment = _sanitize_signal_line(page_url, fragment)
        score = _signal_score(cleaned_fragment)
        if score > 0:
            _bucket_signal_line(
                cleaned_fragment,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    for raw_value in _DOUBLE_QUOTED_RE.findall(scoped_text):
        decoded = _sanitize_signal_line(page_url, _decode_js_string(raw_value))
        score = _signal_score(decoded)
        if score > 0:
            _bucket_signal_line(
                decoded,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    for raw_value in _SINGLE_QUOTED_RE.findall(scoped_text):
        decoded = _sanitize_signal_line(page_url, _decode_js_string(raw_value))
        score = _signal_score(decoded)
        if score > 0:
            _bucket_signal_line(
                decoded,
                score,
                general_candidates,
                person_candidates,
                role_candidates,
                company_candidates,
            )
    ordered = (
        sorted(email_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(person_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(role_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:24]
        + sorted(company_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:20]
        + sorted(general_candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[:120]
    )
    result: list[str] = []
    seen: set[str] = set()
    for _score, line in ordered:
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
        if len(result) >= 120:
            break
    return result


def _bucket_signal_line(
    line: str,
    score: int,
    general_candidates: list[tuple[int, str]],
    person_candidates: list[tuple[int, str]],
    role_candidates: list[tuple[int, str]],
    company_candidates: list[tuple[int, str]],
) -> None:
    general_candidates.append((score, line))
    lowered = _normalize_signal_text(line)
    if _looks_like_person_name(line):
        person_candidates.append((score + 20, line))
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        role_candidates.append((score + 12, line))
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        company_candidates.append((score + 10, line))


def _decode_js_string(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\\/", "/")
    text = text.encode("utf-8", errors="ignore").decode("unicode_escape", errors="ignore")
    text = re.sub(r"\s+", " ", text).strip().strip(".,;")
    return text


def _extract_context_fragments(scoped_text: str) -> list[str]:
    fragments: list[str] = []
    for match in _ASSET_ANCHOR_RE.finditer(scoped_text):
        fragment = scoped_text[max(match.start() - 120, 0) : min(match.end() + 160, len(scoped_text))]
        fragment = re.sub(r"[{}\[\](),:+*/]", " ", fragment)
        fragment = re.sub(r"\s+", " ", fragment).strip().strip(".,;")
        if len(fragment) < 12 or len(fragment) > 180:
            continue
        fragments.append(fragment)
        if len(fragments) >= 40:
            break
    return fragments


def _slice_asset_signal_windows(asset_text: str) -> str:
    text = str(asset_text or "")
    windows: list[tuple[int, int]] = []
    for match in _EMAIL_RE.finditer(text):
        windows.append((max(match.start() - 600, 0), min(match.end() + 600, len(text))))
    for match in _ASSET_ANCHOR_RE.finditer(text):
        windows.append((max(match.start() - 600, 0), min(match.end() + 600, len(text))))
        if len(windows) >= 80:
            break
    if not windows:
        return text[:200_000]
    merged: list[tuple[int, int]] = []
    start, end = sorted(windows)[0]
    for next_start, next_end in sorted(windows)[1:]:
        if next_start <= end + 200:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return "\n".join(text[left:right] for left, right in merged[:24])


def _signal_score(value: str) -> int:
    text = str(value or "").strip()
    if len(text) < 3 or len(text) > 180:
        return 0
    if text.startswith(("http://", "https://")):
        return 0
    if text.count("{") or text.count("function(") or text.count("=>"):
        return 0
    lowered = _normalize_signal_text(text)
    score = 0
    if _EMAIL_RE.search(text):
        score += 90
    if _PHONE_SIGNAL_RE.search(text):
        score += 36
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        score += 48
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        score += 38
    if _looks_like_person_name(text):
        score += 58
    if "mailto:" in lowered:
        score += 12
    if re.search(r"[A-Za-z].*[A-Za-z]", text) and " " in text:
        score += 4
    return score


def _looks_like_person_name(value: str) -> bool:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text or len(text) > 80:
        return False
    return bool(re.fullmatch(r"[A-Z][A-Za-z.-]+(?: [A-Z][A-Za-z.-]+){1,3}", text))


def _normalize_signal_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text).strip().lower()


def _dedupe_lines(lines: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = re.sub(r"\s+", " ", str(raw_line or "")).strip()
        if not line:
            continue
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
    return result


def _normalize_fingerprint_asset_url(asset_url: str) -> str:
    parsed = urlparse(asset_url)
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not host and not path:
        return ""
    return f"{host}{path}"


def _extract_email_context_fragment(text: str, start: int, end: int) -> str:
    left_boundaries = [
        text.rfind("\n", 0, start),
        text.rfind('"', 0, start),
        text.rfind("'", 0, start),
    ]
    right_boundaries = [
        index
        for index in (
            text.find("\n", end),
            text.find('"', end),
            text.find("'", end),
        )
        if index != -1
    ]
    left = max(max(left_boundaries), start - 120)
    right = min(right_boundaries) if right_boundaries else min(end + 120, len(text))
    return text[max(left, 0) : min(right, len(text))]


def _sanitize_signal_line(page_url: str, line: str) -> str:
    cleaned = str(line or "")
    for match in list(_EMAIL_RE.finditer(cleaned)):
        email = normalize_email_candidate(match.group(0))
        if not email:
            continue
        if _should_keep_shell_email(page_url, email, cleaned):
            continue
        cleaned = cleaned.replace(match.group(0), " ")
    return re.sub(r"\s+", " ", cleaned).strip(" ,;")


def _should_keep_shell_email(page_url: str, email: str, context: str) -> bool:
    if email_matches_website(page_url, email):
        return True
    lowered = _normalize_signal_text(context)
    registrable_domain = extract_registrable_domain(email.split("@", 1)[1])
    if not registrable_domain:
        return False
    if any(token in registrable_domain for token in _SHELL_EMAIL_DROP_HINTS):
        return False
    if any(token in lowered for token in _SHELL_EMAIL_DROP_HINTS):
        return False
    if not any(token in lowered for token in _SHELL_EMAIL_KEEP_HINTS):
        return False
    if "mailto" in lowered:
        return True
    if _looks_like_person_name(context):
        return True
    if any(keyword in lowered for keyword in _ROLE_KEYWORDS):
        return True
    if any(keyword in f" {lowered} " for keyword in _COMPANY_KEYWORDS):
        return True
    return False


def _page_shell_fingerprint(page: object | None) -> str:
    if page is None:
        return ""
    url = str(getattr(page, "url", "") or "").strip()
    html_text = str(getattr(page, "html", "") or "")
    if not url or not html_text:
        return ""
    return build_shell_fingerprint(url, html_text)


def _pick_shell_canonical_url(
    *,
    start_url: str,
    homepage_fingerprint: str,
    fingerprint: str,
    urls: list[str],
) -> str:
    if homepage_fingerprint and fingerprint == homepage_fingerprint and start_url in urls:
        return start_url
    return sorted(urls, key=_shell_canonical_sort_key)[0]


def _shell_canonical_sort_key(url: str) -> tuple[int, int, int, str]:
    lowered = str(url or "").strip().lower()
    path = lowered.rstrip("/").split("://", 1)[-1].split("/", 1)[-1]
    score = 0
    for token, value in (
        ("impressum", 120),
        ("imprint", 118),
        ("legal-notice", 110),
        ("legal", 100),
        ("contact-us", 94),
        ("contact", 90),
        ("about-us", 82),
        ("about", 76),
        ("company", 70),
        ("our-team", 58),
        ("team", 52),
        ("people", 48),
        ("executive-team", 18),
        ("leadership", 12),
        ("management", 8),
    ):
        if token in lowered:
            score += value
    depth = path.count("/") if path else 0
    return (-score, depth, len(lowered), lowered)


def _merge_unique_urls(left: list[str], right: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*left, *right]:
        value = str(url or "").strip()
        canonical = _canonical_target_url(value)
        if not value or canonical in seen:
            continue
        seen.add(canonical)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _canonical_target_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return str(url or "").strip().lower().rstrip("/")
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").rstrip("/")
    query = str(parsed.query or "").strip()
    if path == "/":
        path = ""
    if query:
        return f"{parsed.scheme.lower()}://{host}{path}?{query}"
    return f"{parsed.scheme.lower()}://{host}{path}"
