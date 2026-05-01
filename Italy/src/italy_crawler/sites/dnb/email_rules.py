"""Italy DNB 官网邮箱选页规则。"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import unquote
from urllib.parse import urlparse


_EMAIL_WEIGHTS = {
    "contact": 20,
    "kontakt": 22,
    "driver": 10,
    "drive": 8,
    "datenschutz": 12,
    "impressum": 10,
    "imprint": 10,
    "jobs": 8,
    "careers": 8,
    "career": 8,
    "recruit": 8,
    "join": 6,
    "support": 14,
    "help": 10,
    "customer": 12,
    "service": 10,
    "privacy": 8,
    "legal": 8,
    "terms": 8,
    "office": 10,
    "location": 8,
    "about": 5,
}
_NEGATIVE_TOKENS = {
    "blog", "discussion", "discussions", "event", "forum", "forums",
    "news", "post", "press", "release", "sponsored", "tag",
    "author", "category",
}
_COMPOSITE_TOKEN_MAP = {
    "aboutus": ["about", "us"],
    "contactus": ["contact", "us"],
    "ourteam": ["our", "team"],
    "ourpeople": ["our", "people"],
    "privacypolicy": ["privacy", "policy"],
}
_EMAIL_STRONG_SCORE = 12
_EMAIL_STOP_SCORE = 8
_EMAIL_FAMILY_TARGET = 6


@dataclass(slots=True)
class EmailUrlCandidate:
    url: str
    tokens: list[str]
    family_key: str
    discovery_order: int
    depth: int
    score: int


def build_email_candidates(start_url: str, discovered_urls: list[str]) -> list[EmailUrlCandidate]:
    urls: list[str] = []
    for url in [start_url, *discovered_urls]:
        value = str(url or "").strip()
        if value and value not in urls:
            urls.append(value)
    candidates: list[EmailUrlCandidate] = []
    for discovery_order, url in enumerate(urls):
        tokens = extract_path_tokens(url)
        depth = max((urlparse(url).path or "").count("/"), 0)
        score = _score_tokens(tokens)
        if url == start_url:
            score += 20
        candidates.append(
            EmailUrlCandidate(
                url=url,
                tokens=tokens,
                family_key=_family_key(tokens),
                discovery_order=discovery_order,
                depth=depth,
                score=score - min(depth, 6),
            )
        )
    return candidates


def select_email_urls(candidates: list[EmailUrlCandidate]) -> list[str]:
    ordered = sorted(candidates, key=lambda item: (-item.score, item.depth, item.url))
    urls: list[str] = []
    family_counts: dict[str, int] = {}
    strong_families: set[str] = set()
    for candidate in ordered:
        score = candidate.score
        if score <= 0:
            if urls:
                break
            continue
        family_limit = 2 if _is_strong_email_candidate(candidate) else 1
        if family_counts.get(candidate.family_key, 0) >= family_limit:
            continue
        if _should_stop_email_selection(score, strong_families):
            break
        urls.append(candidate.url)
        family_counts[candidate.family_key] = family_counts.get(candidate.family_key, 0) + 1
        if _is_strong_email_candidate(candidate):
            strong_families.add(candidate.family_key)
    return urls


def build_email_fetch_plan(
    start_url: str,
    email_urls: list[str],
    *,
    email_soft_limit: int,
    email_hard_limit: int,
    total_hard_limit: int,
) -> dict[str, list[str]]:
    homepage_primary_urls = [start_url] if start_url else []
    email_candidates = _exclude_existing_urls(
        _take_unique_urls(email_urls, limit=max(len(email_urls), 0), priority_url=start_url),
        homepage_primary_urls,
    )
    email_total_cap = max(min(email_hard_limit, total_hard_limit - len(homepage_primary_urls)), 0)
    email_primary_cap = max(min(email_soft_limit, email_total_cap), 0)
    email_primary_urls = email_candidates[:email_primary_cap]
    email_overflow_urls = email_candidates[email_primary_cap:email_total_cap]
    return {
        "homepage_primary_urls": homepage_primary_urls,
        "email_primary_urls": email_primary_urls,
        "email_overflow_urls": email_overflow_urls,
        "all_primary_urls": [*homepage_primary_urls, *email_primary_urls],
    }


def extract_path_tokens(url: str) -> list[str]:
    parsed = urlparse(str(url or ""))
    parts = [segment for segment in parsed.path.split("/") if segment]
    tokens: list[str] = []
    for part in parts:
        decoded = re.sub(r"\.[a-z0-9]{2,5}$", "", unquote(part).strip().lower())
        for token in re.split(r"[\W_]+", decoded, flags=re.UNICODE):
            for clean in _expand_composite_token(token):
                if len(clean) < 3 or clean in tokens:
                    continue
                tokens.append(clean)
    return tokens


def _expand_composite_token(token: str) -> list[str]:
    clean = str(token or "").strip().lower()
    if not clean:
        return []
    expanded = _COMPOSITE_TOKEN_MAP.get(clean)
    if expanded:
        return expanded
    ascii_variant = unicodedata.normalize("NFKD", clean).encode("ascii", "ignore").decode("ascii")
    if ascii_variant and ascii_variant != clean:
        return [clean, ascii_variant]
    return [clean]


def _family_key(tokens: list[str]) -> str:
    if not tokens:
        return "root"
    return "/".join(tokens[:2])


def _score_tokens(tokens: list[str]) -> int:
    score = 0
    for token in tokens:
        if token in _NEGATIVE_TOKENS:
            score -= 6
        score += int(_EMAIL_WEIGHTS.get(token, 0))
    return score


def _is_strong_email_candidate(candidate: EmailUrlCandidate) -> bool:
    return candidate.score >= _EMAIL_STRONG_SCORE


def _should_stop_email_selection(score: int, strong_families: set[str]) -> bool:
    if len(strong_families) < _EMAIL_FAMILY_TARGET:
        return False
    return score < _EMAIL_STOP_SCORE


def _take_unique_urls(urls: list[str], *, limit: int, priority_url: str) -> list[str]:
    ordered_urls = _promote_priority_url(urls, priority_url)
    result: list[str] = []
    seen: set[str] = set()
    for url in ordered_urls:
        value = str(url or "").strip()
        canonical = _canonical_target_url(value)
        if not value or canonical in seen:
            continue
        seen.add(canonical)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _exclude_existing_urls(urls: list[str], existing_urls: list[str]) -> list[str]:
    blocked = {_canonical_target_url(url) for url in existing_urls if str(url or "").strip()}
    return [url for url in urls if _canonical_target_url(url) not in blocked]


def _promote_priority_url(urls: list[str], priority_url: str) -> list[str]:
    priority = str(priority_url or "").strip()
    ordered_urls = [str(url or "").strip() for url in urls if str(url or "").strip()]
    if not priority:
        return ordered_urls
    priority_key = _canonical_target_url(priority)
    matched = [url for url in ordered_urls if _canonical_target_url(url) == priority_key]
    if not matched:
        return ordered_urls
    first = matched[0]
    return [first, *[url for url in ordered_urls if _canonical_target_url(url) != priority_key]]


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
