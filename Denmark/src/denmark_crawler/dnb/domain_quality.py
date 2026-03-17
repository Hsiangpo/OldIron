"""丹麦 DNB 域名质量判定。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse


DOMAIN_PATTERN = re.compile(r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
TOKEN_PATTERN = re.compile(r"[0-9a-z가-힣]+")
GENERIC_TOKENS = {
    "company", "companies", "limited", "ltd", "co", "public", "partnership",
    "construction", "engineering", "development", "property", "properties",
    "asset", "assets", "group", "civil", "consultant", "service", "services",
    "design", "power", "energy", "industrial", "supply", "contractor",
    "management", "industry", "information", "system", "systems",
}
HARD_EXCLUDED_SUFFIXES = (
    "wikipedia.org",
    "wikidata.org",
    "wikimedia.org",
    "talent.vn",
)
HARD_EXCLUDED_DOMAINS = {
    "hktdc.com",
    "jewelry.org.hk",
    "mingluji.com",
}
PLATFORM_HINTS = (
    "recruit", "career", "careers", "talent", "jobs", "job", "wiki",
)


@dataclass(slots=True)
class DomainAssessment:
    blocked: bool
    reason: str
    match_score: int
    suspicious: bool = False


def normalize_website_url(raw: str) -> str:
    value = re.sub(r"\s+", "", str(raw or "").strip())
    if not value:
        return ""
    if value.startswith("//"):
        value = f"https:{value}"
    if value.startswith("www."):
        value = f"https://{value}"
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    return value.rstrip("/")


def normalize_domain(raw: str) -> str:
    value = normalize_website_url(raw)
    if not value:
        return ""
    try:
        parsed = urlparse(value)
    except ValueError:
        return ""
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def is_valid_domain(domain: str) -> bool:
    value = normalize_domain(domain)
    return bool(value and DOMAIN_PATTERN.fullmatch(value))


def is_excluded_company_domain(domain: str) -> bool:
    value = normalize_domain(domain)
    if not is_valid_domain(value):
        return False
    if value in HARD_EXCLUDED_DOMAINS:
        return True
    for suffix in HARD_EXCLUDED_SUFFIXES:
        if value == suffix or value.endswith("." + suffix):
            return True
    return False


def _domain_label(domain: str) -> str:
    value = normalize_domain(domain)
    return value.split(".", 1)[0] if value else ""


def _company_tokens(company_name: str) -> list[str]:
    tokens = [item for item in TOKEN_PATTERN.findall(str(company_name or "").lower()) if len(item) >= 3]
    unique: list[str] = []
    for token in tokens:
        if token in GENERIC_TOKENS:
            continue
        if token not in unique:
            unique.append(token)
    return unique


def domain_match_score(company_name: str, domain: str) -> int:
    label = _domain_label(domain)
    if not label:
        return 0
    best = 0
    for token in _company_tokens(company_name):
        if token == label:
            best = max(best, 100)
            continue
        if token in label or label in token:
            best = max(best, 75)
    return best


def _has_platform_hint(domain: str) -> bool:
    label = _domain_label(domain)
    host = normalize_domain(domain)
    return any(hint in label or hint in host for hint in PLATFORM_HINTS)


def assess_company_domain(company_name: str, domain: str, *, source: str) -> DomainAssessment:
    value = normalize_domain(domain)
    if not value:
        return DomainAssessment(blocked=False, reason="", match_score=0, suspicious=False)
    if is_excluded_company_domain(value):
        return DomainAssessment(blocked=True, reason="excluded_domain", match_score=0, suspicious=True)
    score = domain_match_score(company_name, value)
    if _has_platform_hint(value):
        return DomainAssessment(blocked=True, reason="platform_like_domain", match_score=score, suspicious=True)
    if source == "gmap" and score <= 0:
        return DomainAssessment(blocked=True, reason="gmap_low_match_domain", match_score=score, suspicious=True)
    return DomainAssessment(blocked=False, reason="", match_score=score, suspicious=False)

