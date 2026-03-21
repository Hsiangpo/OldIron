"""域名质量判定。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse


DOMAIN_PATTERN = re.compile(r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
TOKEN_PATTERN = re.compile(r"[0-9a-zก-๙]+")
GENERIC_TOKENS = {
    "company",
    "companies",
    "limited",
    "ltd",
    "co",
    "public",
    "partnership",
    "construction",
    "engineering",
    "development",
    "property",
    "properties",
    "asset",
    "assets",
    "group",
    "civil",
    "consultant",
    "service",
    "services",
    "design",
    "power",
    "energy",
    "industrial",
    "supply",
    "contractor",
    "management",
}
HARD_EXCLUDED_SUFFIXES = (
    "go.th",
    "wordpress.com",
    "booking.com",
    "traveloka.com",
    "trip.com",
    "bluepillow.com",
    "laterooms.com",
    "trivago.com",
    "trivago.co.kr",
)
HARD_EXCLUDED_DOMAINS = {
    "fb.me",
    "bit.ly",
    "centarahotelsresorts.com",
    "expedia.com",
    "expedia.co.th",
    "expedia.co.kr",
    "agoda.com",
    "hotels.com",
    "zenhotels.com",
    "imperialhotels.com",
    "capellahotels.com",
    "wyndhamhotels.com",
    "dreamhotels.com",
    "ozohotels.com",
    "saiihotels.com",
    "shasahotels.com",
    "shotelsresorts.com",
}
PLATFORM_HINTS = (
    "book",
    "travel",
    "trip",
    "hotel",
    "resort",
    "villa",
    "hostel",
    "stay",
    "expedia",
    "agoda",
    "trivago",
    "room",
)


@dataclass(slots=True)
class DomainAssessment:
    blocked: bool
    reason: str
    match_score: int
    suspicious: bool = False


def normalize_domain(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
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
        if len(token) < 4:
            continue
        if token == label:
            best = max(best, 100)
            continue
        if token in label or label in token:
            best = max(best, 75)
    return best


def _has_platform_hint(domain: str) -> bool:
    label = _domain_label(domain)
    return any(hint in label for hint in PLATFORM_HINTS)


def assess_company_domain(company_name: str, domain: str, *, shared_count: int = 1) -> DomainAssessment:
    value = normalize_domain(domain)
    if not value:
        return DomainAssessment(blocked=False, reason="", match_score=0, suspicious=False)
    if is_excluded_company_domain(value):
        return DomainAssessment(blocked=True, reason="excluded_domain", match_score=0, suspicious=True)
    score = domain_match_score(company_name, value)
    if shared_count >= 3 and score <= 0 and _has_platform_hint(value):
        return DomainAssessment(blocked=True, reason="platform_like_domain", match_score=score, suspicious=True)
    if shared_count >= 10 and score <= 0:
        return DomainAssessment(blocked=False, reason="shared_unrelated_domain", match_score=score, suspicious=True)
    return DomainAssessment(blocked=False, reason="", match_score=score, suspicious=False)
