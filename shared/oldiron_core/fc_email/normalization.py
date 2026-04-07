"""邮箱标准化与可疑集合识别。"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import unquote
from urllib.parse import urlparse


_EMAIL_RE = re.compile(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", re.IGNORECASE)
_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.jp",
    "or.jp",
    "ne.jp",
    "go.jp",
    "ac.jp",
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
}
_BAD_EMAIL_TLDS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico", "avif",
    "mp4", "webm", "mov", "pdf", "js", "css", "woff", "woff2", "ttf", "eot",
}
_BAD_EMAIL_HOST_HINTS = (
    "example.com",
    "example.org",
    "example.net",
    "sample.com",
    "sample.co.jp",
    "mysite.com",
    "mysite.co.jp",
    "eksempel.dk",
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
)
_IGNORE_LOCAL_PARTS = {
    "x",
    "xx",
    "xxx",
    "test",
    "example",
    "sample",
    "yourname",
    "youremail",
    "email",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}
_EMAIL_PRIORITY_LOCAL_PARTS = {
    "contact",
    "customer",
    "hello",
    "help",
    "hr",
    "info",
    "inquiry",
    "office",
    "privacy",
    "pr",
    "press",
    "recruit",
    "recruiting",
    "sales",
    "service",
    "support",
    "saiyo",
    "soumu",
    "kojinjoho",
}


@dataclass(slots=True)
class EmailSetAnalysis:
    emails: list[str]
    same_domain_emails: list[str]
    domain_count: int
    suspicious_directory_like: bool


def extract_registrable_domain(value: str) -> str:
    """从 URL 或 host 提取注册域。"""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "://" not in text and "/" not in text:
        host = text
    else:
        if "://" not in text:
            text = f"https://{text}"
        parsed = urlparse(text)
        host = str(parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2


def normalize_email_candidate(value: object) -> str:
    """标准化单个邮箱候选值。"""
    text = unquote(str(value or "")).strip().lower()
    if not text:
        return ""
    text = text.replace("mailto:", "")
    text = re.sub(r"^(?:u003e|u003c|>|<)+", "", text)
    text = re.sub(r"(?i)\[(?:at)\]|\((?:at)\)|\s+at\s+", "@", text)
    text = re.sub(r"(?i)\[(?:dot)\]|\((?:dot)\)|\s+dot\s+", ".", text)
    match = _EMAIL_RE.search(text)
    if match is None:
        return ""
    email = str(match.group(1) or "").strip().lower().rstrip(".,);:]}>")
    if "@" not in email:
        return ""
    local, domain = email.split("@", 1)
    if local in _IGNORE_LOCAL_PARTS:
        return ""
    suffix = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if suffix in _BAD_EMAIL_TLDS:
        return ""
    if any(flag in domain for flag in _BAD_EMAIL_HOST_HINTS):
        return ""
    return email


def split_emails(values: Iterable[str] | str) -> list[str]:
    """从字符串或列表拆分并标准化邮箱。"""
    items: list[object]
    if isinstance(values, str):
        items = re.split(r"[;,]", values)
    else:
        items = list(values)
    result: list[str] = []
    for raw in items:
        email = normalize_email_candidate(raw)
        if email and email not in result:
            result.append(email)
    return result


def analyze_email_set(website: str, values: Iterable[str] | str) -> EmailSetAnalysis:
    """分析邮箱集合是否像目录页误抓。"""
    emails = _prioritize_emails(split_emails(values))
    same_domain_emails = [email for email in emails if email_matches_website(website, email)]
    domains = {extract_registrable_domain(email.split("@", 1)[1]) for email in emails if "@" in email}
    suspicious = bool(
        emails
        and not same_domain_emails
        and len(emails) >= 8
        and len(domains) >= 5
    )
    return EmailSetAnalysis(
        emails=emails,
        same_domain_emails=same_domain_emails,
        domain_count=len(domains),
        suspicious_directory_like=suspicious,
    )


def email_matches_website(website: str, email: str) -> bool:
    """判断邮箱域名是否与站点域名一致。"""
    site_domain = extract_registrable_domain(website)
    value = str(email or "").strip().lower()
    if not site_domain or "@" not in value:
        return False
    email_domain = value.split("@", 1)[1]
    return email_domain == site_domain or email_domain.endswith(f".{site_domain}")


def join_emails(values: Iterable[str] | str) -> str:
    """把邮箱列表拼成统一分隔文本。"""
    return "; ".join(split_emails(values))


def _prioritize_emails(emails: list[str]) -> list[str]:
    return sorted(emails, key=lambda item: (-_email_priority_score(item), emails.index(item)))


def _email_priority_score(email: str) -> int:
    value = str(email or "").strip().lower()
    if "@" not in value:
        return 0
    local = value.split("@", 1)[0]
    if local in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 100
    normalized = re.sub(r"[^a-z0-9]+", "", local)
    if normalized in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 90
    if any(token in normalized for token in _EMAIL_PRIORITY_LOCAL_PARTS):
        return 70
    if re.fullmatch(r"[a-z]+", normalized):
        return 20
    if re.search(r"\d", normalized):
        return 5
    return 10
