"""邮箱标准化与可疑集合识别。"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import unquote
from urllib.parse import urlparse


_EMAIL_RE = re.compile(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", re.IGNORECASE)
_INVISIBLE_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad\u2060]")
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
    "mockconsole.prototype",
    "prototype.render",
    "prototype.read",
    "rendertostring",
    "template.com",
    "template.net",
    "template.org",
    "template.ae",
    "template.co.jp",
    "group.calendar.google.com",
    "example.jp",
    "example.co.jp",
    "example.com",
    "example.org",
    "example.net",
    "gmaii.com",
    "gmai.com",
    "gmail.jp",
    "48g9-.bybgnptut",
    "sample.com",
    "sample.co.jp",
    "site.com.br",
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
    "name",
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
_PLACEHOLDER_EXACT_PARTS = {
    "aaa",
    "aaaa",
    "aaaaa",
    "dummy",
    "example",
    "hogehoge",
    "hoge",
    "name",
    "sample",
    "test",
    "xxx",
    "xxxx",
    "xxxxx",
    "xxxxxx",
    "yourdomain",
    "yourdmain",
}
_PLACEHOLDER_DOMAIN_WORDS = {
    "aaa",
    "dummy",
    "email",
    "example",
    "sample",
    "test",
    "yourdomain",
    "yourdmain",
}
_PLACEHOLDER_STEM_WORDS = {
    "dummy",
    "email",
    "example",
    "name",
    "sample",
    "test",
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
_FREE_MAIL_DOMAINS = {
    "aol.com",
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "mac.com",
    "me.com",
    "msn.com",
    "outlook.com",
    "pm.me",
    "proton.me",
    "protonmail.com",
    "yahoo.co.jp",
    "yahoo.com",
    "yahoo.com.br",
}
_OFFSITE_ALWAYS_DROP_LOCAL_PARTS = {
    "found",
    "posted",
    "profile",
    "webmaster",
    "website",
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
    text = _INVISIBLE_RE.sub("", unquote(str(value or ""))).strip().lower()
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
    if _email_appears_inside_url_token(text, email):
        return ""
    if "@" not in email:
        return ""
    local, domain = email.split("@", 1)
    if local.startswith(".") or local.endswith(".") or ".." in local:
        return ""
    if domain.startswith(".") or domain.endswith(".") or ".." in domain:
        return ""
    if local in _IGNORE_LOCAL_PARTS:
        return ""
    suffix = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if suffix in _BAD_EMAIL_TLDS:
        return ""
    if any(flag in domain for flag in _BAD_EMAIL_HOST_HINTS):
        return ""
    return email


def is_real_email_candidate(value: object) -> bool:
    """判断邮箱候选值是否像真实邮箱。"""
    email = normalize_email_candidate(value)
    return bool(email and not _is_placeholder_email(email))


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
        if email and not _is_placeholder_email(email) and email not in result:
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


def filter_emails_for_website(website: str, values: Iterable[str] | str) -> list[str]:
    """结合站点域名过滤明显不属于该公司的脏邮箱。"""
    emails = split_emails(values)
    if not emails:
        return []
    site_domain = extract_registrable_domain(website)
    if not site_domain:
        return _prioritize_emails(emails)

    same_domain_emails = [email for email in emails if email_matches_website(website, email)]
    filtered: list[str] = list(same_domain_emails)
    has_same_domain_email = bool(same_domain_emails)

    for email in emails:
        if email in filtered:
            continue
        if _should_keep_offsite_email(email, has_same_domain_email):
            filtered.append(email)
    return _prioritize_emails(filtered)


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


def _email_appears_inside_url_token(text: str, email: str) -> bool:
    for token in re.split(r"\s+", str(text or "").strip()):
        if "://" not in token:
            continue
        if email in token:
            return True
    return False


def _is_placeholder_email(email: str) -> bool:
    value = str(email or "").strip().lower()
    if "@" not in value:
        return True
    local, domain = value.split("@", 1)
    return _local_part_is_placeholder(local) or _domain_is_placeholder(domain)


def _local_part_is_placeholder(local: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", str(local or "").strip().lower())
    if not normalized:
        return True
    if normalized in _PLACEHOLDER_EXACT_PARTS:
        return True
    if _matches_placeholder_stem(normalized):
        return True
    if re.fullmatch(r"x{4,}", normalized):
        return True
    if re.fullmatch(r"a{3,}", normalized):
        return True
    if re.fullmatch(r"0{3,}", normalized):
        return True
    if re.search(r"x{4,}|0{4,}", normalized):
        return True
    parts = [part for part in re.split(r"[._%+\-]+", str(local or "").strip().lower()) if part]
    for part in parts:
        if part in _PLACEHOLDER_EXACT_PARTS:
            return True
        if _matches_placeholder_stem(re.sub(r"[^a-z0-9]+", "", part)):
            return True
        if re.fullmatch(r"x{2,}", part):
            return True
        if re.fullmatch(r"a{3,}", part):
            return True
    return False


def _domain_is_placeholder(domain: str) -> bool:
    labels = [label for label in str(domain or "").strip().lower().split(".") if label]
    if len(labels) < 2:
        return True
    if ".".join(labels[-2:]) in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) < 3:
        return True
    core_labels = _core_domain_labels(labels)
    if not core_labels:
        return True
    for label in core_labels:
        normalized = re.sub(r"[^a-z0-9]+", "", label)
        if not normalized:
            return True
        if normalized in _PLACEHOLDER_DOMAIN_WORDS:
            return True
        if any(
            normalized.startswith(word) or normalized.endswith(word)
            for word in _PLACEHOLDER_DOMAIN_WORDS
        ):
            return True
        if re.search(r"x{4,}|0{4,}", normalized):
            return True
        if re.fullmatch(r"x{2,}", normalized):
            return True
        if re.fullmatch(r"a{3,}", normalized):
            return True
        if re.fullmatch(r"0{2,}", normalized):
            return True
    return False


def _core_domain_labels(labels: list[str]) -> list[str]:
    if len(labels) < 2:
        return []
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES:
        return labels[:-2]
    return labels[:-1]


def _matches_placeholder_stem(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    for stem in _PLACEHOLDER_STEM_WORDS:
        if normalized == stem:
            return True
        if len(normalized) <= len(stem) + 8 and (normalized.startswith(stem) or normalized.endswith(stem)):
            return True
    return False


def _should_keep_offsite_email(email: str, has_same_domain_email: bool) -> bool:
    value = str(email or "").strip().lower()
    if "@" not in value:
        return False
    local, domain = value.split("@", 1)
    registrable_domain = extract_registrable_domain(domain)
    if not registrable_domain:
        return False
    if registrable_domain in _FREE_MAIL_DOMAINS:
        return True
    local_key = _normalize_local_part_key(local)
    if local_key in _OFFSITE_ALWAYS_DROP_LOCAL_PARTS:
        return False
    return True


def _normalize_local_part_key(local: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(local or "").strip().lower())


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
