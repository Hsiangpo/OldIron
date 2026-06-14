from __future__ import annotations

import html
import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import unquote
from urllib.parse import urlparse


_EMAIL_RE = re.compile(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", re.IGNORECASE)
_CFEMAIL_RE = re.compile(r"data-cfemail=[\"']([0-9a-fA-F]+)[\"']", re.IGNORECASE)
_INVISIBLE_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad\u2060]")
_SCRIPT_BLOCK_RE = re.compile(r"(?is)<(script|style|template)\b[^>]*>.*?</\1>")
_OBFUSCATED_EMAIL_HINT_RE = re.compile(
    r"(?is)[a-z0-9._%+\-]{2,}\s*(?:\[at\]|\(at\)|\bat\b|arroba)\s*"
    r"[a-z0-9.\-\s\[\]\(\)]{2,}(?:\[dot\]|\(dot\)|\bdot\b|\.)\s*[a-z]{2,}"
)
_EMAIL_LABEL_DOMAIN_HINT_RE = re.compile(
    r"(?is)\b(?:e-?mail|mail|e[-\s]?posta|eposta|correo)\b.{0,120}"
    r"\b[a-z0-9][a-z0-9.\-]*\.[a-z]{2,}\b"
)
_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "ac.jp", "co.jp", "go.jp", "ne.jp", "or.jp",
    "ac.uk", "co.uk", "gov.uk", "org.uk",
    "com.au", "net.au", "org.au",
    "com.br", "net.br", "org.br",
    "co.nz", "org.nz",
    "com.mx", "org.mx",
}
_BAD_EMAIL_TLDS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico", "avif",
    "mp4", "webm", "mov", "pdf", "js", "css", "woff", "woff2", "ttf", "eot",
}
_BAD_EMAIL_HOST_HINTS = (
    "mockconsole.prototype",
    "prototype.render",
    "prototype.read",
    "template.com",
    "template.net",
    "template.org",
    "example.com",
    "example.org",
    "example.net",
    "sample.com",
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
    "xyz.com",
    "godaddy.com",
    "website.com",
    ".pb.hx",
)
_IGNORE_LOCAL_PARTS = {
    "x", "xx", "xxx", "name", "test", "example", "exemplo", "sample", "yourname",
    "youremail", "email", "noreply", "no-reply", "donotreply", "do-not-reply",
}
_PLACEHOLDER_EXACT_PARTS = {
    "aaa", "aaaa", "beispiel", "dummy", "example", "exemplo", "hogehoge", "hoge", "name", "sample",
    "test", "xxx", "xxxx", "xxxxx", "yourdomain", "yourdmain",
}
_PLACEHOLDER_DOMAIN_WORDS = {"aaa", "beispiel", "dominio", "dummy", "email", "example", "exemplo", "sample", "test", "yourdomain", "yourdmain"}
_PLACEHOLDER_STEM_WORDS = {"beispiel", "dummy", "email", "example", "exemplo", "name", "sample", "test"}
_EMAIL_PRIORITY_LOCAL_PARTS = {
    "contact", "customer", "hello", "help", "hr", "info", "inquiry", "office",
    "privacy", "pr", "press", "recruit", "recruiting", "sales", "service",
    "support", "customercare", "customercare", "enquiries", "enquiry",
}
_FREE_MAIL_DOMAINS = {
    "aol.com", "gmail.com", "googlemail.com", "hotmail.com", "icloud.com",
    "live.com", "mac.com", "me.com", "msn.com", "outlook.com", "pm.me",
    "proton.me", "protonmail.com", "yahoo.co.jp", "yahoo.com", "yahoo.com.br",
}
_OFFSITE_ALWAYS_DROP_LOCAL_PARTS = {"found", "posted", "profile", "webmaster", "website"}


@dataclass
class EmailSetAnalysis:
    emails: list[str]
    same_domain_emails: list[str]
    domain_count: int
    suspicious_directory_like: bool


def extract_registrable_domain(value: str) -> str:
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


def split_emails(values: Iterable[str] | str) -> list[str]:
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
    return _drop_prefixed_local_duplicates(result)


def join_emails(values: Iterable[str] | str) -> str:
    return "; ".join(split_emails(values))


def normalize_email_candidate(value: object) -> str:
    text = _INVISIBLE_RE.sub("", unquote(str(value or ""))).strip().lower()
    if not text:
        return ""
    text = text.replace("mailto:", "")
    text = re.sub(r"^(?:u003e|u003c|>|<)+", "", text)
    text = re.sub(r"(?i)\s*(?:\[at\]|\(at\))\s*|\s+at\s+", "@", text)
    text = re.sub(r"(?i)\s*(?:\[dot\]|\(dot\))\s*|\s+dot\s+", ".", text)
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


def analyze_email_set(website: str, values: Iterable[str] | str) -> EmailSetAnalysis:
    emails = _prioritize_emails(split_emails(values))
    same_domain_emails = [email for email in emails if email_matches_website(website, email)]
    domains = {extract_registrable_domain(email.split("@", 1)[1]) for email in emails if "@" in email}
    suspicious = bool(emails and not same_domain_emails and len(emails) >= 8 and len(domains) >= 5)
    return EmailSetAnalysis(
        emails=emails,
        same_domain_emails=same_domain_emails,
        domain_count=len(domains),
        suspicious_directory_like=suspicious,
    )


def filter_emails_for_website(website: str, values: Iterable[str] | str) -> list[str]:
    emails = split_emails(values)
    if not emails:
        return []
    site_domain = extract_registrable_domain(website)
    if not site_domain:
        return _prioritize_emails(emails)
    same_domain = [email for email in emails if email_matches_website(website, email)]
    same_domain_locals = {_normalize_local_part_key(email.split("@", 1)[0]) for email in same_domain if "@" in email}
    filtered: list[str] = list(same_domain)
    for email in emails:
        if email in filtered:
            continue
        if same_domain_locals and "@" in email:
            local_key = _normalize_local_part_key(email.split("@", 1)[0])
            if local_key in same_domain_locals:
                continue
        if _should_keep_offsite_email(email, bool(same_domain)):
            filtered.append(email)
    filtered = _drop_same_local_broken_domain_variants(filtered)
    return _prioritize_emails(filtered)


def email_matches_website(website: str, email: str) -> bool:
    site_domain = extract_registrable_domain(website)
    value = str(email or "").strip().lower()
    if not site_domain or "@" not in value:
        return False
    email_domain = value.split("@", 1)[1]
    if email_domain.startswith("www."):
        return False
    return email_domain == site_domain or email_domain.endswith(f".{site_domain}")


def extract_emails_from_html(raw_html: str) -> list[str]:
    html_text = str(raw_html or "")
    if not html_text.strip():
        return []
    if not has_email_evidence_hint(html_text):
        return []
    normalized = html.unescape(html_text)
    normalized = _SCRIPT_BLOCK_RE.sub(" ", normalized)
    normalized = normalized.replace("%40", "@").replace("%2E", ".")
    normalized = re.sub(r"(?i)\s*(?:\[at\]|\(at\))\s*|\s+at\s+", "@", normalized)
    normalized = re.sub(r"(?i)\s*(?:\[dot\]|\(dot\))\s*|\s+dot\s+", ".", normalized)
    found: list[str] = []
    for match in _EMAIL_RE.findall(normalized):
        value = str(match or "").strip().lower().rstrip(".,);:]}>")
        if value and value not in found:
            found.append(value)
    for decoded in _extract_cloudflare_emails(normalized):
        if decoded not in found:
            found.append(decoded)
    return found


def _extract_cloudflare_emails(html_text: str) -> list[str]:
    found: list[str] = []
    for encoded in _CFEMAIL_RE.findall(str(html_text or "")):
        email = normalize_email_candidate(_decode_cloudflare_email(encoded))
        if email and email not in found:
            found.append(email)
    return found


def _decode_cloudflare_email(encoded: str) -> str:
    value = str(encoded or "").strip()
    if len(value) < 4 or len(value) % 2 != 0:
        return ""
    try:
        data = bytes.fromhex(value)
    except ValueError:
        return ""
    if not data:
        return ""
    key = data[0]
    return "".join(chr(byte ^ key) for byte in data[1:])


def extract_same_domain_emails_from_embedded_content(website: str, raw_html: str) -> list[str]:
    html_text = str(raw_html or "")
    if not html_text.strip():
        return []
    if not has_email_evidence_hint(html_text):
        return []
    normalized = html.unescape(html_text)
    normalized = normalized.replace("%40", "@").replace("%2E", ".")
    normalized = re.sub(r"(?i)\s*(?:\[at\]|\(at\))\s*|\s+at\s+", "@", normalized)
    normalized = re.sub(r"(?i)\s*(?:\[dot\]|\(dot\))\s*|\s+dot\s+", ".", normalized)
    found: list[str] = []
    for match in _EMAIL_RE.findall(normalized):
        email = normalize_email_candidate(match)
        if not email:
            continue
        if not email_matches_website(website, email):
            continue
        if email not in found:
            found.append(email)
    return _prioritize_emails(found)


def collect_emails_for_pages(website: str, pages: list[tuple[str, str]]) -> tuple[list[str], dict[str, list[str]]]:
    collected: list[str] = []
    page_hits: dict[str, list[str]] = {}
    for url, html_text in pages:
        if not has_email_evidence_hint(html_text):
            continue
        page_emails = extract_emails_from_html(html_text)
        if not _has_direct_same_domain_email(website, page_emails):
            embedded_same_domain = extract_same_domain_emails_from_embedded_content(website, html_text)
            for email in embedded_same_domain:
                if email not in page_emails:
                    page_emails.append(email)
        analysis = analyze_email_set(website, page_emails)
        if analysis.suspicious_directory_like:
            if analysis.same_domain_emails:
                page_emails = analysis.same_domain_emails
            else:
                continue
        cleaned = drop_typo_domains_for_site(website, filter_emails_for_website(website, page_emails))
        if cleaned:
            page_hits[url] = cleaned
        for email in cleaned:
            if email not in collected:
                collected.append(email)
    return drop_typo_domains_for_site(website, filter_emails_for_website(website, collected)), page_hits
def has_email_evidence_hint(raw_html: str) -> bool:
    text = str(raw_html or "")
    if not text.strip():
        return False
    lowered = text.lower()
    if any(token in lowered for token in ("@", "%40", "&#64;", "&commat;", "mailto:", "data-cfemail")):
        return True
    unescaped = html.unescape(text)
    return (
        _OBFUSCATED_EMAIL_HINT_RE.search(unescaped) is not None
        or _EMAIL_LABEL_DOMAIN_HINT_RE.search(unescaped) is not None
    )


def _has_direct_same_domain_email(website: str, emails: Iterable[str]) -> bool:
    for raw in emails:
        email = normalize_email_candidate(raw)
        if email and email_matches_website(website, email):
            return True
    return False


def _normalize_text_for_email_evidence(pages: list[tuple[str, str]]) -> str:
    parts: list[str] = []
    for _url, html_text in pages:
        text = str(html_text or "")
        if not text.strip():
            continue
        normalized = html.unescape(text)
        normalized = normalized.replace("%40", "@").replace("%2e", ".").replace("%2E", ".")
        normalized = re.sub(r"(?i)\s*(?:\[at\]|\(at\))\s*|\s+at\s+", "@", normalized)
        normalized = re.sub(r"(?i)\s*(?:\[dot\]|\(dot\))\s*|\s+dot\s+", ".", normalized)
        parts.append(normalized.lower())
    return "\n".join(parts)


def select_emails_present_in_pages(emails: Iterable[str], pages: list[tuple[str, str]]) -> list[str]:
    """只保留在页面正文里能找到出处的邮箱，挡掉 AI 凭空编造的地址。

    对去混淆后整页文本做证据匹配：整条邮箱命中，或本地名与域名分别都在页面里出现
    （覆盖 name [at] domain [dot] com 这类被拆开的写法），才算有据。
    """
    evidence = _normalize_text_for_email_evidence(pages)
    if not evidence:
        return []
    # 去掉所有空白后再比对：name [at] acme [dot] com 去混淆后是 "name @ acme . com"，
    # 压成 "name@acme.com" 才能和 AI 还原出的标准邮箱对上。
    evidence_nospace = re.sub(r"\s+", "", evidence)
    kept: list[str] = []
    for raw in emails:
        email = str(raw or "").strip().lower()
        if "@" not in email or email in kept:
            continue
        local, domain = email.split("@", 1)
        if email in evidence_nospace or (
            local and domain and local in evidence_nospace and domain in evidence_nospace
        ):
            kept.append(email)
    return kept


def merge_ai_emails_for_website(
    website: str,
    rule_emails: Iterable[str],
    ai_emails: Iterable[str],
    pages: list[tuple[str, str]],
) -> list[str]:
    """规则邮箱 ∪ 经证据校验的 AI 邮箱，走和规则同一套站点域优先 / 去重收尾。"""
    evidenced_ai = select_emails_present_in_pages(split_emails(ai_emails), pages)
    rule_list = list(rule_emails)
    seen = set(rule_list)
    combined = rule_list + [email for email in evidenced_ai if email not in seen]
    return drop_typo_domains_for_site(website, filter_emails_for_website(website, combined))


def _email_appears_inside_url_token(text: str, email: str) -> bool:
    for token in re.split(r"\s+", str(text or "").strip()):
        if "://" in token and email in token:
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
    if _local_part_looks_like_address_noise(str(local or "")):
        return True
    if normalized in _PLACEHOLDER_EXACT_PARTS or _matches_placeholder_stem(normalized):
        return True
    if re.fullmatch(r"x{4,}", normalized) or re.fullmatch(r"a{3,}", normalized) or re.fullmatch(r"0{3,}", normalized):
        return True
    if re.search(r"x{4,}|0{4,}", normalized):
        return True
    return False


def _domain_is_placeholder(domain: str) -> bool:
    labels = [label for label in str(domain or "").strip().lower().split(".") if label]
    if len(labels) < 2:
        return True
    if ".".join(labels[-2:]) in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) < 3:
        return True
    core_labels = labels[:-2] if ".".join(labels[-2:]) in _MULTI_LABEL_PUBLIC_SUFFIXES else labels[:-1]
    if not core_labels:
        return True
    for label in core_labels:
        normalized = re.sub(r"[^a-z0-9]+", "", label)
        if not normalized:
            return True
        if normalized in _PLACEHOLDER_DOMAIN_WORDS:
            return True
        if any(normalized.startswith(word) or normalized.endswith(word) for word in _PLACEHOLDER_DOMAIN_WORDS):
            return True
    return False


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
    if domain.startswith("www."):
        return False
    if registrable_domain in _FREE_MAIL_DOMAINS:
        return True
    if has_same_domain_email and re.sub(r"[^a-z0-9]+", "", local) in _OFFSITE_ALWAYS_DROP_LOCAL_PARTS:
        return False
    return True


def _normalize_local_part_key(local: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(local or "").strip().lower())


def _drop_same_local_broken_domain_variants(emails: list[str]) -> list[str]:
    groups: dict[str, list[str]] = {}
    for email in emails:
        local, _, domain = email.partition("@")
        key = _normalize_local_part_key(local)
        if not key or not domain:
            continue
        groups.setdefault(key, []).append(email)
    drop_set: set[str] = set()
    for group in groups.values():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda item: (len(item.partition("@")[2]), item))
        winner = ordered[0]
        winner_domain = winner.partition("@")[2]
        for email in ordered[1:]:
            domain = email.partition("@")[2]
            if domain.startswith(winner_domain) or winner_domain.startswith(domain):
                drop_set.add(email)
    return [email for email in emails if email not in drop_set]


def drop_typo_domains_for_site(website: str, emails: list[str]) -> list[str]:
    site_domain = extract_registrable_domain(website)
    if not site_domain:
        return emails
    result: list[str] = []
    for email in emails:
        domain = email.partition("@")[2]
        registrable = extract_registrable_domain(domain)
        if registrable and registrable != site_domain:
            if registrable.startswith(site_domain) or site_domain.startswith(registrable):
                continue
        result.append(email)
    return result


def _prioritize_emails(emails: list[str]) -> list[str]:
    return sorted(emails, key=lambda item: (-_email_priority_score(item), emails.index(item)))


def _drop_prefixed_local_duplicates(emails: list[str]) -> list[str]:
    values = [str(email or "").strip().lower() for email in emails if str(email or "").strip()]
    existing = set(values)
    result: list[str] = []
    for email in values:
        local, _, domain = email.partition("@")
        if local.startswith("at") and len(local) > 2:
            base_local = local[2:]
            base_email = f"{base_local}@{domain}"
            normalized_base = re.sub(r"[^a-z0-9]+", "", base_local)
            if base_email in existing and normalized_base in _EMAIL_PRIORITY_LOCAL_PARTS:
                continue
        if email not in result:
            result.append(email)
    return result


def _email_priority_score(email: str) -> int:
    value = str(email or "").strip().lower()
    if "@" not in value:
        return 0
    local = value.split("@", 1)[0]
    normalized = re.sub(r"[^a-z0-9]+", "", local)
    if local in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 100
    if normalized in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 90
    if any(token in normalized for token in _EMAIL_PRIORITY_LOCAL_PARTS):
        return 70
    if re.fullmatch(r"[a-z]+", normalized):
        return 20
    if re.search(r"\d", normalized):
        return 5
    return 10


def _local_part_looks_like_address_noise(local: str) -> bool:
    text = str(local or "").strip().lower()
    if len(text) < 36:
        return False
    separator_count = sum(text.count(ch) for ch in "+-._")
    digit_count = sum(ch.isdigit() for ch in text)
    address_words = ("road", "street", "lane", "avenue", "drive", "close", "court", "house", "london", "catford")
    if separator_count >= 4 and digit_count >= 2:
        return True
    return any(word in text for word in address_words) and separator_count >= 2
