from __future__ import annotations

from urllib.parse import urlparse


FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "yahoo.co.jp",
    "icloud.com",
    "me.com",
    "qq.com",
    "163.com",
    "126.com",
    "sina.com",
    "sohu.com",
    "proton.me",
    "protonmail.com",
}

EMAIL_PREFERRED_LOCAL = {
    "info",
    "contact",
    "sales",
    "office",
    "admin",
    "support",
    "hello",
    "inquiry",
    "otoiawase",
    "toiawase",
    "webmaster",
    "mail",
}

EMAIL_DISFAVORED_TOKENS = {
    "feedback",
    "privacy",
    "security",
    "vulnerability",
    "abuse",
    "legal",
    "compliance",
    "press",
    "media",
    "pr",
    "ir",
    "investor",
    "billing",
    "account",
    "accounting",
    "finance",
    "invoice",
    "career",
    "recruit",
    "jobs",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}

INVALID_LOCAL_PARTS = {
    "the",
    "2",
    "3",
    "4",
    "123",
    "20info",
    "aaa",
    "ab",
    "abc",
    "acc",
    "acc_kaz",
    "account",
    "accounts",
    "accueil",
    "ad",
    "adi",
    "adm",
    "an",
    "and",
    "available",
    "b",
    "c",
    "cc",
    "com",
    "domain",
    "domen",
    "email",
    "fb",
    "foi",
    "for",
    "found",
    "g",
    "get",
    "h",
    "here",
    "includes",
    "linkedin",
    "mailbox",
    "more",
    "my_name",
    "n",
    "name",
    "need",
    "nfo",
    "ninfo",
    "now",
    "o",
    "online",
    "post",
    "s",
    "sales2",
    "test",
    "up",
    "we",
    "www",
    "xxx",
    "xxxxx",
    "y",
    "username",
    "firstname.lastname",
}


def normalize_host(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host.strip(".")


def matches_company_domain(website: str, email_domain: str) -> bool:
    host = normalize_host(website)
    domain = (email_domain or "").lower().strip(".")
    if not host or not domain:
        return False
    if host == domain:
        return True
    if host.endswith("." + domain):
        return True
    if domain.endswith("." + host):
        return True
    return False


def is_company_domain_email(website: str, email: str) -> bool:
    addr = (email or "").strip().lower()
    if "@" not in addr:
        return False
    local, domain = addr.rsplit("@", 1)
    if not local or not domain:
        return False
    if domain in FREE_EMAIL_DOMAINS:
        return False
    return matches_company_domain(website, domain)
