from __future__ import annotations

import html
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

_SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".tar", ".gz", ".rar", ".7z", ".exe", ".dmg", ".apk",
}
_RELATED_SUBDOMAIN_HOST_TOKENS = {
    "about", "atendimento", "career", "careers", "company", "contact", "contato",
    "help", "inquiry", "jobs", "leadership", "people", "support", "team",
}
_RELATED_SUBDOMAIN_PATH_TOKENS = {
    "about", "atendimento", "board", "career", "careers", "company", "contact",
    "contato", "director", "executive", "fale", "founder", "governance",
    "iletisim", "inquiry", "jobs", "leadership", "management", "officers",
    "ouvidoria", "people", "president", "privacy", "support", "team", "terms",
}
_SUBDOMAIN_SCAN_PAGE_TOKENS = {
    "about", "atendimento", "contact", "contato", "company", "help", "iletisim",
    "inquiry", "people", "privacy", "support", "team",
}
_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "ref_src",
    "srsltid",
}
_UNSUPPORTED_PATH_FRAGMENTS = (
    "/.well-known/sgcaptcha",
    "/cdn-cgi/",
    "/wp-admin/",
    "/wp-login.php",
    "/xmlrpc.php",
)
_UNSUPPORTED_QUERY_PAIRS = (
    ("action", "lostpassword"),
)
_ANCHOR_TAG_RE = re.compile(r"<a\b(?P<attrs>[^>]*)>", re.IGNORECASE | re.DOTALL)
_HREF_ATTR_RE = re.compile(r"\bhref\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
_LINK_HINT_FRAGMENT_PREFIX = "oi-link-"
_VALUE_ANCHOR_PHRASES = (
    ("bize ulasin", "bize-ulasin"),
    ("fale conosco", "fale-conosco"),
    ("insan kaynaklari", "insan-kaynaklari"),
    ("trabalhe conosco", "trabalhe-conosco"),
    ("e posta", "email"),
)
_VALUE_ANCHOR_TOKENS = {
    "atendimento",
    "career",
    "careers",
    "contact",
    "contato",
    "email",
    "fale",
    "hakkimizda",
    "iletisim",
    "inquiry",
    "kariyer",
    "kontakt",
    "kurumsal",
    "kvkk",
    "mail",
    "mailform",
    "otoiawase",
    "ouvidoria",
    "recruit",
    "saiyo",
    "toiawase",
    "ulasin",
}
_HOMEPAGE_VALUE_LINK_TOKENS = _VALUE_ANCHOR_TOKENS | {
    "conosco",
    "dpo",
    "form",
    "lgpd",
    "privacidade",
    "privacy",
    "sac",
    "support",
    "trabalhe",
}
_COMMON_VALUE_PATHS = (
    "/impressum",
    "/imprint",
    "/kontakt",
    "/kontakt.html",
    "/iletisim",
    "/iletisim.html",
    "/iletisim/index.html",
    "/bize-ulasin",
    "/contato",
    "/Contato",
    "/Home/Contato",
    "/home/Contato",
    "/home/contato",
    "/fale-conosco",
    "/faleconosco",
    "/FaleConosco",
    "/Home/FaleConosco",
    "/atendimento",
    "/ouvidoria",
    "/inquiry",
    "/contact/form",
    "/contact/mail",
    "/mailform",
    "/ueber-uns",
    "/uber-uns",
    "/hakkimizda",
    "/kurumsal",
    "/Home/Institucional",
    "/institucional",
    "/corporate",
    "/about-us/our-people",
    "/our-people",
    "/company-leadership",
    "/executive-team",
    "/leadership",
    "/management",
    "/our-team",
    "/team-members",
    "/team",
    "/people",
    "/about-us",
    "/about",
    "/about.html",
    "/company",
    "/ir/contact",
    "/contact-us",
    "/contact",
    "/contact.html",
    "/form",
    "/recruit",
    "/recruit/form",
    "/kariyer",
    "/insan-kaynaklari",
    "/trabalhe-conosco",
    "/legal-notice",
    "/privacy-policy",
    "/privacy",
    "/kvkk",
    "/lgpd",
    "/politica-de-privacidade",
    "/politica-de-privacidade/",
    "/politica-de-privacidade.html",
    "/politica-privacidade",
    "/politica-privacidade/",
    "/politica-privacidade.html",
    "/privacidade",
    "/terms",
)
_DISCOVERY_PRIORITY_PHRASES = (
    ("/about-us/our-people/", 90),
    ("/about-us/our-people", 88),
    ("/our-people/", 86),
    ("/our-people", 84),
    ("/team-members/", 84),
    ("/team-members", 82),
    ("/impressum", 80),
    ("/imprint", 80),
    ("/kontakt", 76),
    ("/iletisim", 76),
    ("/bize-ulasin", 70),
    ("/fale-conosco", 70),
    ("/contato", 68),
    ("/inquiry", 68),
    ("/contact/form", 66),
    ("/contact/mail", 66),
    ("/mailform", 66),
    ("/ueber-uns", 60),
    ("/uber-uns", 58),
    ("/hakkimizda", 58),
    ("/kurumsal", 56),
    ("/institucional", 54),
    ("/company-leadership", 78),
    ("/executive-team", 78),
    ("/leadership", 74),
    ("/management", 72),
    ("/board", 70),
    ("/governance", 70),
    ("/officers", 68),
    ("/about-us", 56),
    ("/about", 44),
    ("/contact-us", 32),
    ("/contact", 24),
    ("/ouvidoria", 24),
    ("/atendimento", 22),
    ("/politica-de-privacidade", 24),
    ("/politica-privacidade", 23),
    ("/privacidade", 22),
    ("/trabalhe-conosco", 20),
    ("/recruit/form", 20),
)
_BRAZIL_SPECULATIVE_PATH_WEIGHTS = (
    ("fale-conosco", 172),
    ("home/faleconosco", 154),
    ("home/contato", 154),
    ("contato", 166),
    ("faleconosco", 156),
    ("atendimento", 132),
    ("ouvidoria", 128),
    ("lgpd", 96),
    ("politica-de-privacidade", 98),
    ("politica-privacidade", 96),
    ("privacidade", 92),
    ("trabalhe-conosco", 82),
    ("institucional", 72),
    ("contact", 60),
)
_TURKEY_SPECULATIVE_PATH_WEIGHTS = (
    ("iletisim", 180),
    ("bize-ulasin", 172),
    ("kvkk", 112),
    ("hakkimizda", 96),
    ("kurumsal", 94),
    ("insan-kaynaklari", 86),
    ("kariyer", 82),
    ("contact", 60),
)
_JAPAN_SPECULATIVE_PATH_WEIGHTS = (
    ("inquiry", 180),
    ("mailform", 172),
    ("contact/form", 168),
    ("contact/mail", 166),
    ("recruit/form", 112),
    ("form", 104),
    ("contact", 92),
)
_GENERIC_SPECULATIVE_PATH_WEIGHTS = (
    ("contact-us", 120),
    ("contact", 112),
    ("kontakt", 108),
    ("impressum", 96),
    ("imprint", 94),
    ("privacy", 64),
)
_DISCOVERY_PRIORITY_NEGATIVE_TOKENS = {
    "article",
    "articles",
    "award",
    "awards",
    "blog",
    "case",
    "event",
    "events",
    "funds",
    "insight",
    "insights",
    "journeys",
    "news",
    "post",
    "posts",
    "resource",
    "resources",
    "stories",
    "story",
    "update",
    "updates",
}
_DISCOVERY_NAMED_DETAIL_CONTEXT_TOKENS = {
    "about",
    "leadership",
    "management",
    "officers",
    "our",
    "people",
    "profile",
    "profiles",
    "referral",
    "referrals",
    "team",
}
_DISCOVERY_NAMED_DETAIL_STOP_TOKENS = _DISCOVERY_NAMED_DETAIL_CONTEXT_TOKENS | {
    "business",
    "careers",
    "charitable",
    "company",
    "corporate",
    "culture",
    "cultures",
    "director",
    "directors",
    "executive",
    "foundation",
    "fund",
    "funds",
    "group",
    "investment",
    "investments",
    "legal",
    "office",
    "our",
    "people",
    "planning",
    "responsibility",
    "service",
    "services",
    "wealth",
}


def extract_same_site_links(html_text: str, page_url: str, *, limit: int) -> list[str]:
    base_host = (urlparse(page_url).netloc or "").strip().lower()
    if not base_host:
        return []
    join_base = _pick_relative_link_base_url(html_text, page_url)
    result: list[str] = []
    seen: set[str] = set()
    for raw_href, anchor_text in _iter_anchor_links(html_text):
        href = raw_href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(join_base, href)
        parsed = urlparse(absolute)
        link_host = (parsed.netloc or "").strip().lower()
        if not link_host or not (link_host == base_host or link_host.endswith(f".{base_host}") or base_host.endswith(f".{link_host}")):
            continue
        normalized = normalize_discovery_url(_add_anchor_hint_fragment(absolute, anchor_text))
        if normalized not in seen and is_supported_url(normalized):
            seen.add(normalized)
            result.append(normalized)
            if len(result) >= limit:
                break
    return result


def _iter_anchor_links(html_text: str) -> list[tuple[str, str]]:
    text = str(html_text or "")
    links: list[tuple[str, str]] = []
    for match in _ANCHOR_TAG_RE.finditer(text):
        href_match = _HREF_ATTR_RE.search(match.group("attrs") or "")
        if href_match is None:
            continue
        links.append((href_match.group(1), _extract_anchor_body_hint(text, match.end())))
    return links


def _extract_anchor_body_hint(html_text: str, start_index: int) -> str:
    closing_index = html_text.find("</a>", start_index)
    next_anchor_index = html_text.find("<a", start_index)
    end_index = len(html_text)
    if closing_index >= 0:
        end_index = min(end_index, closing_index)
    if next_anchor_index >= 0:
        end_index = min(end_index, next_anchor_index)
    return html_text[start_index : min(end_index, start_index + 500)]


def _pick_relative_link_base_url(html_text: str, page_url: str) -> str:
    directory_base_url = _ensure_directory_base_url(page_url)
    parsed = urlparse(directory_base_url)
    base_host = (parsed.netloc or "").strip().lower()
    dominant_host = _pick_dominant_www_pair_host(html_text, base_host)
    if not dominant_host:
        return directory_base_url
    return parsed._replace(netloc=dominant_host).geturl()


def _ensure_directory_base_url(page_url: str) -> str:
    parsed = urlparse(page_url)
    path = str(parsed.path or "")
    if not path or path.endswith("/"):
        return page_url
    last_segment = path.rsplit("/", 1)[-1]
    if "." in last_segment:
        return page_url
    return parsed._replace(path=f"{path}/").geturl()


def _pick_dominant_www_pair_host(html_text: str, base_host: str) -> str:
    counts: dict[str, int] = {}
    for raw_href, _anchor_text in _iter_anchor_links(html_text):
        parsed = urlparse(str(raw_href or "").strip())
        host = (parsed.netloc or "").strip().lower()
        if not host or not _is_www_pair(base_host, host):
            continue
        counts[host] = counts.get(host, 0) + 1
    if not counts:
        return ""
    host, count = max(counts.items(), key=lambda item: (item[1], item[0]))
    return host if count >= 2 else ""


def _is_www_pair(left: str, right: str) -> bool:
    if not left or not right or left == right:
        return False
    return left == f"www.{right}" or right == f"www.{left}"


def _add_anchor_hint_fragment(url: str, anchor_text: str) -> str:
    hint = _pick_anchor_value_hint(anchor_text)
    if not hint:
        return url
    if _url_has_value_hint(url):
        return url
    parsed = urlparse(url)
    if parsed.fragment.startswith(_LINK_HINT_FRAGMENT_PREFIX):
        return url
    return parsed._replace(fragment=f"{_LINK_HINT_FRAGMENT_PREFIX}{hint}").geturl()


def _pick_anchor_value_hint(anchor_text: str) -> str:
    normalized = _normalize_anchor_text(anchor_text)
    if not normalized:
        return ""
    for phrase, hint in _VALUE_ANCHOR_PHRASES:
        if phrase in normalized:
            return hint
    for token in re.split(r"[\W_]+", normalized):
        if token in _VALUE_ANCHOR_TOKENS:
            return token
    return ""


def _normalize_anchor_text(anchor_text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html.unescape(str(anchor_text or "")))
    lowered = re.sub(r"\s+", " ", text).strip().lower()
    if not lowered:
        return ""
    return unicodedata.normalize("NFKD", lowered).encode("ascii", "ignore").decode("ascii")


def _url_has_value_hint(url: str) -> bool:
    return any(token in _VALUE_ANCHOR_TOKENS for token in extract_url_hint_tokens(url))


def has_homepage_value_links(start_url: str, urls: list[str]) -> bool:
    for url in urls:
        tokens = extract_url_hint_tokens(url)
        if any(token in _HOMEPAGE_VALUE_LINK_TOKENS for token in tokens):
            return True
        if _discovery_priority_score(start_url, url) >= 18:
            return True
    return False


def extract_same_org_seed_urls(html_text: str, page_url: str, *, site_domain: str, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    page_host = (urlparse(page_url).netloc or "").strip().lower()
    for raw_href in re.findall(r'<a\s[^>]*href=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        href = raw_href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        host = (parsed.netloc or "").strip().lower()
        if not host or host == page_host:
            continue
        if extract_registrable_domain(host) != site_domain:
            continue
        normalized = normalize_discovery_url(absolute)
        if normalized in seen or not looks_related_subdomain_seed(normalized):
            continue
        if not is_supported_url(normalized):
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= limit:
            break
    return result


def is_supported_url(url: str) -> bool:
    text = str(url or "").strip()
    lowered = text.lower()
    if "{" in text or "}" in text or "itemdataobject." in lowered:
        return False
    parsed = urlparse(text)
    path = str(parsed.path or "").lower()
    query = str(parsed.query or "").lower()
    if any(fragment in path for fragment in _UNSUPPORTED_PATH_FRAGMENTS):
        return False
    if any(f"{key}={value}" in query for key, value in _UNSUPPORTED_QUERY_PAIRS):
        return False
    return not any(path.endswith(suffix) for suffix in _SKIP_EXTENSIONS)


def merge_unique_urls(left: list[str], right: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*left, *right]:
        value = normalize_discovery_url(str(url or "").strip())
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def normalize_discovery_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text
    kept_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        clean_key = str(key or "").strip().lower()
        if not clean_key:
            continue
        if clean_key.startswith("utm_") or clean_key in _TRACKING_QUERY_KEYS:
            continue
        kept_pairs.append((key, value))
    fragment = parsed.fragment if parsed.fragment.startswith(_LINK_HINT_FRAGMENT_PREFIX) else ""
    return parsed._replace(query=urlencode(kept_pairs, doseq=True), fragment=fragment).geturl()


def build_common_probe_urls(start_url: str) -> list[str]:
    parsed = urlparse(start_url)
    if not parsed.scheme or not parsed.netloc:
        return []
    locale_prefix = extract_path_locale_prefix(parsed.path)
    base_prefixes = _build_common_probe_prefixes(parsed.netloc, locale_prefix)
    result: list[str] = []
    seen: set[str] = set()
    hosts = [parsed.netloc]
    if parsed.netloc and not parsed.netloc.lower().startswith("www."):
        hosts.append(f"www.{parsed.netloc}")
    for host in hosts:
        for path in _COMMON_VALUE_PATHS:
            for base_prefix in base_prefixes:
                joined_path = f"{base_prefix}{path}" if base_prefix else path
                probe_url = parsed._replace(netloc=host, path=joined_path, query="", fragment="").geturl()
                if probe_url not in seen:
                    seen.add(probe_url)
                    result.append(probe_url)
    if _should_prioritize_country_common_paths(parsed.netloc):
        return _prioritize_common_probe_urls(start_url, result)
    return result


def _should_prioritize_country_common_paths(host: str) -> bool:
    lowered = str(host or "").strip().lower()
    return (
        lowered.endswith(".br")
        or ".com.br" in lowered
        or lowered.endswith(".tr")
        or ".com.tr" in lowered
        or lowered.endswith(".jp")
        or ".co.jp" in lowered
    )


def _prioritize_common_probe_urls(start_url: str, urls: list[str]) -> list[str]:
    scored = [
        (-_score_speculative_common_value_url(start_url, url), index, url)
        for index, url in enumerate(urls)
    ]
    scored.sort()
    return [url for _score, _index, url in scored]


def pick_speculative_common_value_urls(start_url: str, *, limit: int) -> list[str]:
    if limit <= 0:
        return []
    scored: list[tuple[int, int, str]] = []
    for index, url in enumerate(build_common_probe_urls(start_url)):
        score = _score_speculative_common_value_url(start_url, url)
        if score <= 0:
            continue
        scored.append((-score, index, url))
    scored.sort()
    return [url for _, _, url in scored[:limit]]


def _score_speculative_common_value_url(start_url: str, url: str) -> int:
    parsed_start = urlparse(start_url)
    parsed_url = urlparse(url)
    host = (parsed_start.netloc or "").lower()
    path = (parsed_url.path or "").strip("/").lower()
    if not path:
        return 0
    score = 0
    if host.endswith(".br") or ".com.br" in host:
        score = _score_country_path(path, _BRAZIL_SPECULATIVE_PATH_WEIGHTS)
    elif host.endswith(".tr") or ".com.tr" in host:
        score = _score_country_path(path, _TURKEY_SPECULATIVE_PATH_WEIGHTS)
    elif host.endswith(".jp") or ".co.jp" in host:
        score = _score_country_path(path, _JAPAN_SPECULATIVE_PATH_WEIGHTS)
    if score <= 0:
        score = _score_country_path(path, _GENERIC_SPECULATIVE_PATH_WEIGHTS)
    if score <= 0:
        return 0
    return score + _score_locale_prefix(path, host) + _score_original_host(parsed_start.netloc, parsed_url.netloc)


def _score_country_path(path: str, weights: tuple[tuple[str, int], ...]) -> int:
    for fragment, weight in weights:
        if fragment in path:
            return weight
    return 0


def _score_locale_prefix(path: str, host: str) -> int:
    prefix = path.split("/", 1)[0]
    if prefix == "pt" and (host.endswith(".br") or ".com.br" in host):
        return 8
    if prefix == "tr" and (host.endswith(".tr") or ".com.tr" in host):
        return 8
    if prefix == "ja" and (host.endswith(".jp") or ".co.jp" in host):
        return 8
    if prefix in {"br", "jp"}:
        return 4
    if prefix == "en":
        return 2
    return 6


def _score_original_host(start_host: str, candidate_host: str) -> int:
    if (start_host or "").lower() == (candidate_host or "").lower():
        return 6
    return 0


def _build_common_probe_prefixes(host: str, locale_prefix: str) -> list[str]:
    prefixes: list[str] = []
    if locale_prefix:
        prefixes.append(locale_prefix)
    inferred = _infer_common_locale_prefixes(host)
    for prefix in [*inferred, ""]:
        if prefix not in prefixes:
            prefixes.append(prefix)
    return prefixes


def _infer_common_locale_prefixes(host: str) -> list[str]:
    lowered = str(host or "").strip().lower()
    if lowered.endswith(".tr") or ".com.tr" in lowered:
        return ["/tr", "/en"]
    if lowered.endswith(".br") or ".com.br" in lowered:
        return ["/pt", "/br", "/en"]
    if lowered.endswith(".jp") or ".co.jp" in lowered:
        return ["/ja", "/jp", "/en"]
    return []


def extract_path_locale_prefix(path: str) -> str:
    cleaned = str(path or "").strip("/")
    if not cleaned:
        return ""
    first = cleaned.split("/", 1)[0].strip().lower()
    if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", first):
        return f"/{first}"
    return ""


def pick_subdomain_probe_urls(start_url: str, direct_urls: list[str]) -> list[str]:
    start_domain = extract_registrable_domain(start_url)
    picked: list[str] = []
    for url in direct_urls:
        parsed = urlparse(url)
        host = (parsed.netloc or "").strip().lower()
        if not host or extract_registrable_domain(host) != start_domain:
            continue
        tokens = extract_url_hint_tokens(url)
        if not any(token in _SUBDOMAIN_SCAN_PAGE_TOKENS for token in tokens):
            continue
        if url not in picked:
            picked.append(url)
        if len(picked) >= 4:
            break
    return picked


def looks_related_subdomain_seed(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return False
    host_tokens = [token for token in re.split(r"[\W_]+", host) if len(token) >= 3]
    path_tokens = extract_url_hint_tokens(url)
    if any(token in _RELATED_SUBDOMAIN_HOST_TOKENS for token in host_tokens):
        return True
    if any(token in _RELATED_SUBDOMAIN_PATH_TOKENS for token in path_tokens):
        return True
    return False


def extract_url_hint_tokens(url: str) -> list[str]:
    parsed = urlparse(str(url or ""))
    tokens: list[str] = []
    for part in parsed.path.split("/"):
        for token in re.split(r"[\W_]+", part.strip().lower()):
            clean = token.strip().lower()
            if len(clean) < 3 or clean in tokens:
                continue
            tokens.append(clean)
    return tokens


def extract_registrable_domain(value: str) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if "://" in host or "/" in host:
        parsed = urlparse(host if "://" in host else f"https://{host}")
        host = str(parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in {"ac.jp", "co.jp", "go.jp", "ne.jp", "or.jp", "ac.uk", "co.uk", "gov.uk", "org.uk", "com.au", "net.au", "org.au", "com.br", "net.br", "org.br", "co.nz", "org.nz", "com.mx", "org.mx"} and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2


def prioritize_discovery_urls(start_url: str, urls: list[str], *, limit: int) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for url in urls:
        normalized = normalize_discovery_url(url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    ranked = sorted(
        enumerate(unique),
        key=lambda item: (
            -_discovery_priority_score(start_url, item[1]),
            item[0],
        ),
    )
    return [url for _index, url in ranked[:limit]]


def _discovery_priority_score(start_url: str, url: str) -> int:
    lowered = str(url or "").lower()
    tokens = extract_url_hint_tokens(url)
    score = 0
    for phrase, value in _DISCOVERY_PRIORITY_PHRASES:
        if phrase in lowered:
            score += value
    if _looks_like_named_detail_url(tokens):
        score += 34
    if _extract_locale_token(start_url) and _extract_locale_token(start_url) == _extract_locale_token(url):
        score += 10
    elif _extract_locale_token(start_url) and _extract_locale_token(url):
        score -= 10
    if any(token in _DISCOVERY_PRIORITY_NEGATIVE_TOKENS for token in tokens):
        score -= 28
    score -= min((urlparse(url).path or "").count("/"), 6)
    return score


def _looks_like_named_detail_url(tokens: list[str]) -> bool:
    if len(tokens) < 3 or len(tokens) > 6:
        return False
    if not any(token in _DISCOVERY_NAMED_DETAIL_CONTEXT_TOKENS for token in tokens):
        return False
    name_tokens = [
        token
        for token in tokens
        if token not in _DISCOVERY_NAMED_DETAIL_STOP_TOKENS and token not in _DISCOVERY_PRIORITY_NEGATIVE_TOKENS
    ]
    if len(name_tokens) < 2 or len(name_tokens) > 3:
        return False
    return all(token.isalpha() and len(token) >= 3 for token in name_tokens[:2])


def _extract_locale_token(url: str) -> str:
    path = str(urlparse(str(url or "")).path or "").strip("/")
    if not path:
        return ""
    first = path.split("/", 1)[0].strip().lower()
    if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", first):
        return first
    return ""
