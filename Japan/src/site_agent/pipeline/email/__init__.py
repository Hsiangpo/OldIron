from __future__ import annotations

import asyncio
import re
from html import unescape
from typing import Any, cast
from urllib.parse import urlparse

from ...cache import get_cached_domain_emails, store_domain_emails
from ...config import PipelineSettings
from ...email_rules import (
    EMAIL_DISFAVORED_TOKENS,
    EMAIL_PREFERRED_LOCAL,
    FREE_EMAIL_DOMAINS,
    is_company_domain_email,
)
from ...errors import SnovMaskedEmailError
from ...models import PageContent
from ...snov_client import SnovClient
from ...utils import extract_domain_from_url
from ..heuristics import (
    _build_email_snippet,
    _filter_masked_emails,
    _is_masked_email,
    _sanitize_info,
)
from ..logging import _humanize_exception, _log, drop_snov_prefetch_task

_EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
_EMAIL_OBFUSCATION_AT_RE = re.compile(r"(?i)(?:\(|\[|\{|<)?\s*\bat\b\s*(?:\)|\]|\}|>)")
_EMAIL_OBFUSCATION_DOT_RE = re.compile(
    r"(?i)(?:\(|\[|\{|<)?\s*\bdot\b\s*(?:\)|\]|\}|>)"
)
_CFEMAIL_RE = re.compile(r"data-cfemail=[\"']([0-9a-fA-F]+)[\"']")
_CFEMAIL_HASH_RE = re.compile(r"/cdn-cgi/l/email-protection#([0-9a-fA-F]+)")
_DATA_USER_DOMAIN_RE_1 = re.compile(
    r"data-user=[\"']([^\"']{1,64})[\"'][^>]{0,200}?data-(?:domain|host)=[\"']([^\"']{1,128})[\"']",
    re.IGNORECASE,
)
_DATA_USER_DOMAIN_RE_2 = re.compile(
    r"data-(?:domain|host)=[\"']([^\"']{1,128})[\"'][^>]{0,200}?data-user=[\"']([^\"']{1,64})[\"']",
    re.IGNORECASE,
)
_DATA_EMAIL_RE = re.compile(r"data-email=[\"']([^\"']{1,200})[\"']", re.IGNORECASE)


def _normalize_obfuscated_email_text(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text or ""
    cleaned = unescape(text)
    cleaned = cleaned.replace("＠", "@")
    cleaned = cleaned.replace("．", ".").replace("。", ".").replace("｡", ".")
    cleaned = _EMAIL_OBFUSCATION_AT_RE.sub("@", cleaned)
    cleaned = _EMAIL_OBFUSCATION_DOT_RE.sub(".", cleaned)
    cleaned = re.sub(r"\s+(?:at)\s+", "@", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+(?:dot)\s+", ".", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*@\s*", "@", cleaned)
    cleaned = re.sub(r"\s*\.\s*", ".", cleaned)
    return cleaned


def _decode_cfemail(value: str) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if len(value) < 4 or len(value) % 2 != 0:
        return None
    try:
        key = int(value[:2], 16)
        chars: list[str] = []
        for idx in range(2, len(value), 2):
            chars.append(chr(int(value[idx : idx + 2], 16) ^ key))
        return "".join(chars)
    except Exception:
        return None


def _extract_email_candidates_from_pages(
    visited: dict[str, PageContent],
    website: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    if limit <= 0:
        return candidates
    for page in visited.values():
        text = page.markdown or ""
        raw_html = page.raw_html or ""
        if len(text.strip()) < 80 and isinstance(page.raw_html, str) and page.raw_html.strip():
            text = page.raw_html
        if not text:
            continue

        def _add_candidate(email: str, snippet: str | None, source: str) -> None:
            if not email or _is_masked_email(email):
                return
            key = email.lower()
            if key in seen:
                return
            if not is_company_domain_email(website, email):
                return
            seen.add(key)
            candidates.append(
                {
                    "email": email,
                    "url": page.url,
                    "snippet": snippet,
                    "source": source,
                }
            )

        if raw_html:
            for match in _CFEMAIL_RE.finditer(raw_html):
                decoded = _decode_cfemail(match.group(1))
                if decoded:
                    decoded = _normalize_obfuscated_email_text(decoded).strip()
                    _add_candidate(decoded, decoded, "cfemail")
                    if len(candidates) >= limit:
                        return candidates
            for match in _CFEMAIL_HASH_RE.finditer(raw_html):
                decoded = _decode_cfemail(match.group(1))
                if decoded:
                    decoded = _normalize_obfuscated_email_text(decoded).strip()
                    _add_candidate(decoded, decoded, "cfemail")
                    if len(candidates) >= limit:
                        return candidates
            for match in _DATA_EMAIL_RE.finditer(raw_html):
                value = _normalize_obfuscated_email_text(match.group(1)).strip()
                if _EMAIL_REGEX.search(value):
                    for raw in _EMAIL_REGEX.findall(value):
                        email = raw.strip().strip(".,;:()[]<>")
                        _add_candidate(email, value, "data-email")
                        if len(candidates) >= limit:
                            return candidates
            for match in _DATA_USER_DOMAIN_RE_1.finditer(raw_html):
                user = match.group(1).strip()
                domain = match.group(2).strip()
                candidate = _normalize_obfuscated_email_text(f"{user}@{domain}").strip()
                _add_candidate(candidate, candidate, "data-user")
                if len(candidates) >= limit:
                    return candidates
            for match in _DATA_USER_DOMAIN_RE_2.finditer(raw_html):
                domain = match.group(1).strip()
                user = match.group(2).strip()
                candidate = _normalize_obfuscated_email_text(f"{user}@{domain}").strip()
                _add_candidate(candidate, candidate, "data-user")
                if len(candidates) >= limit:
                    return candidates

        normalized = _normalize_obfuscated_email_text(text)
        scan_texts = [text]
        if normalized and normalized != text:
            scan_texts.append(normalized)
        for scan_text in scan_texts:
            for match in _EMAIL_REGEX.finditer(scan_text):
                raw = match.group(0)
                email = raw.strip().strip(".,;:()[]<>")
                _add_candidate(
                    email,
                    _build_email_snippet(scan_text, match.start(), match.end()),
                    "rule",
                )
                if len(candidates) >= limit:
                    return candidates
    return candidates


async def _fallback_email_from_pages(
    info: dict[str, Any],
    visited: dict[str, PageContent],
    website: str,
    settings: PipelineSettings,
) -> dict[str, Any]:
    candidates = _extract_email_candidates_from_pages(
        visited, website, limit=max(10, settings.email_details_limit)
    )
    if not candidates:
        _log(website, "页面中未发现公司域名邮箱")
        return info
    _log(website, f"页面中发现 {len(candidates)} 个公司域名邮箱，按规则筛选")
    scored = _score_email_candidates(website, candidates)
    existing_emails_value = info.get("emails")
    existing_emails_raw: list[Any] = (
        cast(list[Any], existing_emails_value) if isinstance(existing_emails_value, list) else []
    )
    existing_emails = [e for e in existing_emails_raw if isinstance(e, str) and e.strip()]
    existing_details_value = info.get("email_details")
    existing_details_raw: list[Any] = (
        cast(list[Any], existing_details_value)
        if isinstance(existing_details_value, list)
        else []
    )
    existing_details = [item for item in existing_details_raw if isinstance(item, dict)]
    combined_details: dict[str, dict[str, Any]] = {}
    for item in existing_details:
        email = item.get("email") if isinstance(item, dict) else None
        if isinstance(email, str) and email.strip():
            combined_details[email.strip().lower()] = item
    for item in scored:
        email = item.get("email")
        if not isinstance(email, str) or not email.strip():
            continue
        key = email.strip().lower()
        current = combined_details.get(key)
        if current is None or item.get("score", -10_000) > current.get("score", -10_000):
            combined_details[key] = item
    combined_scored = list(combined_details.values())
    combined_scored.sort(key=lambda x: x.get("score", -10_000), reverse=True)
    deduped = _dedupe_emails(existing_emails + [c["email"] for c in scored if isinstance(c.get("email"), str)])
    if settings.email_max_per_domain > 0:
        deduped = deduped[: settings.email_max_per_domain]
    evidence = info.get("evidence")
    if not isinstance(evidence, dict):
        evidence = {}
        info["evidence"] = evidence
    current_source = evidence.get("email", {}).get("source") if isinstance(evidence.get("email"), dict) else None
    info["emails"] = deduped
    info["email_count"] = len(deduped)
    info["email_details"] = combined_scored[: min(len(combined_scored), settings.email_details_limit)]
    current_email = info.get("email")
    current_email = current_email.strip() if isinstance(current_email, str) and current_email.strip() else None
    best = _pick_best_company_email(website, scored)
    if best and current_source != "rule":
        best_email, best_url = best
        if not current_email or current_email.lower() != best_email.lower():
            info["email"] = best_email
            evidence["email"] = {"url": best_url, "quote": best_email, "source": "rule"}
    if not info.get("email") and deduped and current_source != "rule":
        fallback_email = deduped[0]
        info["email"] = fallback_email
        evidence["email"] = {"url": website, "quote": fallback_email, "source": "rule"}
    return info


def _apply_snov_emails_to_info(
    info: dict[str, Any],
    website: str,
    emails: list[str],
    source_label: str,
    settings: PipelineSettings,
    snov_client: SnovClient | None = None,
) -> bool:
    emails, masked_removed = _filter_masked_emails([email for email in emails if isinstance(email, str)])
    if masked_removed:
        _log(website, f"{source_label} 过滤脱敏邮箱 {masked_removed} 条")
    filtered = [email for email in emails if is_company_domain_email(website, email)]
    if len(filtered) < len(emails):
        _log(website, f"邮箱域名不匹配已过滤 {len(emails) - len(filtered)} 条")
    emails = filtered
    if not emails:
        _log(website, f"{source_label} 未返回邮箱")
        return False
    candidates: list[dict[str, Any]] = []
    for email in emails:
        if isinstance(email, str) and email.strip():
            candidates.append(
                {
                    "email": email.strip(),
                    "url": website,
                    "context": "snov_prefetch",
                    "source": "snov",
                }
            )
    if not candidates:
        _log(website, "未得到有效 Snov 邮箱")
        return False
    scored = _score_email_candidates(website, candidates)
    existing_emails_value = info.get("emails")
    existing_emails_raw: list[Any] = (
        cast(list[Any], existing_emails_value) if isinstance(existing_emails_value, list) else []
    )
    existing_emails = [e for e in existing_emails_raw if isinstance(e, str) and e.strip()]
    existing_details_value = info.get("email_details")
    existing_details_raw: list[Any] = (
        cast(list[Any], existing_details_value)
        if isinstance(existing_details_value, list)
        else []
    )
    existing_details = [item for item in existing_details_raw if isinstance(item, dict)]
    combined_details: dict[str, dict[str, Any]] = {}
    for item in existing_details:
        email = item.get("email") if isinstance(item, dict) else None
        if isinstance(email, str) and email.strip():
            combined_details[email.strip().lower()] = item
    for item in scored:
        email = item.get("email")
        if not isinstance(email, str) or not email.strip():
            continue
        key = email.strip().lower()
        current = combined_details.get(key)
        if current is None or item.get("score", -10_000) > current.get("score", -10_000):
            combined_details[key] = item
    combined_scored = list(combined_details.values())
    combined_scored.sort(key=lambda x: x.get("score", -10_000), reverse=True)
    deduped = _dedupe_emails(existing_emails + [c["email"] for c in scored if isinstance(c.get("email"), str)])
    if settings.email_max_per_domain > 0:
        deduped = deduped[: settings.email_max_per_domain]
    evidence = info.get("evidence")
    if not isinstance(evidence, dict):
        evidence = {}
        info["evidence"] = evidence
    current_source = evidence.get("email", {}).get("source") if isinstance(evidence.get("email"), dict) else None
    info["emails"] = deduped
    info["email_count"] = len(deduped)
    info["email_details"] = combined_scored[: min(len(combined_scored), settings.email_details_limit)]
    current = info.get("email")
    current_email = current.strip() if isinstance(current, str) and current.strip() else None
    best = _pick_best_company_email(website, scored)
    if best and current_source != "rule":
        best_email, best_url = best
        if not current_email or current_email.lower() != best_email.lower():
            info["email"] = best_email
            evidence["email"] = {"url": best_url, "quote": best_email, "source": "snov"}
    elif current_email and current_source != "rule" and not _is_company_like_email(website, current_email, website):
        info["email"] = None
        evidence["email"] = {"url": None, "quote": None, "source": "snov"}
    if not info.get("email") and deduped and current_source != "rule":
        fallback_email = deduped[0]
        info["email"] = fallback_email
        evidence["email"] = {"url": website, "quote": fallback_email, "source": "snov"}
        _log(website, f"{source_label} Snov 兜底邮箱：{fallback_email}")
    return bool(info.get("email"))


async def _prefetch_snov_emails(
    website: str,
    snov_client: SnovClient | None,
    settings: PipelineSettings,
    *,
    max_wait_seconds: int = 30,
) -> list[str]:
    if snov_client is None:
        _log(website, "Snov 未配置，跳过邮箱预检")
        return []
    domain = extract_domain_from_url(website)
    if not domain:
        _log(website, "无法解析官网域名，跳过邮箱预检")
        return []
    try:
        cached = get_cached_domain_emails(domain)
    except Exception as exc:
        _log(website, f"Domain email cache read failed, ignoring cache: {_humanize_exception(exc)}")
        cached = None
    if cached:
        _log(website, f"邮箱缓存命中 {len(cached)} 个")
        return cached
    extension_only = bool(getattr(snov_client, "extension_only", False))
    use_extension = bool(
        (
            getattr(snov_client, "extension_selector", None)
            and getattr(snov_client, "extension_token", None)
        )
        or getattr(snov_client, "extension_cdp_port", None)
        or extension_only
    )
    if use_extension:
        mode = "（仅扩展）" if extension_only else ""
        _log(website, f"Snov 扩展接口{mode}预检邮箱：{website}")
    else:
        _log(website, f"Snov 扩展 预检域名邮箱：{domain}")
    try:
        emails = await asyncio.to_thread(
            snov_client.get_domain_emails,
            domain,
            page_url=website,
            max_wait_seconds=max_wait_seconds,
        )
    except Exception as exc:
        _log(website, f"Snov 扩展 预检失败：{_humanize_exception(exc)}")
        return []
    last_source = getattr(snov_client, "last_source", None)
    if last_source == "extension_missing":
        _log(website, "Snov 扩展未就绪（缺少 cookies 或 CDP）")
    source_label = "Snov extension" if last_source in {"extension", "extension_missing"} else "Snov 扩展"
    if not emails:
        _log(website, f"{source_label} 预检未返回邮箱")
        return []
    filtered = [email for email in emails if isinstance(email, str)]
    filtered, masked_removed = _filter_masked_emails(filtered)
    if masked_removed:
        _log(website, f"{source_label} 过滤脱敏邮箱 {masked_removed} 条")
    filtered = [email for email in filtered if is_company_domain_email(website, email)]
    if len(filtered) < len(emails):
        _log(website, f"邮箱域名不匹配已过滤 {len(emails) - len(filtered)} 条")
    emails = filtered
    if not emails and masked_removed:
        _log(website, f"{source_label} 返回脱敏邮箱 {masked_removed} 条，进入延迟重试队列（请检查扩展登录/点数）")
        raise SnovMaskedEmailError("snov_masked")
    if not emails:
        _log(website, "邮箱全部与官网域名不匹配，忽略该批结果")
        return []
    deduped = _dedupe_emails([email.strip() for email in emails if isinstance(email, str)])
    if settings.email_max_per_domain > 0:
        deduped = deduped[: settings.email_max_per_domain]
    store_domain_emails(domain, deduped)
    _log(website, f"{source_label} 预检返回邮箱 {len(deduped)} 个")
    return deduped


async def _apply_email_policy(
    info: dict[str, Any] | None,
    visited: dict[str, PageContent],
    website: str,
    memory: dict[str, Any],
    settings: PipelineSettings,
    snov_client: SnovClient | None = None,
) -> dict[str, Any] | None:
    if not isinstance(info, dict):
        return info
    info = _sanitize_info(info)
    if bool(getattr(settings, "skip_email", False)):
        return info
    info.setdefault("email", None)
    info.setdefault("emails", [])
    info.setdefault("email_count", 0)
    if visited:
        info = await _fallback_email_from_pages(info, visited, website, settings)
    prefetched = memory.get("snov_prefetched_emails") if isinstance(memory, dict) else None
    prefetch_task = memory.get("snov_prefetch_task") if isinstance(memory, dict) else None
    prefetch_task_value: asyncio.Task[list[str]] | None = (
        cast(asyncio.Task[list[str]], prefetch_task)
        if isinstance(prefetch_task, asyncio.Task)
        else None
    )
    prefetch_consumed = bool(memory.get("snov_prefetch_consumed")) if isinstance(memory, dict) else False
    prefetch_pending = False
    if isinstance(prefetched, list) and prefetched:
        _log(website, f"Snov 预取已返回 {len(prefetched)} 个邮箱，跳过重复查询")
        _apply_snov_emails_to_info(info, website, prefetched, "Snov 预取", settings, snov_client)
    elif isinstance(prefetch_task_value, asyncio.Task) and not prefetch_consumed:
        if prefetch_task_value.done():
            try:
                prefetched = prefetch_task_value.result()
            except SnovMaskedEmailError:
                _log(website, "Snov 预取返回脱敏邮箱，已忽略")
                prefetched = []
            except Exception as exc:
                _log(website, f"Snov 预取失败：{_humanize_exception(exc)}")
                prefetched = []
            prefetch_consumed = True
            if isinstance(prefetched, list) and prefetched:
                memory["snov_prefetched_emails"] = prefetched
                _apply_snov_emails_to_info(info, website, prefetched, "Snov 预取", settings, snov_client)
        else:
            prefetch_pending = True
    if prefetch_consumed:
        memory["snov_prefetch_consumed"] = True
        drop_snov_prefetch_task(website)
    if prefetch_pending:
        memory["snov_prefetch_pending"] = True
    if not info.get("email"):
        _log(website, "未找到可用邮箱")
    return info


def _is_company_like_email(website: str, email: str, source_url: str | None) -> bool:
    return _email_score(website, email, source_url) >= 10


def _pick_best_company_email(
    website: str, candidates: list[dict[str, Any]]
) -> tuple[str, str] | None:
    best: tuple[str, str] | None = None
    best_score = -10_000
    for item in candidates:
        email = item.get("email")
        url = item.get("url")
        score = item.get("score")
        if not (
            isinstance(email, str)
            and email.strip()
            and isinstance(url, str)
            and url.strip()
        ):
            continue
        if not isinstance(score, int):
            continue
        if score > best_score:
            best_score = score
            best = (email, url)
    if best and best_score >= 10:
        return best
    return None


def _score_email_candidates(
    website: str, candidates: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for item in candidates:
        email = item.get("email")
        url = item.get("url")
        if not (
            isinstance(email, str)
            and email.strip()
            and isinstance(url, str)
            and url.strip()
        ):
            continue
        base_score = _email_score(website, email, url)
        source = item.get("source") if isinstance(item.get("source"), str) else "page"
        if source == "rule":
            base_score += 80
        elif source == "snov":
            base_score += 40
        scored.append(
            {
                "email": email.strip(),
                "url": url.strip(),
                "context": item.get("context") if isinstance(item.get("context"), str) else "",
                "source": source,
                "score": base_score,
            }
        )
    scored.sort(key=lambda x: x.get("score", -10_000), reverse=True)
    return scored


def _dedupe_emails(emails: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for email in emails:
        if not isinstance(email, str):
            continue
        value = email.strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _email_score(website: str, email: str, source_url: str | None) -> int:
    addr = (email or "").strip().lower()
    if "@" not in addr:
        return -10_000
    local, domain = addr.rsplit("@", 1)
    domain = domain.strip().strip(".")
    if not domain:
        return -10_000
    if not is_company_domain_email(website, addr):
        return -10_000
    score = 0
    host = (urlparse(website).hostname or "").lower()
    host = host[4:] if host.startswith("www.") else host
    if host:
        if domain == host or host.endswith("." + domain) or domain.endswith("." + host):
            score += 60
        elif host.split(":")[0].endswith(domain):
            score += 40
    if domain in FREE_EMAIL_DOMAINS:
        score -= 120
    if local in EMAIL_PREFERRED_LOCAL:
        score += 20
    for token in EMAIL_DISFAVORED_TOKENS:
        if token in local:
            score -= 80
            break
    path = (source_url or "").lower()
    if any(x in path for x in ("/contact", "/inquiry", "toiawase", "contactus", "/support")):
        score += 10
    if any(x in path for x in ("/privacy", "/terms", "/legal", "/policy", "vulnerability", "security")):
        score -= 20
    return score

