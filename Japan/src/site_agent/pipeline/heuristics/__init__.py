from __future__ import annotations

import re
import unicodedata
from typing import Any
from urllib.parse import urlparse

from ...constants import Patterns
from ...models import PageContent
from ...utils import url_depth
from ..fields import _missing_fields, _normalize_required_fields
from ..selection import _select_pages_for_llm
from ..crawl import _html_to_text

from .._mod2.labeled_html import (  # noqa: F401
    _extract_labeled_from_html,
    _extract_labeled_values_from_html,
)


def _build_email_snippet(text: str, start: int, end: int, *, window: int = 40) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    snippet = text[left:right].replace("\n", " ").replace("\r", " ")
    snippet = re.sub(r"\s+", " ", snippet).strip()
    if len(snippet) > 140:
        snippet = snippet[:140] + "…"
    return snippet


_REP_ANCHOR_TOKENS = (
    "代表",
    "社長",
    "会長",
    "役員",
    "top-message",
    "topmessage",
    "greeting",
    "president",
    "ceo",
    "chairman",
)


def _build_rep_snippet(text: str, rep: str) -> str | None:
    if not text or not rep:
        return None
    rep = rep.strip()
    if not rep:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    best_line = None
    best_score = -1
    tokens = list(Patterns.REPRESENTATIVE_TITLES) + list(_REP_ANCHOR_TOKENS)
    rep_norm = _normalize_rep_text(rep)
    for line in lines:
        line_norm = _normalize_rep_text(line)
        if rep_norm:
            if rep_norm not in line_norm:
                continue
        elif rep not in line:
            continue
        score = 1
        if any(token in line for token in tokens if token):
            score += 2
        score -= max(0, len(line) - 120) // 40
        if score > best_score:
            best_score = score
            best_line = line
    if best_line:
        return best_line[:140]
    idx = text.find(rep)
    if idx >= 0:
        return _build_email_snippet(text, idx, idx + len(rep))
    rep_plain = _normalize_rep_text(rep)
    text_plain = _normalize_rep_text(text)
    if rep_plain and text_plain and rep_plain in text_plain:
        return rep
    return None


def _backfill_rep_evidence(
    info: dict[str, Any] | None, visited: dict[str, PageContent]
) -> dict[str, Any] | None:
    if not isinstance(info, dict):
        return info
    rep = info.get("representative")
    if not isinstance(rep, str) or not rep.strip():
        return info
    evidence = info.get("evidence")
    if not isinstance(evidence, dict):
        evidence = {}
        info["evidence"] = evidence
    rep_ev = evidence.get("representative")
    if isinstance(rep_ev, dict) and rep_ev.get("url") and rep_ev.get("quote"):
        return info
    pages = _select_pages_for_llm(visited, max_pages=8, missing_fields=["representative"])
    if not pages:
        return info
    rep_clean = rep.strip()
    for page in pages:
        for text in (_html_to_text(page.raw_html or ""), page.fit_markdown or "", page.markdown or ""):
            if not text:
                continue
            snippet = _build_rep_snippet(text, rep_clean)
            if snippet:
                evidence["representative"] = {"url": page.url, "quote": snippet}
                return info
    return info


def _find_labeled_value(text: str, labels: list[str]) -> tuple[str | None, str | None]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        for label in labels:
            if label not in line:
                continue
            match = re.search(
                rf"{re.escape(label)}[\s\u3000]*[:：]?\s*([^\n\r]{{2,60}})",
                line,
            )
            if match:
                value = match.group(1).strip()
                if value and label not in value:
                    return value, line[:120]
            if idx + 1 < len(lines):
                next_line = lines[idx + 1].strip()
                if next_line and label not in next_line and len(next_line) <= 60:
                    return next_line, line[:120]
    return None, None


_COMPANY_NAME_RE = re.compile(
    r"([A-Za-z0-9&\-\u3000ぁ-んァ-ン一-龥가-힣]{2,40}"
    r"(?:株式会社|有限会社|合同会社|合資会社|有限公司|股份有限公司|集团有限公司|集团|주식회사|㈜|Inc\.|Corp\.|Ltd\.|LLC|Co\.,? ?Ltd\.?))"
)
_PHONE_RE = re.compile(
    r"(?:\+81[-\s()]*(?:0[-\s()]*)?[1-9]\d{0,3}[-\s()]*\d{1,4}[-\s()]*\d{3,4}|"
    r"0[1-9]\d{0,3}[-\s()]*\d{1,4}[-\s()]*\d{3,4})"
)


def _find_company_like_name(text: str) -> tuple[str | None, str | None]:
    match = _COMPANY_NAME_RE.search(text)
    if not match:
        return None, None
    value = match.group(1).strip()
    return value, value


_MISSING_REPRESENTATIVE_TEXT = "未找到代表人"
_MISSING_PHONE_TEXT = "未找到座机电话"

_MASKED_EMAIL_TOKENS = (
    "*",
    "?",
    "•",
    "●",
    "＊",
    "□",
    "■",
    "…",
    "⋯",
)
_REP_TITLE_PREFIX_RE = re.compile(
    r"^(?:代表取締役(?:社長|会長)?|代表執行役(?:社長)?|上席執行理事|執行理事|執行役員社長|執行役員|取締役(?:社長|会長)?|代表理事|理事長|理事|院長|校長|所長|代表者|代表|社長|会長|CEO|President|Chairman|Owner|"
    r"Managing Director|Representative Director|Chief Executive Officer|Chief Executive)\s*[:：\-–]?\s*",
    re.I,
)
_REP_TITLE_SUFFIX_RE = re.compile(
    r"\s*(?:代表取締役(?:社長|会長)?|代表執行役(?:社長)?|上席執行理事|執行理事|執行役員社長|執行役員|取締役(?:社長|会長)?|代表理事|理事長|理事|院長|校長|所長|社長|会長|CEO|President|Chairman)\s*$",
    re.I,
)
_REP_GREETING_ONLY_RE = re.compile(
    r"^(?:代表者|代表|社長|会長)?(?:ご)?(?:挨拶|あいさつ|(?:トップ)?メッセージ)$"
)
_REP_GREETING_PREFIX_RE = re.compile(
    r"^(?:代表者|代表|社長|会長)?(?:ご)?(?:挨拶|あいさつ|(?:トップ)?メッセージ)\s*[:：\-–]?\s*"
)
_REP_GREETING_SUFFIX_RE = re.compile(
    r"(?:\s|[\-–—・/])?(?:代表者|代表|社長|会長)?(?:ご)?(?:挨拶|あいさつ|(?:トップ)?メッセージ)\s*$"
)
_REP_INVALID_TOKENS = [
    "株式会社",
    "有限会社",
    "合同会社",
    "(株)",
    "（株）",
    "㈱",
    "co.",
    "ltd",
    "inc",
    "corp",
    "company",
    "group",
    "本店",
    "支店",
    "営業所",
    "事務所",
    "工場",
    "センター",
    "お問い合わせ",
    "問合せ",
    "ご連絡",
    "担当",
    "窓口",
    "受付",
    "連絡先",
    "電話",
    "tel",
    "fax",
    "メール",
    "email",
    "e-mail",
    "homepage",
    "home page",
    "hp",
    "official",
    "profile",
    "company info",
    "overview",
    "note",
    "blog",
    "公式サイト",
    "ホームページ",
    "詳細はこちら",
    "詳しくはこちら",
    "はこちら",
    "趣味",
    "特技",
    "紹介",
    "者名",
    "プロフィール",
    "メッセージ",
    "ごあいさつ",
    "あいさつ",
    "会社案内",
    "会社概要",
    "企業概要",
    "事業内容",
    "沿革",
    "ブログ",
    "お知らせ",
    "ニュース",
    "ケーススタディ",
    "インタビュー",
    "就任",
    "トップ",
    "弁護士",
    "役員報酬",
    "開催実績",
    "上席執行理事",
    "執行理事",
    "理事",
    "参考",
]
_REP_INVALID_RE = re.compile("|".join(re.escape(token) for token in _REP_INVALID_TOKENS), re.I)
_REP_HAS_CJK_RE = re.compile(
    r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF"
    r"\U00020000-\U0002A6DF\U0002A700-\U0002B73F"
    r"\U0002B740-\U0002B81F\U0002B820-\U0002CEAF\U0002CEB0-\U0002EBEF]"
)
_REP_HAS_LATIN_RE = re.compile(r"[A-Za-z]")
_REP_HAS_DIGIT_RE = re.compile(r"\d")
_REP_URL_EMAIL_RE = re.compile(r"@|https?://|www\.", re.I)
_REP_BAD_CHARS_RE = re.compile(r"[\\/|{}<>=;]|[。､、※■□◆◇★☆●◎○△▽]")
_REP_ELLIPSIS_RE = re.compile(r"(?:\.\.\.|…|⋯)")


_REP_STRIP_PREFIX_RE = re.compile(r"^[\s\u3000\-–—:：|｜/／・･]+")
_REP_STRIP_SUFFIX_RE = re.compile(r"[\s\u3000\-–—:：|｜/／・･]+$")
_REP_TITLE_MID_PREFIX_RE = re.compile(
    r"^(?:兼|副|共同)?\s*(?:CEO|COO|CFO|CTO|President|Chairman|Owner|Managing Director)\s*[:：\-–]?\s*",
    re.I,
)
_REP_TRAILING_NOISE_RE = re.compile(
    r"(?:HP|homepage|home page|official|profile|company info|overview|note|blog|公式サイト|ホームページ|詳細はこちら|詳しくはこちら|はこちら)$",
    re.I,
)

_REP_LATIN_NAME_RE = re.compile(
    r"^[A-Za-z][A-Za-z'\-.]{0,29}(?:\s+[A-Za-z][A-Za-z'\-.]{0,29}){1,3}$"
)
_REP_LATIN_STOPWORDS = {
    "about",
    "blog",
    "company",
    "contact",
    "display",
    "element",
    "flex",
    "homepage",
    "info",
    "news",
    "note",
    "official",
    "overview",
    "profile",
    "service",
}
_REP_TRAILING_SENTENCE_RE = re.compile(r"(?:からの|について|はこちら|です|ます)$")
_REP_JP_TITLE_SPLIT_RE = re.compile(
    r"\s+(?:代表取締役(?:社長|会長)?|代表執行役(?:社長)?|執行役員社長|執行役員|取締役(?:社長|会長)?|社長|会長|理事長|院長|校長|所長)\s+"
)
_REP_NEWSLIKE_END_RE = re.compile(r"(?:に就任|を就任|のお知らせ|掲載|公開|更新)$")

def _is_masked_email(value: str | None) -> bool:
    if not isinstance(value, str) or not value:
        return False
    return any(token in value for token in _MASKED_EMAIL_TOKENS)


def _filter_masked_emails(emails: list[str]) -> tuple[list[str], int]:
    cleaned: list[str] = []
    removed = 0
    for email in emails:
        if _is_masked_email(email):
            removed += 1
            continue
        cleaned.append(email)
    return cleaned, removed


def _is_valid_representative_name(text: str) -> bool:
    if not text:
        return False
    if len(text) < 2 or len(text) > 40:
        return False
    compact = re.sub(r"[\s　]+", "", text)
    if _REP_GREETING_ONLY_RE.fullmatch(compact):
        return False
    if _REP_URL_EMAIL_RE.search(text):
        return False
    if _REP_BAD_CHARS_RE.search(text):
        return False
    if _REP_ELLIPSIS_RE.search(text):
        return False
    if _REP_HAS_DIGIT_RE.search(text):
        return False
    if _REP_INVALID_RE.search(text):
        return False
    has_cjk = bool(_REP_HAS_CJK_RE.search(text))
    has_latin = bool(_REP_HAS_LATIN_RE.search(text))
    if has_cjk and has_latin:
        return False
    if has_latin:
        normalized = re.sub(r"[\s　]+", " ", text).strip()
        if not _REP_LATIN_NAME_RE.fullmatch(normalized):
            return False
        parts = [p.strip(".-'") for p in normalized.split(" ") if p.strip(".-'")]
        if len(parts) < 2 or len(parts) > 4:
            return False
        lowered = [p.lower() for p in parts]
        if any(token in _REP_LATIN_STOPWORDS for token in lowered):
            return False
        if not all(p.isupper() or p[0].isupper() for p in parts):
            return False
    if has_cjk:
        if len(compact) < 3 or len(compact) > 10:
            return False
        if _REP_TRAILING_SENTENCE_RE.search(compact):
            return False
        if _REP_NEWSLIKE_END_RE.search(compact):
            return False
        return True
    if has_latin:
        return True
    return False


def _normalize_rep_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"[\s\u3000]+", "", normalized)
    normalized = normalized.replace("・", "").replace("·", "")
    return normalized


def _clean_representative_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = unicodedata.normalize("NFKC", value.strip())
    if not text:
        return None
    if text == _MISSING_REPRESENTATIVE_TEXT:
        return None
    text = re.sub(r"[\s　]+", " ", text).strip()
    text = re.sub(r"[*_`]+", "", text).strip()
    if not text:
        return None
    previous = None
    while previous != text:
        previous = text
        text = _REP_TITLE_PREFIX_RE.sub("", text).strip()
        text = _REP_TITLE_MID_PREFIX_RE.sub("", text).strip()
        text = _REP_GREETING_PREFIX_RE.sub("", text).strip()
        text = _REP_STRIP_PREFIX_RE.sub("", text).strip()
    text = re.sub(
        r"[（(][^）)]{0,30}(?:社長|代表|取締役|CEO|President|Chairman)[^）)]{0,30}[）)]",
        "",
        text,
    ).strip()
    text = _REP_TITLE_SUFFIX_RE.sub("", text).strip()
    text = _REP_GREETING_SUFFIX_RE.sub("", text).strip()
    text = _REP_TRAILING_NOISE_RE.sub("", text).strip()
    text = _REP_TRAILING_SENTENCE_RE.sub("", text).strip()
    # Drop role tail when multiple names are glued in one line.
    text = _REP_JP_TITLE_SPLIT_RE.split(text, 1)[0].strip()
    text = _REP_STRIP_SUFFIX_RE.sub("", text).strip()
    text = re.sub(r"[\s　]+", " ", text).strip()
    if not text:
        return None
    if not _is_valid_representative_name(text):
        return None
    return text


def _is_rep_evidence_strong(rep: str, evidence: dict[str, Any]) -> bool:
    if not rep or not isinstance(evidence, dict):
        return False
    rep_ev = evidence.get("representative")
    if not isinstance(rep_ev, dict):
        return False
    quote = rep_ev.get("quote")
    url = rep_ev.get("url")
    if not isinstance(url, str) or not url.strip():
        return False
    if not isinstance(quote, str) or not quote.strip():
        return False
    rep_norm = _normalize_rep_text(rep)
    quote_norm = _normalize_rep_text(quote)
    if rep_norm and quote_norm and rep_norm in quote_norm:
        return True
    return False


def _sanitize_info(info: dict[str, Any]) -> dict[str, Any]:
    company = info.get("company_name")
    company_clean = _clean_company_value(company)
    if isinstance(company_clean, str) and company_clean.strip():
        info["company_name"] = company_clean
    rep = info.get("representative")
    rep_clean = _clean_representative_name(rep)
    evidence_obj = info.get("evidence")
    evidence: dict[str, Any]
    if isinstance(evidence_obj, dict):
        evidence = evidence_obj
    else:
        evidence = {}
        info["evidence"] = evidence
    if rep_clean is None:
        info["representative"] = None
        rep_evidence = evidence.get("representative")
        if isinstance(rep_evidence, dict):
            rep_evidence["quote"] = None
        else:
            evidence["representative"] = {"url": None, "quote": None}
    else:
        if _is_rep_evidence_strong(rep_clean, evidence):
            info["representative"] = rep_clean
        else:
            info["representative"] = None
            rep_evidence = evidence.get("representative")
            if isinstance(rep_evidence, dict):
                rep_evidence["quote"] = None
    email = info.get("email")
    if _is_masked_email(email):
        info["email"] = None
        email_ev = evidence.get("email")
        if isinstance(email_ev, dict):
            email_ev["quote"] = None
        else:
            evidence["email"] = {"url": None, "quote": None}
    phone = info.get("phone")
    phone_clean = _normalize_phone_value(phone)
    if phone_clean is None:
        info["phone"] = None
        phone_ev = evidence.get("phone")
        if isinstance(phone_ev, dict):
            phone_ev["quote"] = None
        else:
            evidence["phone"] = {"url": None, "quote": None}
    else:
        info["phone"] = phone_clean
    capital = info.get("capital")
    capital_clean = _clean_capital_value(capital)
    if capital_clean is None:
        info["capital"] = None
        capital_ev = evidence.get("capital")
        if isinstance(capital_ev, dict):
            capital_ev["quote"] = None
        else:
            evidence["capital"] = {"url": None, "quote": None}
    else:
        info["capital"] = capital_clean
    employees = info.get("employees")
    employees_clean = _clean_employees_value(employees)
    if employees_clean is None:
        info["employees"] = None
        employees_ev = evidence.get("employees")
        if isinstance(employees_ev, dict):
            employees_ev["quote"] = None
        else:
            evidence["employees"] = {"url": None, "quote": None}
    else:
        info["employees"] = employees_clean
    emails = info.get("emails")
    if isinstance(emails, list):
        filtered, _ = _filter_masked_emails([e for e in emails if isinstance(e, str) and e.strip()])
        info["emails"] = filtered
        info["email_count"] = len(filtered)
        details = info.get("email_details")
        if isinstance(details, list):
            cleaned_details = []
            for item in details:
                if not isinstance(item, dict):
                    continue
                addr = item.get("email")
                if isinstance(addr, str) and addr.strip() and not _is_masked_email(addr):
                    cleaned_details.append(item)
            info["email_details"] = cleaned_details
    return info


def _merge_info(
    base: dict[str, Any] | None, incoming: dict[str, Any] | None
) -> dict[str, Any]:
    if not isinstance(base, dict):
        base = {}
    if not isinstance(incoming, dict):
        return base
    merged = dict(base)
    incoming_ev = incoming.get("evidence")
    base_ev = merged.get("evidence") if isinstance(merged.get("evidence"), dict) else {}
    if isinstance(incoming_ev, dict):
        for key, value in incoming_ev.items():
            existing = base_ev.get(key)
            if not isinstance(existing, dict) or not existing.get("quote"):
                base_ev[key] = value
    if base_ev:
        merged["evidence"] = base_ev
    for key in ("company_name", "representative", "capital", "employees", "phone", "email"):
        value = incoming.get(key)
        if isinstance(value, str) and value.strip():
            current = merged.get(key)
            if not isinstance(current, str) or not current.strip():
                merged[key] = value.strip()
            elif key == "representative" and current.strip() == _MISSING_REPRESENTATIVE_TEXT:
                merged[key] = value.strip()
            elif key == "phone" and current.strip() == _MISSING_PHONE_TEXT:
                merged[key] = value.strip()
    if isinstance(incoming.get("emails"), list) and not merged.get("emails"):
        merged["emails"] = incoming.get("emails")
    if isinstance(incoming.get("email_count"), int) and not merged.get("email_count"):
        merged["email_count"] = incoming.get("email_count")
    notes = incoming.get("notes")
    if isinstance(notes, str) and notes.strip():
        existing_notes = merged.get("notes")
        if isinstance(existing_notes, str) and existing_notes.strip():
            if notes.strip() not in existing_notes:
                merged["notes"] = f"{existing_notes.strip()};{notes.strip()}"
        else:
            merged["notes"] = notes.strip()
    return merged


def _apply_heuristic_extraction(
    info: dict[str, Any] | None,
    visited: dict[str, PageContent],
    *,
    required_fields: list[str] | None = None,
    company_labels: list[str] | None = None,
    rep_labels: list[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(info, dict):
        info = {}
    evidence = info.get("evidence")
    if not isinstance(evidence, dict):
        info["evidence"] = {}
    required = _normalize_required_fields(required_fields)
    missing = _missing_fields(info, required_fields=required)
    if not visited:
        return info

    def _iter_pages() -> list[PageContent]:
        pages = [p for p in visited.values() if isinstance(p, PageContent)]
        pages = [p for p in pages if p.success and isinstance(p.url, str) and p.url]
        pages.sort(key=lambda p: (url_depth(p.url), len(p.url)))
        return pages

    def _extract_rep_candidates(text: str, labels: list[str]) -> list[str]:
        if not text:
            return []
        candidates: list[str] = []
        for label in labels:
            if not label:
                continue
            pattern = rf"{re.escape(label)}[\s\u3000:：\-–]*([^\n\r<]{{2,60}})"
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = match.group(1).strip()
                if not value:
                    continue
                value = re.split(r"[｜|/／,，;；]", value)[0].strip()
                value = re.sub(r"(?:様|氏|さん)$", "", value).strip()
                if value:
                    candidates.append(value)
        return candidates

    def _extract_rep_candidates_next_line(text: str, labels: list[str]) -> list[str]:
        if not text:
            return []
        candidates: list[str] = []
        lines = [line.strip() for line in text.splitlines()]
        if not lines:
            return []
        for idx, line in enumerate(lines):
            if not line:
                continue
            for label in labels:
                if not label or label not in line:
                    continue
                after = line.split(label, 1)[1]
                if after and after.strip(" \t:：-–"):
                    break
                next_idx = idx + 1
                while next_idx < len(lines) and not lines[next_idx]:
                    next_idx += 1
                if next_idx < len(lines):
                    candidates.append(lines[next_idx])
                break
        return candidates

    rep_found = False
    if "representative" in missing:
        labels = rep_labels[:] if isinstance(rep_labels, list) and rep_labels else []
        if not labels:
            labels = list(Patterns.REPRESENTATIVE_TITLES) + [
                "代表者名",
                "代表取締役社長",
                "代表取締役会長",
                "代表取締役",
                "代表社員",
                "代表理事",
                "代表者",
                "代表者氏名",
                "代表取締役氏名",
                "社長名",
                "社長",
            ]
        for page in _iter_pages():
            html = page.raw_html or ""
            md = page.fit_markdown or page.markdown or ""
            candidates: list[str] = []
            if html:
                candidates.extend(_extract_labeled_values_from_html(html, labels))
                html_text = _html_to_text(html)
                if html_text:
                    candidates.extend(_extract_rep_candidates(html_text, labels))
                    candidates.extend(_extract_rep_candidates_next_line(html_text, labels))
            if md:
                candidates.extend(_extract_rep_candidates(md, labels))
                candidates.extend(_extract_rep_candidates_next_line(md, labels))
                labeled, _ = _find_labeled_value(md, labels)
                if labeled:
                    candidates.append(labeled)
            for candidate in candidates:
                rep_clean = _clean_representative_name(candidate)
                if rep_clean:
                    info["representative"] = rep_clean
                    evidence = info.get("evidence")
                    if isinstance(evidence, dict):
                        evidence["representative"] = {"url": page.url, "quote": candidate[:80]}
                    rep_found = True
                    break
            if rep_found:
                break

    capital_value = info.get("capital") if isinstance(info, dict) else None
    if not (isinstance(capital_value, str) and capital_value.strip()):
        for page in _iter_pages():
            html = page.raw_html or ""
            md = page.fit_markdown or page.markdown or ""
            extracted = None
            if html:
                extracted = _extract_labeled_from_html(html, _CAPITAL_LABELS)
            if not extracted and md:
                labeled, _ = _find_labeled_value(md, _CAPITAL_LABELS)
                extracted = labeled
            cleaned = _clean_capital_value(extracted)
            if cleaned:
                info["capital"] = cleaned
                evidence = info.get("evidence")
                if isinstance(evidence, dict):
                    evidence["capital"] = {"url": page.url, "quote": str(extracted)[:80]}
                break

    employees_value = info.get("employees") if isinstance(info, dict) else None
    if not (isinstance(employees_value, str) and employees_value.strip()):
        for page in _iter_pages():
            html = page.raw_html or ""
            md = page.fit_markdown or page.markdown or ""
            extracted = None
            if html:
                extracted = _extract_labeled_from_html(html, _EMPLOYEE_LABELS)
            if not extracted and md:
                labeled, _ = _find_labeled_value(md, _EMPLOYEE_LABELS)
                extracted = labeled
            cleaned = _clean_employees_value(extracted)
            if cleaned:
                info["employees"] = cleaned
                evidence = info.get("evidence")
                if isinstance(evidence, dict):
                    evidence["employees"] = {"url": page.url, "quote": str(extracted)[:80]}
                break

    return _sanitize_info(info)


def _clean_capital_value(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return None
    if len(cleaned) > 80:
        return None
    if not re.search(r"\d", cleaned):
        return None
    return cleaned


def _clean_employees_value(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return None
    if len(cleaned) > 80:
        return None
    if not re.search(r"\d", cleaned):
        return None
    return cleaned


def _normalize_phone_value(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    match = _PHONE_RE.search(value)
    if not match:
        return None
    raw = match.group(0).strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("81") and len(digits) in (11, 12):
        digits = "0" + digits[2:]
    if not digits.startswith("0"):
        return None
    if digits.startswith("00"):
        return None
    if len(digits) not in (10, 11):
        return None
    if len(set(digits)) == 1:
        return None
    return raw if re.search(r"\D", raw) else digits


def _clean_company_value(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return None
    if cleaned in ("|", "-", "/", "\\", ".", ",", "Home", "Top"):
        return None
    match = _COMPANY_NAME_RE.search(cleaned)
    if match:
        return match.group(1).strip()
    cleaned = re.sub(
        r"^(?:会社名|社名|商号|法人名|企業名|公司名称|企业名称|会社概要)[:：\\s]*",
        "",
        cleaned,
    )
    cutoff_tokens = [
        "設立",
        "資本金",
        "所在地",
        "住所",
        "TEL",
        "電話",
        "代表者",
        "代表取締役",
        "社長",
        "董事长",
        "总经理",
        "URL",
        "ホームページ",
        "事業内容",
        "営業時間",
        "受付時間",
    ]
    for token in cutoff_tokens:
        idx = cleaned.find(token)
        if idx > 0:
            cleaned = cleaned[:idx].strip()
            break
    return cleaned or None


_COMPANY_LABELS = [
    "会社名",
    "会社概要",
    "会社情報",
    "会社紹介",
    "社名",
    "商号",
    "法人名",
    "企業名",
    "企業情報",
    "公司名称",
    "企业名称",
    "公司名",
    "公司简介",
    "公司介绍",
    "公司名稱",
    "회사명",
    "법인명",
    "회사 이름",
    "Company Name",
    "Corporate Name",
    "Legal Name",
    "Company",
]
_REP_LABELS = [
    "代表者",
    "代表取締役",
    "代表取締役社長",
    "代表取締役会長",
    "取締役社長",
    "取締役会長",
    "代表",
    "社長",
    "会長",
    "代表者名",
    "理事長",
    "院長",
    "校長",
    "所長",
    "執行役員",
    "法人代表",
    "法定代表人",
    "代表人",
    "董事长",
    "总经理",
    "总裁",
    "대표이사",
    "대표",
    "사장",
    "CEO",
    "President",
    "Chairman",
    "Managing Director",
]
_PHONE_LABELS = [
    "電話番号",
    "電話",
    "TEL",
    "Tel",
    "TEL.",
    "代表電話",
    "連絡先",
    "お問い合わせ",
    "TEL/FAX",
    "TEL・FAX",
    "電話/FAX",
    "電話／FAX",
]
_CAPITAL_LABELS = [
    "資本金",
    "資本",
    "资本金",
    "注册资金",
    "注册资本",
    "Capital",
    "Paid-in Capital",
    "Paid in capital",
]
_EMPLOYEE_LABELS = [
    "従業員数",
    "従業員",
    "社員数",
    "スタッフ数",
    "Employee",
    "Employees",
    "Staff",
    "员工人数",
    "员工数",
    "公司人数",
]


def _labels_for_country(country_code: str | None) -> tuple[list[str], list[str]]:
    code = (country_code or "").lower()
    base_company = list(_COMPANY_LABELS)
    base_rep = list(_REP_LABELS)
    if code in ("jp", "jpn", "japan"):
        return base_company, base_rep
    if code in ("kr", "kor", "korea", "south_korea"):
        company = base_company + ["회사명", "법인명", "회사 이름"]
        rep = base_rep + ["대표이사", "대표", "사장"]
        return company, rep
    if code in ("cn", "china", "zh-cn"):
        company = base_company + ["公司名称", "企业名称", "法人名称"]
        rep = base_rep + ["法人代表", "法定代表人", "董事长", "总经理"]
        return company, rep
    if code in ("tw", "taiwan", "zh-tw"):
        company = base_company + ["公司名稱", "法人名稱", "企業名稱"]
        rep = base_rep + ["負責人", "代表人", "董事長", "總經理"]
        return company, rep
    if code in ("hk", "hong_kong"):
        company = base_company + ["公司名稱", "公司名称", "法人名稱"]
        rep = base_rep + ["負責人", "代表人", "董事長", "總經理", "总经理"]
        return company, rep
    if code in ("sg", "singapore"):
        company = base_company + ["Company Name", "Legal Name"]
        rep = base_rep + ["CEO", "Managing Director", "Director"]
        return company, rep
    if code in ("us", "usa", "en"):
        company = base_company + ["Company Name", "Legal Name", "Business Name"]
        rep = base_rep + ["CEO", "President", "Managing Director", "Owner"]
        return company, rep
    if code in ("th", "thailand"):
        company = base_company + ["บริษัท", "ชื่อบริษัท"]
        rep = base_rep + ["กรรมการผู้จัดการ", "ประธานกรรมการ"]
        return company, rep
    if code in ("vn", "vietnam"):
        company = base_company + ["Tên công ty", "Pháp nhân"]
        rep = base_rep + ["Giám đốc", "Tổng giám đốc", "Chủ tịch"]
        return company, rep
    if code in ("id", "indonesia"):
        company = base_company + ["Nama perusahaan", "Badan hukum"]
        rep = base_rep + ["Direktur", "CEO", "Presiden Direktur"]
        return company, rep
    if code in ("ph", "philippines"):
        company = base_company + ["Company Name", "Legal Name"]
        rep = base_rep + ["President", "CEO", "Managing Director"]
        return company, rep
    return base_company, base_rep

