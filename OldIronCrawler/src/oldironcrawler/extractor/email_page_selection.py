from __future__ import annotations

from urllib.parse import urlparse


_EMAIL_PAGE_BUDGET = 24_000
_EMAIL_HEAD_LINE_LIMIT = 16
_EMAIL_TAIL_LINE_LIMIT = 10
_EMAIL_CONTEXT_HINTS = (
    "@", "mailto:", "[at]", "(at)", " at ", "[dot]", "(dot)", " dot ",
    "contact", "contacts", "contacto", "contato", "fale", "conosco",
    "fale conosco", "fale-conosco", "atendimento", "ouvidoria", "sac",
    "email", "e-mail", "mail", "sales", "commercial", "comercial",
    "support", "customer", "service", "help", "privacy", "privacidade",
    "lgpd", "dpo", "iletisim", "iletişim", "bize", "ulasin", "ulaşın",
    "kvkk", "inquiry", "inquiries", "form", "mailform", "otoiawase",
    "toiawase", "recruit", "saiyo", "career", "careers", "kariyer",
    "insan kaynaklari", "insan kaynakları", "trabalhe", "trabalhe conosco",
    "お問い合わせ", "問合せ", "採用",
)


def build_email_teacher_pool(snapshot, website: str, *, limit: int = 60) -> list[str]:
    selected = {_teacher_identity_url(url) for url in snapshot.email_urls}
    homepage_key = _teacher_identity_url(website)
    result: list[str] = []
    seen: set[str] = set()
    ordered = sorted(
        snapshot.candidates,
        key=lambda item: (-item.email_final_score, item.depth, item.discovery_order, item.url),
    )
    for candidate in ordered:
        _append_email_teacher_url(candidate.url, result, seen, selected, homepage_key, limit)
        if len(result) >= limit:
            return result
    for url in snapshot.urls:
        _append_email_teacher_url(url, result, seen, selected, homepage_key, limit)
        if len(result) >= limit:
            break
    return result


def pick_email_urls_or_empty(
    llm_client,
    *,
    homepage: str,
    candidate_urls: list[str],
    existing_email_urls: list[str],
    target_count: int,
    deadline_monotonic: float | None,
) -> list[str]:
    picker = getattr(llm_client, "pick_email_urls", None)
    if not callable(picker):
        return []
    try:
        return picker(
            homepage=homepage,
            candidate_urls=candidate_urls,
            existing_email_urls=existing_email_urls,
            target_count=target_count,
            deadline_monotonic=deadline_monotonic,
        )
    except Exception:  # noqa: BLE001
        return []


def prepare_email_pages_for_llm(pages: list[dict[str, str]]) -> list[dict[str, str]]:
    ranked = sorted(
        pages,
        key=lambda page: _email_page_priority(page.get("url", ""), page.get("content", "")),
        reverse=True,
    )
    prepared = [
        {
            "url": str(page.get("url", "") or "").strip(),
            "content": _prioritize_email_content(str(page.get("content", "") or "").strip()),
        }
        for page in ranked
    ]
    return _fit_email_pages_to_budget(prepared, budget=_EMAIL_PAGE_BUDGET)


def _append_email_teacher_url(
    url: str,
    result: list[str],
    seen: set[str],
    selected: set[str],
    homepage_key: str,
    limit: int,
) -> None:
    if len(result) >= limit:
        return
    value = str(url or "").strip()
    key = _teacher_identity_url(value)
    if not value or not key or key == homepage_key or key in selected or key in seen:
        return
    seen.add(key)
    result.append(value)


def _teacher_identity_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.netloc:
        return str(url or "").strip().lower().rstrip("/")
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").rstrip("/")
    if path == "/":
        path = ""
    query = str(parsed.query or "").strip()
    return f"{host}{path}?{query}" if query else f"{host}{path}"


def _email_page_priority(url: str, content: str) -> int:
    lowered_url = str(url or "").lower()
    lowered_content = str(content or "").lower()
    score = 0
    for hint in _EMAIL_CONTEXT_HINTS:
        if hint in lowered_url:
            score += 6
        if hint in lowered_content:
            score += 1
    if "@" in lowered_content or "[at]" in lowered_content or "(at)" in lowered_content:
        score += 10
    return score


def _prioritize_email_content(content: str) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    windows = _collect_email_windows(lines)
    if not windows:
        return _edge_email_content(lines)
    prioritized_lines: list[str] = []
    seen: set[str] = set()
    prioritized_lines.append("--- email/contact context ---")
    for start, end in windows:
        _append_unique_lines(prioritized_lines, lines[start:end], seen)
        prioritized_lines.append("---")
    if prioritized_lines and prioritized_lines[-1] == "---":
        prioritized_lines.pop()
    prioritized_lines.append("--- page start ---")
    _append_unique_lines(prioritized_lines, lines[:_EMAIL_HEAD_LINE_LIMIT], seen)
    if len(lines) > _EMAIL_TAIL_LINE_LIMIT:
        prioritized_lines.append("--- page end ---")
        _append_unique_lines(prioritized_lines, lines[-_EMAIL_TAIL_LINE_LIMIT:], seen)
    return "\n".join(prioritized_lines).strip()


def _collect_email_windows(lines: list[str]) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for index, line in enumerate(lines):
        lowered = line.lower()
        if any(hint in lowered for hint in _EMAIL_CONTEXT_HINTS):
            windows.append((max(0, index - 3), min(len(lines), index + 7)))
    if not windows:
        return []
    merged: list[tuple[int, int]] = []
    start, end = windows[0]
    for next_start, next_end in windows[1:]:
        if next_start <= end + 2:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return merged[:24]


def _edge_email_content(lines: list[str]) -> str:
    text = "\n".join(lines).strip()
    if len(text) <= _EMAIL_PAGE_BUDGET:
        return text
    result: list[str] = []
    seen: set[str] = set()
    result.append("--- page start ---")
    _append_unique_lines(result, lines[:_EMAIL_HEAD_LINE_LIMIT], seen)
    if len(lines) > _EMAIL_TAIL_LINE_LIMIT:
        result.append("--- page end ---")
        _append_unique_lines(result, lines[-_EMAIL_TAIL_LINE_LIMIT:], seen)
    return "\n".join(result).strip()


def _fit_email_pages_to_budget(pages: list[dict[str, str]], *, budget: int) -> list[dict[str, str]]:
    if _total_page_chars(pages) <= budget:
        return pages
    remaining = max(int(budget), 1)
    fitted: list[dict[str, str]] = []
    for page in pages:
        if remaining <= 0:
            break
        url = str(page.get("url", "") or "").strip()
        content = str(page.get("content", "") or "").strip()
        if len(content) > remaining:
            content = _truncate_email_content(content, remaining)
        fitted.append({"url": url, "content": content})
        remaining -= len(content)
    return fitted


def _total_page_chars(pages: list[dict[str, str]]) -> int:
    return sum(len(str(page.get("content", "") or "")) for page in pages)


def _truncate_email_content(content: str, max_chars: int) -> str:
    text = str(content or "").strip()
    if len(text) <= max_chars:
        return text
    marker = "\n...\n"
    if max_chars <= len(marker):
        return text[:max_chars]
    return text[: max_chars - len(marker)] + marker


def _append_unique_lines(target: list[str], source: list[str], seen: set[str]) -> None:
    for line in source:
        if line in seen:
            continue
        seen.add(line)
        target.append(line)
