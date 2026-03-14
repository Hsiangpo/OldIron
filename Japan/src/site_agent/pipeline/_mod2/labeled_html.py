from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser


def extract_labeled_values_from_html(
    html: str, labels: list[str], *, max_values: int = 6
) -> list[str]:
    """Extract values for labels from table/definition-list like HTML structures."""
    if not html:
        return []
    results: list[str] = []
    # Prefer a structured HTML parse first to avoid regex over-capturing.
    for label_text, value_text in _extract_label_value_pairs_from_html(html):
        if not label_text or not value_text:
            continue
        if len(label_text) > 40:
            continue
        for label in labels:
            if not label:
                continue
            if label in label_text:
                if value_text not in results:
                    results.append(value_text)
                if len(results) >= max_values:
                    return results

    # Fallback: regex extraction (keep patterns conservative).
    for label in labels:
        if not label:
            continue
        label_pattern = (
            rf"(?:[^<]|<(?!/t[hd]|/dt|/dd)[^>]+>)*{re.escape(label)}"
            rf"(?:[^<]|<(?!/t[hd]|/dt|/dd)[^>]+>)*"
        )
        patterns = [
            rf"<(?:th|dt)[^>]*>{label_pattern}</(?:th|dt)>\s*<(?:td|dd)[^>]*>(.*?)</(?:td|dd)>",
            rf"<tr[^>]*>.*?<t[dh][^>]*>{label_pattern}</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>",
            rf"<td[^>]*class=[\"'][^\"']*(?:label|title|name|item)[^\"']*[\"'][^>]*>{label_pattern}</td>\s*<td[^>]*>(.*?)</td>",
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html, flags=re.IGNORECASE | re.DOTALL):
                raw = match.group(1)
                value = re.sub(r"<[^>]+>", " ", unescape(raw))
                value = re.sub(r"\s+", " ", value).strip()
                if not value:
                    continue
                if value not in results:
                    results.append(value)
                if len(results) >= max_values:
                    return results
    return results


class _LabelValueHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.pairs: list[tuple[str, str]] = []
        self._in_tr = False
        self._row_cells: list[tuple[str, str, bool]] = []
        self._capture_tag: str | None = None
        self._capture_text: list[str] = []
        self._capture_is_label_td = False
        self._pending_dt_label: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._in_tr = True
            self._row_cells = []
            return
        if tag in ("th", "td", "dt", "dd"):
            self._capture_tag = tag
            self._capture_text = []
            if tag == "td":
                self._capture_is_label_td = _td_looks_like_label(attrs)
            else:
                self._capture_is_label_td = False

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._flush_row()
            self._in_tr = False
            return
        if self._capture_tag != tag:
            return
        text = _clean_html_text("".join(self._capture_text))
        if tag in ("th", "td"):
            if text:
                self._row_cells.append((tag, text, self._capture_is_label_td))
        elif tag == "dt":
            if text:
                self._pending_dt_label = text
        elif tag == "dd":
            if text and self._pending_dt_label:
                self.pairs.append((self._pending_dt_label, text))
            self._pending_dt_label = None
        self._capture_tag = None
        self._capture_text = []
        self._capture_is_label_td = False

    def handle_data(self, data: str) -> None:
        if self._capture_tag is None:
            return
        if data:
            self._capture_text.append(data)

    def _flush_row(self) -> None:
        if not self._row_cells:
            return
        for idx in range(len(self._row_cells) - 1):
            tag, label_text, is_label_td = self._row_cells[idx]
            value_text = self._row_cells[idx + 1][1]
            if not label_text or not value_text:
                continue
            if tag == "th" or is_label_td:
                self.pairs.append((label_text, value_text))
            else:
                self.pairs.append((label_text, value_text))


def _td_looks_like_label(attrs: list[tuple[str, str | None]]) -> bool:
    if not attrs:
        return False
    classes = []
    for key, value in attrs:
        if key.lower() != "class" or not value:
            continue
        if isinstance(value, str):
            classes.extend(value.split())
    if not classes:
        return False
    hints = {"label", "title", "name", "item", "header", "head"}
    return any(cls.lower() in hints for cls in classes)


def _clean_html_text(text: str) -> str:
    if not text:
        return ""
    cleaned = unescape(text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _extract_label_value_pairs_from_html(html: str) -> list[tuple[str, str]]:
    parser = _LabelValueHTMLParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return []
    return parser.pairs


def extract_labeled_from_html(html: str, labels: list[str]) -> str | None:
    """Extract the first labeled value for labels from HTML."""
    values = extract_labeled_values_from_html(html, labels, max_values=1)
    return values[0] if values else None


# Backwards-compatible names (used by tests/imports in `site_agent.pipeline`). Keep in sync.
_extract_labeled_values_from_html = extract_labeled_values_from_html
_extract_labeled_from_html = extract_labeled_from_html

