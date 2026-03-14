from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from ..models import SiteInput
from ..utils import canonical_site_key, normalize_url

WEBSITE_KEYS = [
    "website",
    "official_website",
    "officialWebsite",
    "url",
    "homepage",
    "site",
    "web",
    "link",
]

NAME_KEYS = [
    "name",
    "company_name",
    "merchant_name",
    "title",
]


def _pick_first(record: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _to_site(record: dict[str, Any]) -> SiteInput | None:
    website = _pick_first(record, WEBSITE_KEYS)
    website = normalize_url(website or "")
    if not website:
        return None
    name = _pick_first(record, NAME_KEYS)
    source = record.get("source") if isinstance(record.get("source"), str) else None
    return SiteInput(website=website, input_name=name, source=source, raw=record)


def load_sites(path: Path) -> list[SiteInput]:
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")
    if path.suffix.lower() == ".csv":
        return _load_csv(path)
    if path.suffix.lower() == ".jsonl":
        return _load_jsonl(path)
    if path.suffix.lower() == ".json":
        return _load_json(path)
    raise ValueError(f"unsupported input format: {path}")


def _load_csv(path: Path) -> list[SiteInput]:
    items: list[SiteInput] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {k: v for k, v in row.items() if k}
            site = _to_site(record)
            if site:
                items.append(site)
    return _dedupe(items)


def _load_jsonl(path: Path) -> list[SiteInput]:
    items: list[SiteInput] = []
    for line in _read_lines(path, ["utf-8-sig", "utf-16"]):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            site = _to_site(record)
            if site:
                items.append(site)
    return _dedupe(items)


def _load_json(path: Path) -> list[SiteInput]:
    raw_text = _read_text(path, ["utf-8-sig", "utf-16"])
    raw = json.loads(raw_text)
    items: list[SiteInput] = []
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        raw = raw["items"]
    if isinstance(raw, list):
        for record in raw:
            if isinstance(record, dict):
                site = _to_site(record)
                if site:
                    items.append(site)
    return _dedupe(items)


def _dedupe(items: list[SiteInput]) -> list[SiteInput]:
    seen: set[str] = set()
    deduped: list[SiteInput] = []
    for item in items:
        key = canonical_site_key(item.website) or item.website.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _read_text(path: Path, encodings: list[str]) -> str:
    last_exc: UnicodeError | None = None
    for encoding in encodings:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeError as exc:
            last_exc = exc
    if last_exc:
        data = path.read_bytes()
        return data.decode("utf-8", errors="replace")
    return path.read_text(encoding="utf-8")


def _read_lines(path: Path, encodings: list[str]) -> list[str]:
    last_exc: UnicodeError | None = None
    for encoding in encodings:
        try:
            with path.open("r", encoding=encoding) as file:
                return file.readlines()
        except UnicodeError as exc:
            last_exc = exc
    if last_exc:
        data = path.read_bytes()
        text = data.decode("utf-8", errors="replace")
        return text.splitlines()
    with path.open("r", encoding="utf-8") as file:
        return file.readlines()
