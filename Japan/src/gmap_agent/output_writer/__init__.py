from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path

from ..models import PlaceRecord


FIELDS = [
    "cid",
    "name",
    "website",
    "phone",
    "rating",
    "review_count",
    "status",
    "source",
]


def write_json(path: Path, records: list[PlaceRecord]) -> None:
    data = [asdict(record) for record in records]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, records: list[PlaceRecord]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


def append_jsonl(path: Path, records: list[PlaceRecord]) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


def write_csv(path: Path, records: list[PlaceRecord]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        file.write("\ufeff")
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def load_existing_records(path: Path) -> list[PlaceRecord]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        return _load_jsonl(path)
    if path.suffix.lower() == ".json":
        return _load_json(path)
    if path.suffix.lower() == ".csv":
        return _load_csv(path)
    return []


def _load_jsonl(path: Path) -> list[PlaceRecord]:
    items: list[PlaceRecord] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items.append(_to_record(payload))
    return items


def _load_json(path: Path) -> list[PlaceRecord]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    items: list[PlaceRecord] = []
    if isinstance(raw, list):
        for payload in raw:
            if isinstance(payload, dict):
                items.append(_to_record(payload))
    return items


def _load_csv(path: Path) -> list[PlaceRecord]:
    items: list[PlaceRecord] = []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            items.append(_to_record(row))
    return items


def _to_record(payload: dict) -> PlaceRecord:
    return PlaceRecord(
        cid=str(payload.get("cid") or ""),
        name=payload.get("name") or None,
        website=payload.get("website") or None,
        phone=payload.get("phone") or None,
        rating=_maybe_float(payload.get("rating")),
        review_count=_maybe_int(payload.get("review_count")),
        status=payload.get("status") or None,
        source=payload.get("source") or None,
    )


def _maybe_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _maybe_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
