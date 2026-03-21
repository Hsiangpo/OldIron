from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path

from .models import CorpRecord


FIELDS = [
    "corporate_number",
    "name",
    "kind",
    "prefecture",
    "city",
    "address",
    "updated_at",
    "source",
]


def write_jsonl(path: Path, records: list[CorpRecord]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


def write_json(path: Path, records: list[CorpRecord]) -> None:
    data = [asdict(record) for record in records]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, records: list[CorpRecord]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        file.write("\ufeff")
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))
