from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Iterable, TextIO
from zipfile import ZipFile

from .models import CorpRecord


_DEFAULT_INDEX = {
    "corporate_number": 0,
    "update_date": 3,
    "change_date": 4,
    "name": 5,
    "kind": 7,
    "prefecture": 8,
    "city": 9,
    "street": 10,
    "address_outside": 15,
}


def iter_corp_records(path: Path, encoding: str = "utf-8") -> Iterable[CorpRecord]:
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            for name in names:
                with zf.open(name) as raw:
                    with io.TextIOWrapper(raw, encoding=encoding, errors="replace", newline="") as handle:
                        yield from _iter_csv(handle, source=name)
        return
    with path.open("r", encoding=encoding, errors="replace", newline="") as handle:
        yield from _iter_csv(handle, source=str(path))


def _iter_csv(handle: TextIO, source: str) -> Iterable[CorpRecord]:
    reader = csv.reader(handle)
    header = next(reader, None)
    header_map, header_lower = _build_header_map(header or [])
    for row in reader:
        record = _row_to_record(row, header_map, header_lower, source=source)
        if record:
            yield record


def _build_header_map(header: list[str]) -> tuple[dict[str, int], dict[str, int]]:
    clean = [item.strip() for item in header if isinstance(item, str)]
    header_map = {name: idx for idx, name in enumerate(clean) if name}
    header_lower = {name.lower(): idx for name, idx in header_map.items()}
    return header_map, header_lower


def _row_to_record(
    row: list[str],
    header_map: dict[str, int],
    header_lower: dict[str, int],
    *,
    source: str,
) -> CorpRecord | None:
    if not row:
        return None

    corporate_number = _get_value(row, header_map, header_lower, "法人番号", "corporatenumber", "corporate_number") or _get_by_index(row, "corporate_number")
    name = _get_value(row, header_map, header_lower, "名称", "name") or _get_by_index(row, "name")
    if not corporate_number or not name:
        return None
    kind = _get_value(row, header_map, header_lower, "種別", "kind") or _get_by_index(row, "kind")
    prefecture = _get_value(row, header_map, header_lower, "都道府県名", "prefecturename") or _get_by_index(row, "prefecture")
    city = _get_value(row, header_map, header_lower, "市区町村名", "cityname") or _get_by_index(row, "city")
    street = _get_value(row, header_map, header_lower, "丁目番地等", "streetnumber") or _get_by_index(row, "street")
    address_outside = _get_value(row, header_map, header_lower, "住所外", "addressoutside") or _get_by_index(row, "address_outside")
    update_date = _get_value(row, header_map, header_lower, "更新日", "updatedate") or _get_by_index(row, "update_date")
    change_date = _get_value(row, header_map, header_lower, "変更日", "changedate") or _get_by_index(row, "change_date")

    parts = [prefecture, city, street, address_outside]
    address = "".join([p for p in parts if p])
    updated_at = update_date or change_date

    return CorpRecord(
        corporate_number=corporate_number,
        name=name,
        kind=kind,
        prefecture=prefecture,
        city=city,
        address=address if address else None,
        updated_at=updated_at,
        source=source,
    )


def _get_value(
    row: list[str],
    header_map: dict[str, int],
    header_lower: dict[str, int],
    *aliases: str,
) -> str | None:
    for alias in aliases:
        key = alias.strip()
        if not key:
            continue
        idx = header_map.get(key)
        if idx is None:
            idx = header_lower.get(key.lower())
        if idx is not None and idx < len(row):
            value = str(row[idx]).strip()
            if value:
                return value
    return None


def _get_by_index(row: list[str], key: str) -> str | None:
    idx = _DEFAULT_INDEX.get(key)
    if idx is None or idx >= len(row):
        return None
    value = str(row[idx]).strip()
    return value or None
