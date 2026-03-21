from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Tuple


OUTPUT_FIELDS = ["cin", "name", "status", "email", "director_name"]


def parse_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}


def extract_email(row: Dict[str, str]) -> str:
    direct = row.get("email")
    if direct:
        return direct
    contact_details = parse_json(row.get("contact_details"))
    for key in ("Email ID", "Email", "email"):
        value = contact_details.get(key)
        if value:
            return str(value)
    return ""


def extract_director_name(row: Dict[str, str]) -> str:
    direct = row.get("director_name")
    if direct:
        return direct
    current_director = parse_json(row.get("current_director"))
    for key in ("Director Name", "director_name", "name"):
        value = current_director.get(key)
        if value:
            return str(value)
    return ""


def resolve_output_paths(input_path: Path, output_path: Path | None) -> Tuple[Path, Path, bool]:
    if output_path is None:
        output_path = input_path
    if output_path.resolve() == input_path.resolve():
        temp_path = input_path.with_suffix(".csv.tmp")
        return output_path, temp_path, True
    return output_path, output_path, False


def trim_csv(input_path: Path, output_path: Path | None = None) -> bool:
    final_path, write_path, inplace = resolve_output_paths(input_path, output_path)
    with input_path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError("input CSV has no header")
        with write_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()
            for row in reader:
                writer.writerow(
                    {
                        "cin": row.get("cin", ""),
                        "name": row.get("name", ""),
                        "status": row.get("status", ""),
                        "email": extract_email(row),
                        "director_name": extract_director_name(row),
                    }
                )
    if inplace:
        try:
            write_path.replace(final_path)
        except PermissionError:
            print(
                "target file is in use. "
                f"close it and replace manually: {write_path} -> {final_path}"
            )
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Trim companies.csv to required fields")
    parser.add_argument("--input", required=True, help="input companies.csv path")
    parser.add_argument("--output", help="output path (default: overwrite input)")
    args = parser.parse_args()

    ok = trim_csv(Path(args.input), Path(args.output) if args.output else None)
    if ok:
        print("trim completed")


if __name__ == "__main__":
    main()
