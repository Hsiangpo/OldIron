import argparse
import csv
import json
from pathlib import Path
from typing import Dict


def flatten(prefix: str, data) -> Dict[str, str]:
    if isinstance(data, dict):
        result: Dict[str, str] = {}
        for key, value in data.items():
            sub = flatten(f"{prefix}{key}", value)
            result.update(sub)
        return result
    if isinstance(data, list):
        return {prefix.rstrip("__"): json.dumps(data, ensure_ascii=False)}
    return {prefix.rstrip("__"): "" if data is None else str(data)}


def collect_fields(input_path: Path) -> list[str]:
    fields = set()
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            flat = flatten_record(record)
            fields.update(flat.keys())
    return sorted(fields)


def flatten_record(record: Dict[str, object]) -> Dict[str, str]:
    flat = {}
    for key, value in record.items():
        if isinstance(value, dict):
            flat.update(flatten(f"{key}__", value))
        else:
            flat[key] = "" if value is None else str(value)
    return flat


def main() -> None:
    parser = argparse.ArgumentParser(description="Expand JSONL to wide CSV")
    parser.add_argument("--input", required=True, help="JSONL 鏂囦欢璺緞")
    parser.add_argument("--output", required=True, help="杈撳嚭 CSV 璺緞")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    fields = collect_fields(input_path)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        with input_path.open("r", encoding="utf-8") as source:
            for line in source:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                writer.writerow(flatten_record(record))

    print(f"杈撳嚭瀹屾垚: {output_path}")


if __name__ == "__main__":
    main()
