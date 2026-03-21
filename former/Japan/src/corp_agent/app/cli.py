from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
import time
from datetime import datetime
from pathlib import Path
import requests

from ..core.loader import iter_corp_records
from ..core.models import CorpRecord
from ..core.output import write_json


def _print_ts(message: str, *, end: str = "\n", flush: bool = True) -> None:
    text = str(message or "")
    if not text:
        print(text, end=end, flush=flush)
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} {text}", end=end, flush=flush)


def main() -> None:
    args = _parse_args()
    run_dir = Path(args.run_dir) if args.run_dir else _build_run_dir(Path(args.output_dir))
    run_dir.mkdir(parents=True, exist_ok=True)

    source_path = _resolve_source(args, run_dir)
    if not source_path or not source_path.exists():
        raise SystemExit("未找到可用的输入文件，请提供 --input 或 --download-url")

    seen = set()
    output_jsonl = run_dir / "corp_registry.jsonl"
    output_csv = run_dir / "corp_registry.csv"
    output_json = run_dir / "corp_registry.json"

    if args.resume and output_jsonl.exists():
        seen = _load_seen_numbers(output_jsonl)

    pref = args.prefecture.strip() if args.prefecture else ""
    city = args.city.strip() if args.city else ""
    name_contains = args.name_contains.strip() if args.name_contains else ""
    kinds = _parse_list(args.kind)

    json_records: list[CorpRecord] = []
    total = 0
    kept = 0
    last_log = time.time()
    last_total = 0

    with output_jsonl.open("w", encoding="utf-8") as jsonl_file:
        with output_csv.open("w", encoding="utf-8", newline="") as csv_file:
            csv_file.write("\ufeff")
            writer = csv.DictWriter(csv_file, fieldnames=[
                "corporate_number",
                "name",
                "kind",
                "prefecture",
                "city",
                "address",
                "updated_at",
                "source",
            ])
            writer.writeheader()
            for record in iter_corp_records(source_path, encoding=args.encoding):
                total += 1
                if record.corporate_number in seen:
                    continue
                if not _match_filters(record, pref, city, name_contains, kinds):
                    continue
                record.source = record.source or str(source_path)
                payload = asdict(record)
                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
                writer.writerow(payload)
                if args.write_json:
                    json_records.append(record)
                kept += 1
                if args.limit and kept >= args.limit:
                    break
                if args.log_every and total % args.log_every == 0:
                    delta = total - last_total
                    _log_progress(total, kept, delta, last_log)
                    last_log = time.time()
                    last_total = total
    if args.write_json:
        write_json(output_json, json_records)

    _print_ts(f"[完成] 总记录={total}，命中={kept}")
    _print_ts(f"[完成] 输出目录: {run_dir}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Corporate registry loader")
    parser.add_argument("--input", help="本地 CSV/ZIP 文件路径")
    parser.add_argument("--download-url", help="下载官方数据的 URL（ZIP/CSV）")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--prefecture", default=None)
    parser.add_argument("--city", default=None)
    parser.add_argument("--name-contains", default=None)
    parser.add_argument("--kind", default=None, help="逗号分隔，如 401")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--write-json", action="store_true", help="同时输出 JSON（数据量大时慎用）")
    parser.add_argument("--log-every", type=int, default=200000)
    return parser.parse_args()


def _resolve_source(args: argparse.Namespace, run_dir: Path) -> Path | None:
    if args.input:
        return Path(args.input)
    if args.download_url:
        name = Path(args.download_url).name or "corp_source.zip"
        target = run_dir / name
        _download_file(args.download_url, target)
        return target
    return None


def _download_file(url: str, path: Path) -> None:
    headers = {"User-Agent": "OfficialSiteAgent/1.0"}
    with requests.get(url, headers=headers, stream=True, timeout=60) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"download failed: HTTP {resp.status_code}")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _parse_list(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {part.strip() for part in raw.split(",") if part.strip()}


def _match_filters(
    record: CorpRecord,
    prefecture: str,
    city: str,
    name_contains: str,
    kinds: set[str],
) -> bool:
    if prefecture:
        if prefecture not in (record.prefecture or "") and prefecture not in (record.address or ""):
            return False
    if city:
        if city not in (record.city or "") and city not in (record.address or ""):
            return False
    if name_contains and name_contains not in record.name:
        return False
    if kinds and (record.kind or "").strip() not in kinds:
        return False
    return True


def _load_seen_numbers(path: Path) -> set[str]:
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            number = str(obj.get("corporate_number") or "").strip()
            if number:
                seen.add(number)
    return seen


def _build_run_dir(base_dir: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_dir / f"corp_{stamp}"


def _log_progress(total: int, kept: int, delta: int, last_log: float) -> None:
    elapsed = max(0.1, time.time() - last_log)
    rate = int(delta / elapsed) if elapsed > 0 else 0
    _print_ts(f"[进度] 已扫描 {total} 条，命中 {kept} 条（~{rate}/s）")


if __name__ == "__main__":
    main()
