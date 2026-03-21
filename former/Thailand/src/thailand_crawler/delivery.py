"""日交付打包。"""

from __future__ import annotations

from collections import Counter
import csv
import json
import re
import shutil
import sqlite3
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path

from thailand_crawler.domain_quality import assess_company_domain


DAY_PATTERN = re.compile(r'^day(\d+)$', flags=re.I)


def parse_day_label(raw: str) -> int:
    matched = DAY_PATTERN.fullmatch(raw.strip())
    if matched is None:
        raise ValueError('参数必须是 dayN，例如 day1。')
    value = int(matched.group(1))
    if value <= 0:
        raise ValueError('dayN 中的 N 必须大于 0。')
    return value


def _build_key(record: dict) -> str:
    if str(record.get('_key', '')).strip():
        return str(record['_key']).strip()
    duns = str(record.get('duns', '')).strip()
    domain = str(record.get('domain', '')).strip().lower()
    return f'{duns}|{domain}' if domain else duns


def _list_existing_days(delivery_root: Path) -> list[int]:
    if not delivery_root.exists():
        return []
    days: list[int] = []
    for item in delivery_root.iterdir():
        if not item.is_dir():
            continue
        matched = re.fullmatch(r'Thailand_day(\d{3})', item.name)
        if matched:
            days.append(int(matched.group(1)))
    return sorted(days)


def _read_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    keys_path = delivery_root / f'Thailand_day{baseline_day:03d}' / 'keys.txt'
    if not keys_path.exists():
        return set()
    return {line.strip() for line in keys_path.read_text(encoding='utf-8').splitlines() if line.strip()}


def _load_stream_records(site_dir: Path) -> list[dict]:
    db_path = site_dir / 'store.db'
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            'SELECT duns, company_name, company_manager, contact_emails, domain, phone FROM final_companies ORDER BY updated_at ASC'
        ).fetchall()
    finally:
        conn.close()
    result: list[dict] = []
    for row in rows:
        emails = json.loads(str(row['contact_emails'] or '[]'))
        if not isinstance(emails, list):
            emails = []
        result.append(
            {
                'duns': str(row['duns']),
                'company_name': str(row['company_name']),
                'key_principal': str(row['company_manager']),
                'emails': [str(item).strip() for item in emails if str(item).strip()],
                'domain': str(row['domain']),
                'phone': str(row['phone']),
                '_key': str(row['duns']),
                '_source': site_dir.name,
            }
        )
    return result


def _load_jsonl_records(site_dir: Path) -> list[dict]:
    rows: list[dict] = []
    for data_file in (
        site_dir / 'companies_with_emails.jsonl',
        site_dir / 'final_companies.jsonl',
    ):
        if not data_file.exists():
            continue
        with data_file.open('r', encoding='utf-8') as fp:
            for line in fp:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if isinstance(payload, dict):
                    payload['_source'] = site_dir.name
                    rows.append(payload)
    return rows


def _load_records(data_root: Path) -> list[dict]:
    records: list[dict] = []
    if not data_root.exists():
        return records
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name == 'delivery':
            continue
        rows = _load_stream_records(site_dir)
        if not rows:
            rows = _load_jsonl_records(site_dir)
        records.extend(rows)
    return records


def _filter_domain_quality(records: list[dict]) -> tuple[list[dict], list[dict]]:
    counts = Counter(str(record.get('domain', '')).strip().lower() for record in records if str(record.get('domain', '')).strip())
    accepted: list[dict] = []
    rejected: list[dict] = []
    for record in records:
        domain = str(record.get('domain', '')).strip().lower()
        if not domain:
            accepted.append(record)
            continue
        assessment = assess_company_domain(
            company_name=str(record.get('company_name', '')).strip(),
            domain=domain,
            shared_count=int(counts.get(domain, 1)),
        )
        if assessment.blocked:
            filtered = dict(record)
            filtered['_reject_reason'] = assessment.reason
            rejected.append(filtered)
            continue
        accepted.append(record)
    return accepted, rejected


def _remove_dir_safe(path: Path) -> None:
    if not path.exists():
        return
    for _ in range(8):
        try:
            shutil.rmtree(path)
            return
        except (PermissionError, OSError):
            time.sleep(0.5)
    raise ValueError(f'交付目录被占用，请关闭文件后重试: {path}')


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict:
    target_day = parse_day_label(day_label)
    existing_days = _list_existing_days(delivery_root)
    latest = existing_days[-1] if existing_days else 0
    if latest == 0 and target_day != 1:
        raise ValueError('尚未有交付记录，首个交付只能执行 day1。')
    if target_day < latest:
        raise ValueError(f'第{target_day}天已交付，当前最新是第{latest}天。')
    if target_day > latest + 1:
        raise ValueError(f'只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。')

    baseline_day = target_day - 1
    baseline_keys = _read_baseline_keys(delivery_root, baseline_day)
    qualified: list[dict] = []
    seen_keys: set[str] = set()
    for record in _load_records(data_root):
        company_name = str(record.get('company_name', '')).strip()
        key_principal = str(record.get('key_principal', '')).strip()
        emails = record.get('emails', [])
        if not (company_name and key_principal and isinstance(emails, list) and any(str(item).strip() for item in emails)):
            continue
        key = _build_key(record)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        record['_key'] = key
        qualified.append(record)
    qualified, rejected = _filter_domain_quality(qualified)

    delta_records = [record for record in qualified if record['_key'] not in baseline_keys]
    day_dir = delivery_root / f'Thailand_day{target_day:03d}'
    _remove_dir_safe(day_dir)
    day_dir.mkdir(parents=True, exist_ok=True)

    csv_path = day_dir / 'companies.csv'
    with csv_path.open('w', encoding='utf-8', newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=['公司名', '代表人', '邮箱', '域名', '电话'])
        writer.writeheader()
        for record in delta_records:
            writer.writerow(
                {
                    '公司名': record.get('company_name', ''),
                    '代表人': record.get('key_principal', ''),
                    '邮箱': '; '.join(record.get('emails', [])),
                    '域名': record.get('domain', ''),
                    '电话': record.get('phone', ''),
                }
            )

    (day_dir / 'keys.txt').write_text('\n'.join(record['_key'] for record in qualified), encoding='utf-8')
    summary = {
        'day': target_day,
        'baseline_day': baseline_day,
        'total_current_companies': len(qualified),
        'delta_companies': len(delta_records),
        'filtered_domain_companies': len(rejected),
        'generated_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
    }
    (day_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return summary
