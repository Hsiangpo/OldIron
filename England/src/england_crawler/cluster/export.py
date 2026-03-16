"""England 集群快照与交付导出。"""

from __future__ import annotations

import csv
import json
import tempfile
from datetime import datetime
from datetime import timezone
from pathlib import Path

from england_crawler.cluster.db import ClusterDb
from psycopg.types.json import Jsonb
from england_crawler.snov.client import extract_domain


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    with open(fd, "w", encoding="utf-8", newline="") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(path)


def _parse_emails(raw: object) -> list[str]:
    if isinstance(raw, list):
        source = raw
    else:
        try:
            source = json.loads(str(raw or "[]"))
        except json.JSONDecodeError:
            source = []
    if not isinstance(source, list):
        return []
    values: list[str] = []
    for item in source:
        text = str(item or "").strip().lower()
        if text and text not in values:
            values.append(text)
    return values


def export_cluster_snapshots(db: ClusterDb, output_root: Path, *, include_delivery: bool = True) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT duns, company_name_resolved, company_name_en_dnb, key_principal, website, dnb_website, domain, phone, emails_json
                FROM england_dnb_companies
                ORDER BY duns
                """
            )
            dnb_rows = []
            for row in cur.fetchall():
                homepage = str(row["website"] or "").strip() or str(row["dnb_website"] or "").strip()
                dnb_rows.append(
                    {
                        "comp_id": str(row["duns"]),
                        "duns": str(row["duns"]),
                        "company_name": str(row["company_name_resolved"] or row["company_name_en_dnb"] or "").strip(),
                        "ceo": str(row["key_principal"] or "").strip(),
                        "homepage": homepage,
                        "domain": str(row["domain"] or "").strip() or extract_domain(homepage),
                        "phone": str(row["phone"] or "").strip(),
                        "emails": _parse_emails(row["emails_json"]),
                    }
                )
            cur.execute(
                """
                SELECT comp_id, company_name, ceo, homepage, domain, phone, emails_json, company_number, company_status
                FROM england_ch_companies
                ORDER BY comp_id
                """
            )
            ch_rows = []
            for row in cur.fetchall():
                ch_rows.append(
                    {
                        "comp_id": str(row["comp_id"]),
                        "company_name": str(row["company_name"] or "").strip(),
                        "ceo": str(row["ceo"] or "").strip(),
                        "homepage": str(row["homepage"] or "").strip(),
                        "domain": str(row["domain"] or "").strip(),
                        "phone": str(row["phone"] or "").strip(),
                        "emails": _parse_emails(row["emails_json"]),
                        "company_number": str(row["company_number"] or "").strip(),
                        "company_status": str(row["company_status"] or "").strip(),
                    }
                )
            if include_delivery:
                cur.execute(
                    """
                    SELECT run_id, day_number, keys_text, summary_json
                    FROM england_delivery_runs
                    ORDER BY day_number
                    """
                )
                delivery_runs = cur.fetchall()
                delivery_data = []
                for run in delivery_runs:
                    cur.execute(
                        """
                        SELECT company_name, ceo, homepage, domain, phone, emails_text
                        FROM england_delivery_items
                        WHERE run_id = %s
                        ORDER BY row_index ASC, item_id ASC
                        """,
                        (run["run_id"],),
                    )
                    delivery_data.append((dict(run), cur.fetchall()))
    dnb_dir = output_root / "dnb"
    ch_dir = output_root / "companies_house"
    _write_jsonl(dnb_dir / "final_companies.jsonl", dnb_rows)
    _write_jsonl(dnb_dir / "companies_with_emails.jsonl", dnb_rows)
    _write_jsonl(ch_dir / "final_companies.jsonl", ch_rows)
    _write_jsonl(ch_dir / "companies_with_emails.jsonl", ch_rows)
    if include_delivery:
        _export_delivery_history(output_root / "delivery", delivery_data)


def _parse_generated_at(raw: object) -> datetime:
    text = str(raw or "").strip()
    if not text:
        return datetime.now(timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def _export_delivery_history(delivery_root: Path, delivery_data) -> None:
    delivery_root.mkdir(parents=True, exist_ok=True)
    for run_meta, items in delivery_data:
        summary = dict(run_meta["summary_json"] or {})
        day_number = int(run_meta["day_number"])
        day_dir = delivery_root / f"England_day{day_number:03d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        (day_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (day_dir / "keys.txt").write_text(str(run_meta["keys_text"] or ""), encoding="utf-8")
        with (day_dir / "companies.csv").open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=["company_name", "ceo", "homepage", "domain", "phone", "emails"],
            )
            writer.writeheader()
            for row in items:
                writer.writerow(
                    {
                        "company_name": str(row["company_name"] or ""),
                        "ceo": str(row["ceo"] or ""),
                        "homepage": str(row["homepage"] or ""),
                        "domain": str(row["domain"] or ""),
                        "phone": str(row["phone"] or ""),
                        "emails": str(row["emails_text"] or ""),
                    }
                )
        if day_number == 1:
            (day_dir / "companies_001.csv").write_text(
                (day_dir / "companies.csv").read_text(encoding="utf-8"),
                encoding="utf-8",
            )


def sync_delivery_history_to_db(db: ClusterDb, delivery_root: Path, *, day_number: int | None = None) -> None:
    run_rows: list[tuple] = []
    item_groups: list[tuple[int, list[tuple[str, str, str, str, str, str, int]]]] = []
    day_dirs = sorted(delivery_root.glob("England_day*"))
    for day_dir in day_dirs:
        if not day_dir.is_dir():
            continue
        matched_day = int(day_dir.name[-3:])
        if day_number is not None and matched_day != int(day_number):
            continue
        summary_path = day_dir / "summary.json"
        csv_path = day_dir / "companies.csv"
        keys_path = day_dir / "keys.txt"
        if not summary_path.exists() or not csv_path.exists() or not keys_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        run_rows.append(
            (
                matched_day,
                int(summary.get("baseline_day", 0) or 0),
                int(summary.get("total_current_companies", 0) or 0),
                int(summary.get("delta_companies", 0) or 0),
                _parse_generated_at(summary.get("generated_at", "")),
                keys_path.read_text(encoding="utf-8"),
                Jsonb(summary),
            )
        )
        items: list[tuple[str, str, str, str, str, str, int]] = []
        with csv_path.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            for index, row in enumerate(reader, start=1):
                items.append(
                    (
                        str(row.get("company_name", "")).strip(),
                        str(row.get("ceo", "")).strip(),
                        str(row.get("homepage", "")).strip(),
                        str(row.get("domain", "")).strip(),
                        str(row.get("phone", "")).strip(),
                        str(row.get("emails", "")).strip(),
                        index,
                    )
                )
        item_groups.append((matched_day, items))
    with db.transaction() as conn:
        with conn.cursor() as cur:
            if run_rows:
                cur.executemany(
                    """
                    INSERT INTO england_delivery_runs(
                        day_number, baseline_day, total_current_companies, delta_companies, generated_at, keys_text, summary_json
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(day_number) DO UPDATE SET
                        baseline_day = EXCLUDED.baseline_day,
                        total_current_companies = EXCLUDED.total_current_companies,
                        delta_companies = EXCLUDED.delta_companies,
                        generated_at = EXCLUDED.generated_at,
                        keys_text = EXCLUDED.keys_text,
                        summary_json = EXCLUDED.summary_json,
                        updated_at = NOW()
                    """,
                    run_rows,
                )
            for matched_day, items in item_groups:
                cur.execute("SELECT run_id FROM england_delivery_runs WHERE day_number = %s", (matched_day,))
                row = cur.fetchone()
                if row is None:
                    continue
                run_id = int(row["run_id"])
                cur.execute("DELETE FROM england_delivery_items WHERE run_id = %s", (run_id,))
                for company_name, ceo, homepage, domain, phone, emails_text, row_index in items:
                    cur.execute(
                        """
                        INSERT INTO england_delivery_items(
                            run_id, company_name, ceo, homepage, domain, phone, emails_text, row_index
                        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (run_id, company_name, ceo, homepage, domain, phone, emails_text, row_index),
                    )
