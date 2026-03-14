"""England SQLite 到 Postgres 迁移。"""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime
from datetime import timezone
from pathlib import Path

from psycopg.types.json import Jsonb

from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.repository import CH_PIPELINE
from england_crawler.cluster.repository import DNB_PIPELINE
from england_crawler.cluster.repository import _build_task_id
from england_crawler.cluster.schema import initialize_schema


def _load_sqlite_rows(db_path: Path, query: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query).fetchall()
    finally:
        conn.close()


def _json_list(raw: object) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item or "").strip().lower()
        if text and text not in out:
            out.append(text)
    return out


def _queue_status(raw: object) -> str:
    status = str(raw or "").strip().lower()
    return "done" if status == "done" else "pending"


def _task_payload_json(payload: dict[str, object]) -> Jsonb:
    return Jsonb(payload)


def _chunked(items: list[tuple], size: int = 2000) -> list[list[tuple]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def _ts(raw: object) -> datetime:
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    text = str(raw or "").strip()
    if not text:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def _migrate_dnb(db: ClusterDb, output_root: Path) -> None:
    db_path = output_root / "dnb" / "store.db"
    companies = _load_sqlite_rows(db_path, "SELECT * FROM companies ORDER BY duns")
    details = {str(row["duns"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM detail_queue")}
    gmaps = {str(row["duns"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM gmap_queue")}
    firecrawls = {str(row["duns"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM snov_queue")}
    discovery = _load_sqlite_rows(db_path, "SELECT * FROM dnb_discovery_queue ORDER BY segment_id")
    segments = _load_sqlite_rows(db_path, "SELECT * FROM dnb_segments ORDER BY segment_id")
    company_rows: list[tuple] = []
    task_rows: list[tuple] = []
    for row in companies:
        duns = str(row["duns"])
        detail = details.get(duns)
        gmap = gmaps.get(duns)
        firecrawl = firecrawls.get(duns)
        company_rows.append(
            (
                duns,
                str(row["company_name_en_dnb"] or ""),
                str(row["company_name_url"] or ""),
                str(row["key_principal"] or ""),
                str(row["address"] or ""),
                str(row["city"] or ""),
                str(row["region"] or ""),
                str(row["country"] or ""),
                str(row["postal_code"] or ""),
                str(row["sales_revenue"] or ""),
                str(row["dnb_website"] or ""),
                str(row["website"] or ""),
                str(row["domain"] or ""),
                str(row["website_source"] or ""),
                str(row["company_name_en_gmap"] or ""),
                str(row["company_name_en_site"] or ""),
                str(row["company_name_resolved"] or ""),
                str(row["site_evidence_url"] or ""),
                str(row["site_evidence_quote"] or ""),
                float(row["site_confidence"] or 0.0),
                str(row["phone"] or ""),
                Jsonb(_json_list(row["emails_json"])),
                bool(int(row["detail_done"] or 0)),
                _queue_status(detail["status"] if detail else "done"),
                int(detail["retries"] if detail else 0),
                _queue_status(gmap["status"] if gmap else ""),
                int(gmap["retries"] if gmap else 0),
                _queue_status(firecrawl["status"] if firecrawl else ""),
                int(firecrawl["retries"] if firecrawl else 0),
                str(row["last_error"] or ""),
                _ts(row["updated_at"]),
            )
        )
        if detail and str(detail["status"]) != "done":
            task_rows.append(
                (
                    _build_task_id(DNB_PIPELINE, "dnb_detail", duns),
                    DNB_PIPELINE,
                    "dnb_detail",
                    duns,
                    "pending",
                    int(detail["retries"] or 0),
                    _ts(detail["next_run_at"]),
                    _task_payload_json(
                        {
                            "duns": duns,
                            "company_name_en_dnb": str(row["company_name_en_dnb"] or ""),
                            "company_name_url": str(row["company_name_url"] or ""),
                            "address": str(row["address"] or ""),
                            "city": str(row["city"] or ""),
                            "region": str(row["region"] or ""),
                            "country": str(row["country"] or ""),
                            "postal_code": str(row["postal_code"] or ""),
                            "sales_revenue": str(row["sales_revenue"] or ""),
                        }
                    ),
                    _ts(detail["updated_at"]),
                )
            )
        if gmap and str(gmap["status"]) != "done":
            task_rows.append(
                (
                    _build_task_id(DNB_PIPELINE, "dnb_gmap", duns),
                    DNB_PIPELINE,
                    "dnb_gmap",
                    duns,
                    "pending",
                    int(gmap["retries"] or 0),
                    _ts(gmap["next_run_at"]),
                    _task_payload_json(
                        {
                            "duns": duns,
                            "company_name_en": str(row["company_name_en_dnb"] or ""),
                            "city": str(row["city"] or ""),
                            "region": str(row["region"] or ""),
                            "country": str(row["country"] or ""),
                            "dnb_website": str(row["dnb_website"] or ""),
                        }
                    ),
                    _ts(gmap["updated_at"]),
                )
            )
        if firecrawl and str(firecrawl["status"]) != "done":
            homepage = str(row["website"] or "").strip() or str(row["dnb_website"] or "").strip()
            task_rows.append(
                (
                    _build_task_id(DNB_PIPELINE, "dnb_firecrawl", duns),
                    DNB_PIPELINE,
                    "dnb_firecrawl",
                    duns,
                    "pending",
                    int(firecrawl["retries"] or 0),
                    _ts(firecrawl["next_run_at"]),
                    _task_payload_json(
                        {
                            "duns": duns,
                            "company_name_en_dnb": str(row["company_name_en_dnb"] or ""),
                            "homepage": homepage,
                            "domain": str(row["domain"] or ""),
                        }
                    ),
                    _ts(firecrawl["updated_at"]),
                )
            )
    discovery_rows = [
        (
            str(row["segment_id"] or ""),
            str(row["industry_path"] or ""),
            str(row["country_iso_two_code"] or ""),
            str(row["region_name"] or ""),
            str(row["city_name"] or ""),
            int(row["expected_count"] or 0),
            _queue_status(row["status"]),
            0,
            _ts(row["updated_at"]),
        )
        for row in discovery
    ]
    segment_rows = [
        (
            str(row["segment_id"] or ""),
            str(row["industry_path"] or ""),
            str(row["country_iso_two_code"] or ""),
            str(row["region_name"] or ""),
            str(row["city_name"] or ""),
            int(row["expected_count"] or 0),
            int(row["next_page"] or 1),
            _queue_status(row["status"]),
            0,
            _ts(row["updated_at"]),
        )
        for row in segments
    ]
    for row in discovery:
        if str(row["status"]) == "done":
            continue
        payload = {
            "segment_id": str(row["segment_id"] or ""),
            "industry_path": str(row["industry_path"] or ""),
            "country_iso_two_code": str(row["country_iso_two_code"] or ""),
            "region_name": str(row["region_name"] or ""),
            "city_name": str(row["city_name"] or ""),
            "expected_count": int(row["expected_count"] or 0),
        }
        task_rows.append(
            (
                _build_task_id(DNB_PIPELINE, "dnb_discovery", str(row["segment_id"] or "")),
                DNB_PIPELINE,
                "dnb_discovery",
                str(row["segment_id"] or ""),
                "pending",
                0,
                _ts(row["updated_at"]),
                _task_payload_json(payload),
                _ts(row["updated_at"]),
            )
        )
    for row in segments:
        if str(row["status"]) == "done":
            continue
        payload = {
            "segment_id": str(row["segment_id"] or ""),
            "industry_path": str(row["industry_path"] or ""),
            "country_iso_two_code": str(row["country_iso_two_code"] or ""),
            "region_name": str(row["region_name"] or ""),
            "city_name": str(row["city_name"] or ""),
            "expected_count": int(row["expected_count"] or 0),
            "next_page": int(row["next_page"] or 1),
            "page_size": 50,
        }
        task_rows.append(
            (
                _build_task_id(DNB_PIPELINE, "dnb_list_segment", str(row["segment_id"] or "")),
                DNB_PIPELINE,
                "dnb_list_segment",
                str(row["segment_id"] or ""),
                "pending",
                0,
                _ts(row["updated_at"]),
                _task_payload_json(payload),
                _ts(row["updated_at"]),
            )
        )
    with db.transaction() as conn:
        with conn.cursor() as cur:
            for chunk in _chunked(company_rows):
                cur.executemany(
                    """
                    INSERT INTO england_dnb_companies(
                        duns, company_name_en_dnb, company_name_url, key_principal, address, city, region, country,
                        postal_code, sales_revenue, dnb_website, website, domain, website_source, company_name_en_gmap,
                        company_name_en_site, company_name_resolved, site_evidence_url, site_evidence_quote, site_confidence,
                        phone, emails_json, detail_done, detail_task_status, detail_task_retries, gmap_task_status,
                        gmap_task_retries, firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(duns) DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """,
                    chunk,
                )
            if discovery_rows:
                cur.executemany(
                    """
                    INSERT INTO england_dnb_discovery_nodes(
                        segment_id, industry_path, country_iso_two_code, region_name, city_name,
                        expected_count, task_status, task_retries, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(segment_id) DO UPDATE SET
                        industry_path = EXCLUDED.industry_path,
                        country_iso_two_code = EXCLUDED.country_iso_two_code,
                        region_name = EXCLUDED.region_name,
                        city_name = EXCLUDED.city_name,
                        expected_count = EXCLUDED.expected_count,
                        task_status = EXCLUDED.task_status,
                        task_retries = EXCLUDED.task_retries,
                        updated_at = EXCLUDED.updated_at
                    """,
                    discovery_rows,
                )
            if segment_rows:
                cur.executemany(
                    """
                    INSERT INTO england_dnb_segments(
                        segment_id, industry_path, country_iso_two_code, region_name, city_name,
                        expected_count, next_page, task_status, task_retries, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(segment_id) DO UPDATE SET
                        industry_path = EXCLUDED.industry_path,
                        country_iso_two_code = EXCLUDED.country_iso_two_code,
                        region_name = EXCLUDED.region_name,
                        city_name = EXCLUDED.city_name,
                        expected_count = EXCLUDED.expected_count,
                        next_page = EXCLUDED.next_page,
                        task_status = EXCLUDED.task_status,
                        task_retries = EXCLUDED.task_retries,
                        updated_at = EXCLUDED.updated_at
                    """,
                    segment_rows,
                )
            if task_rows:
                cur.executemany(
                    """
                    INSERT INTO england_cluster_tasks(
                        task_id, pipeline, task_type, entity_id, status, retries, next_run_at,
                        payload_json, updated_at, created_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        retries = EXCLUDED.retries,
                        next_run_at = EXCLUDED.next_run_at,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    [(*row, row[-1]) for row in task_rows],
                )


def _migrate_companies_house(db: ClusterDb, output_root: Path) -> None:
    db_path = output_root / "companies_house" / "store.db"
    companies = _load_sqlite_rows(db_path, "SELECT * FROM companies ORDER BY comp_id")
    ch_queue = {str(row["comp_id"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM ch_queue")}
    gmap_queue = {str(row["comp_id"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM gmap_queue")}
    firecrawl_queue = {str(row["comp_id"]): row for row in _load_sqlite_rows(db_path, "SELECT * FROM snov_queue")}
    source_files = _load_sqlite_rows(db_path, "SELECT * FROM source_files ORDER BY source_path")
    company_rows: list[tuple] = []
    task_rows: list[tuple] = []
    for row in companies:
        comp_id = str(row["comp_id"])
        ch = ch_queue.get(comp_id)
        gmap = gmap_queue.get(comp_id)
        firecrawl = firecrawl_queue.get(comp_id)
        company_rows.append(
            (
                comp_id,
                str(row["company_name"] or ""),
                str(row["normalized_name"] or ""),
                str(row["company_number"] or ""),
                str(row["company_status"] or ""),
                str(row["ceo"] or ""),
                str(row["homepage"] or ""),
                str(row["domain"] or ""),
                str(row["phone"] or ""),
                Jsonb(_json_list(row["emails_json"])),
                _queue_status(ch["status"] if ch else ""),
                int(ch["retries"] if ch else 0),
                _queue_status(gmap["status"] if gmap else ""),
                int(gmap["retries"] if gmap else 0),
                _queue_status(firecrawl["status"] if firecrawl else ""),
                int(firecrawl["retries"] if firecrawl else 0),
                str(row["last_error"] or ""),
                _ts(row["updated_at"]),
            )
        )
        if ch and str(ch["status"]) != "done":
            task_rows.append(
                (
                    _build_task_id(CH_PIPELINE, "ch_lookup", comp_id),
                    CH_PIPELINE,
                    "ch_lookup",
                    comp_id,
                    "pending",
                    int(ch["retries"] or 0),
                    _ts(ch["next_run_at"]),
                    _task_payload_json(
                        {
                            "comp_id": comp_id,
                            "company_name": str(row["company_name"] or ""),
                            "company_number": str(row["company_number"] or ""),
                            "homepage": str(row["homepage"] or ""),
                            "domain": str(row["domain"] or ""),
                        }
                    ),
                    _ts(ch["updated_at"]),
                )
            )
        if gmap and str(gmap["status"]) != "done":
            task_rows.append(
                (
                    _build_task_id(CH_PIPELINE, "ch_gmap", comp_id),
                    CH_PIPELINE,
                    "ch_gmap",
                    comp_id,
                    "pending",
                    int(gmap["retries"] or 0),
                    _ts(gmap["next_run_at"]),
                    _task_payload_json(
                        {
                            "comp_id": comp_id,
                            "company_name": str(row["company_name"] or ""),
                            "company_number": str(row["company_number"] or ""),
                            "homepage": str(row["homepage"] or ""),
                            "domain": str(row["domain"] or ""),
                        }
                    ),
                    _ts(gmap["updated_at"]),
                )
            )
        if firecrawl and str(firecrawl["status"]) != "done":
            task_rows.append(
                (
                    _build_task_id(CH_PIPELINE, "ch_firecrawl", comp_id),
                    CH_PIPELINE,
                    "ch_firecrawl",
                    comp_id,
                    "pending",
                    int(firecrawl["retries"] or 0),
                    _ts(firecrawl["next_run_at"]),
                    _task_payload_json(
                        {
                            "comp_id": comp_id,
                            "company_name": str(row["company_name"] or ""),
                            "company_number": str(row["company_number"] or ""),
                            "homepage": str(row["homepage"] or ""),
                            "domain": str(row["domain"] or ""),
                        }
                    ),
                    _ts(firecrawl["updated_at"]),
                )
            )
    source_rows = [
        (
            str(row["source_path"] or ""),
            str(row["fingerprint"] or ""),
            int(row["total_rows"] or 0),
            _ts(row["updated_at"]),
        )
        for row in source_files
    ]
    with db.transaction() as conn:
        with conn.cursor() as cur:
            for chunk in _chunked(company_rows):
                cur.executemany(
                    """
                    INSERT INTO england_ch_companies(
                        comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                        phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                        firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(comp_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """,
                    chunk,
                )
            if source_rows:
                cur.executemany(
                    """
                    INSERT INTO england_ch_source_files(source_path, fingerprint, total_rows, updated_at)
                    VALUES(%s, %s, %s, %s)
                    ON CONFLICT(source_path) DO UPDATE SET
                        fingerprint = EXCLUDED.fingerprint,
                        total_rows = EXCLUDED.total_rows,
                        updated_at = EXCLUDED.updated_at
                    """,
                    source_rows,
                )
            if task_rows:
                cur.executemany(
                    """
                    INSERT INTO england_cluster_tasks(
                        task_id, pipeline, task_type, entity_id, status, retries, next_run_at,
                        payload_json, updated_at, created_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        retries = EXCLUDED.retries,
                        next_run_at = EXCLUDED.next_run_at,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    [(*row, row[-1]) for row in task_rows],
                )


def _migrate_firecrawl_state(db: ClusterDb, output_root: Path) -> None:
    cache_db = output_root / "firecrawl_cache.db"
    key_db = output_root / "cache" / "firecrawl_keys.db"
    cache_rows = _load_sqlite_rows(cache_db, "SELECT * FROM firecrawl_domain_cache ORDER BY domain")
    key_rows = _load_sqlite_rows(key_db, "SELECT * FROM keys ORDER BY idx, key")
    domain_rows = [
        (
            str(row["domain"] or ""),
            str(row["status"] or ""),
            Jsonb(_json_list(row["emails_json"])),
            _ts(row["next_retry_at"]) if str(row["next_retry_at"] or "").strip() else None,
            "",
            None,
            str(row["last_error"] or ""),
            _ts(row["updated_at"]),
        )
        for row in cache_rows
    ]
    db_rows = []
    for row in key_rows:
        key = str(row["key"] or "").strip()
        if not key:
            continue
        db_rows.append(
            (
                hashlib.sha256(key.encode("utf-8")).hexdigest(),
                key,
                str(row["state"] or ""),
                int(row["failure_count"] or 0),
                int(row["in_flight"] or 0),
                _ts(row["cooldown_until"]) if row["cooldown_until"] else None,
                "",
                None,
                str(row["disabled_reason"] or ""),
                _ts(row["last_used"]) if row["last_used"] else None,
                datetime.now(timezone.utc),
            )
        )
    with db.transaction() as conn:
        with conn.cursor() as cur:
            if domain_rows:
                cur.executemany(
                    """
                    INSERT INTO england_firecrawl_domain_cache(
                        domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(domain) DO UPDATE SET
                        status = EXCLUDED.status,
                        emails_json = EXCLUDED.emails_json,
                        next_retry_at = EXCLUDED.next_retry_at,
                        last_error = EXCLUDED.last_error,
                        updated_at = EXCLUDED.updated_at
                    """,
                    domain_rows,
                )
            if db_rows:
                cur.executemany(
                    """
                    INSERT INTO england_firecrawl_keys(
                        key_hash, key_value, state, failure_count, in_flight, cooldown_until,
                        lease_owner, lease_expires_at, disabled_reason, last_used_at, updated_at
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(key_hash) DO UPDATE SET
                        key_value = EXCLUDED.key_value,
                        state = EXCLUDED.state,
                        failure_count = EXCLUDED.failure_count,
                        in_flight = EXCLUDED.in_flight,
                        cooldown_until = EXCLUDED.cooldown_until,
                        disabled_reason = EXCLUDED.disabled_reason,
                        last_used_at = EXCLUDED.last_used_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    db_rows,
                )


def _migrate_delivery_history(db: ClusterDb, output_root: Path) -> None:
    delivery_root = output_root / "delivery"
    if not delivery_root.exists():
        return
    run_rows: list[tuple] = []
    item_rows: list[tuple] = []
    for day_dir in sorted(delivery_root.glob("England_day*")):
        if not day_dir.is_dir():
            continue
        summary_path = day_dir / "summary.json"
        csv_path = day_dir / "companies.csv"
        if not csv_path.exists():
            csv_path = day_dir / f"companies_{int(day_dir.name[-3:]):03d}.csv"
        keys_path = day_dir / "keys.txt"
        if not summary_path.exists() or not csv_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        day_number = int(summary.get("day", 0) or 0)
        run_rows.append(
            (
                day_number,
                int(summary.get("baseline_day", 0) or 0),
                int(summary.get("total_current_companies", 0) or 0),
                int(summary.get("delta_companies", 0) or 0),
                _ts(summary.get("generated_at", "")),
                keys_path.read_text(encoding="utf-8") if keys_path.exists() else "",
                Jsonb(summary),
            )
        )
        with csv_path.open("r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp)
            for index, row in enumerate(reader, start=1):
                item_rows.append(
                    (
                        day_number,
                        str(row.get("company_name", "")).strip(),
                        str(row.get("ceo", "")).strip(),
                        str(row.get("homepage", "")).strip(),
                        str(row.get("domain", "")).strip(),
                        str(row.get("phone", "")).strip(),
                        str(row.get("emails", "")).strip(),
                        index,
                    )
                )
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
            for day_number, company_name, ceo, homepage, domain, phone, emails_text, row_index in item_rows:
                cur.execute("SELECT run_id FROM england_delivery_runs WHERE day_number = %s", (day_number,))
                run_id = cur.fetchone()["run_id"]
                if row_index == 1:
                    cur.execute("DELETE FROM england_delivery_items WHERE run_id = %s", (run_id,))
                cur.execute(
                    """
                    INSERT INTO england_delivery_items(
                        run_id, company_name, ceo, homepage, domain, phone, emails_text, row_index
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, company_name, ceo, homepage, domain, phone, emails_text, row_index),
                )


def migrate_england_history(db: ClusterDb, output_root: Path) -> None:
    initialize_schema(db)
    _migrate_dnb(db, output_root)
    _migrate_companies_house(db, output_root)
    _migrate_firecrawl_state(db, output_root)
    _migrate_delivery_history(db, output_root)
