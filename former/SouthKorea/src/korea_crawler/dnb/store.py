"""韩国 DNB sqlite 存储与快照导出。"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from korea_crawler.dnb.client import _is_valid_kr_segment
from korea_crawler.dnb.domain_quality import assess_company_domain
from korea_crawler.dnb.domain_quality import normalize_website_url
from korea_crawler.dnb.naming import has_korean_company_name
from korea_crawler.dnb.naming import resolve_company_name
from korea_crawler.dnb.runtime.snapshot_export import export_jsonl_snapshots as export_snapshot_files
from korea_crawler.snov.client import extract_domain


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(float(seconds), 0.0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _dump_json_list(items: list[str]) -> str:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _merge_text(current: str, incoming: str) -> str:
    return str(incoming or "").strip() or str(current or "").strip()


def _merge_domain(website: str, current_domain: str) -> str:
    return extract_domain(website) or str(current_domain or "").strip()


def _record_score(record: dict[str, object]) -> tuple[int, int, int, int]:
    emails = record.get("emails", [])
    email_list = emails if isinstance(emails, list) else []
    has_email = 1 if email_list else 0
    has_ceo = 1 if str(record.get("ceo", "")).strip() else 0
    has_name = 1 if str(record.get("company_name", "")).strip() else 0
    return has_email, len(email_list), has_ceo, has_name


@dataclass(slots=True)
class SegmentCursor:
    segment_id: str
    industry_path: str
    country_iso_two_code: str
    region_name: str
    city_name: str
    expected_count: int
    next_page: int
    total_pages: int


@dataclass(slots=True)
class GMapTask:
    duns: str
    company_name_en: str
    city: str
    region: str
    country: str
    dnb_website: str
    retries: int


@dataclass(slots=True)
class SiteTask:
    duns: str
    company_name_en_dnb: str
    website: str
    retries: int


@dataclass(slots=True)
class SnovTask:
    duns: str
    domain: str
    retries: int


class DnbKoreaStore:
    """基于 sqlite 的韩国 DNB 断点与队列存储。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = self._new_connection()
        self._init_schema()
        self._repair_runtime_state()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout = 30000;")
        return conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS dnb_discovery_queue (
                    segment_id TEXT PRIMARY KEY,
                    industry_path TEXT NOT NULL,
                    country_iso_two_code TEXT NOT NULL,
                    region_name TEXT NOT NULL DEFAULT '',
                    city_name TEXT NOT NULL DEFAULT '',
                    expected_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dnb_segments (
                    segment_id TEXT PRIMARY KEY,
                    industry_path TEXT NOT NULL,
                    country_iso_two_code TEXT NOT NULL,
                    region_name TEXT NOT NULL DEFAULT '',
                    city_name TEXT NOT NULL DEFAULT '',
                    expected_count INTEGER NOT NULL DEFAULT 0,
                    next_page INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS companies (
                    duns TEXT PRIMARY KEY,
                    company_name_en_dnb TEXT NOT NULL DEFAULT '',
                    company_name_url TEXT NOT NULL DEFAULT '',
                    key_principal TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT 'Republic Of Korea',
                    postal_code TEXT NOT NULL DEFAULT '',
                    sales_revenue TEXT NOT NULL DEFAULT '',
                    dnb_website TEXT NOT NULL DEFAULT '',
                    website TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    website_source TEXT NOT NULL DEFAULT '',
                    company_name_en_gmap TEXT NOT NULL DEFAULT '',
                    company_name_en_site TEXT NOT NULL DEFAULT '',
                    company_name_resolved TEXT NOT NULL DEFAULT '',
                    site_evidence_url TEXT NOT NULL DEFAULT '',
                    site_evidence_quote TEXT NOT NULL DEFAULT '',
                    site_confidence REAL NOT NULL DEFAULT 0.0,
                    phone TEXT NOT NULL DEFAULT '',
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    detail_done INTEGER NOT NULL DEFAULT 0,
                    gmap_status TEXT NOT NULL DEFAULT '',
                    site_name_status TEXT NOT NULL DEFAULT '',
                    snov_status TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gmap_queue (
                    duns TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS site_queue (
                    duns TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS snov_queue (
                    duns TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS final_companies (
                    duns TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    ceo TEXT NOT NULL,
                    homepage TEXT NOT NULL DEFAULT '',
                    contact_emails TEXT NOT NULL DEFAULT '[]',
                    domain TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._conn.commit()

    def _repair_runtime_state(self) -> None:
        now = _utc_now()
        with self._lock:
            self._prune_invalid_kr_segments_locked()
            self._sanitize_invalid_gmap_names_locked(now)
            self._conn.execute(
                "UPDATE dnb_discovery_queue SET status = 'pending', updated_at = ? WHERE status = 'running'",
                (now,),
            )
            for table in ("gmap_queue", "site_queue", "snov_queue"):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running'",
                    (now, now),
                )
            self._conn.commit()

    def _prune_invalid_kr_segments_locked(self) -> None:
        for table in ("dnb_discovery_queue", "dnb_segments"):
            rows = self._conn.execute(
                f"SELECT segment_id, region_name FROM {table} WHERE country_iso_two_code = 'kr'"
            ).fetchall()
            invalid_ids = [
                str(row["segment_id"])
                for row in rows
                if not _is_valid_kr_segment(str(row["region_name"]))
            ]
            if not invalid_ids:
                continue
            placeholders = ",".join("?" for _ in invalid_ids)
            self._conn.execute(
                f"DELETE FROM {table} WHERE segment_id IN ({placeholders})",
                invalid_ids,
            )

    def _sanitize_invalid_gmap_names_locked(self, updated_at: str) -> None:
        rows = self._conn.execute(
            """
            SELECT duns, company_name_en_dnb, company_name_en_gmap, company_name_en_site
            FROM companies
            WHERE company_name_en_gmap != ''
            """
        ).fetchall()
        for row in rows:
            gmap_name = str(row["company_name_en_gmap"]).strip()
            if has_korean_company_name(gmap_name):
                continue
            resolved = resolve_company_name(
                company_name_en_dnb=str(row["company_name_en_dnb"]).strip(),
                company_name_local_gmap="",
                company_name_local_site=str(row["company_name_en_site"]).strip(),
            )
            self._conn.execute(
                """
                UPDATE companies
                SET company_name_en_gmap = '', company_name_resolved = ?, updated_at = ?
                WHERE duns = ?
                """,
                (resolved, updated_at, str(row["duns"]).strip()),
            )

    def ensure_discovery_seed(self, segment_id: str, expected_count: int = 0) -> None:
        with self._lock:
            queued = self._scalar("SELECT COUNT(*) FROM dnb_discovery_queue")
            stable = self._scalar("SELECT COUNT(*) FROM dnb_segments")
            if queued > 0 or stable > 0:
                return
            now = _utc_now()
            self._conn.execute(
                """
                INSERT INTO dnb_discovery_queue(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, status, updated_at
                ) VALUES(?, 'construction', 'kr', '', '', ?, 'pending', ?)
                """,
                (segment_id, max(int(expected_count), 0), now),
            )
            self._conn.commit()

    def ensure_discovery_seeds(self, rows: list[dict[str, object]]) -> None:
        """确保全站行业种子已入 discovery 队列。"""
        if not rows:
            return
        now = _utc_now()
        with self._lock:
            payloads: list[tuple[str, str, str, str, str, int, str]] = []
            for row in rows:
                segment_id = str(row.get("segment_id", "")).strip()
                industry_path = str(row.get("industry_path", "")).strip()
                country_iso_two_code = str(row.get("country_iso_two_code", "")).strip()
                region_name = str(row.get("region_name", "")).strip()
                city_name = str(row.get("city_name", "")).strip()
                if (
                    not segment_id
                    or not industry_path
                    or not country_iso_two_code
                    or (
                        country_iso_two_code == "kr"
                        and not _is_valid_kr_segment(region_name)
                    )
                ):
                    continue
                payloads.append(
                    (
                        segment_id,
                        industry_path,
                        country_iso_two_code,
                        region_name,
                        city_name,
                        max(int(row.get("expected_count", 0) or 0), 0),
                        now,
                    )
                )
            if not payloads:
                return
            self._conn.executemany(
                """
                INSERT INTO dnb_discovery_queue(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, status, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(segment_id) DO NOTHING
                """,
                payloads,
            )
            self._conn.commit()

    def claim_discovery_node(self) -> sqlite3.Row | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count
                FROM dnb_discovery_queue
                WHERE status = 'pending'
                ORDER BY updated_at ASC, rowid ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE dnb_discovery_queue SET status = 'running', updated_at = ? WHERE segment_id = ?",
                (_utc_now(), str(row["segment_id"])),
            )
            self._conn.commit()
            return row

    def enqueue_discovery_node(
        self,
        *,
        segment_id: str,
        industry_path: str,
        country_iso_two_code: str,
        region_name: str,
        city_name: str,
        expected_count: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO dnb_discovery_queue(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, status, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(segment_id) DO NOTHING
                """,
                (
                    segment_id,
                    industry_path,
                    country_iso_two_code,
                    region_name,
                    city_name,
                    max(int(expected_count), 0),
                    now,
                ),
            )
            self._conn.commit()

    def mark_discovery_node_done(self, segment_id: str, *, expected_count: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE dnb_discovery_queue SET status = 'done', expected_count = ?, updated_at = ? WHERE segment_id = ?",
                (max(int(expected_count), 0), _utc_now(), segment_id),
            )
            self._conn.commit()

    def discovery_done(self) -> bool:
        with self._lock:
            total = self._scalar("SELECT COUNT(*) FROM dnb_discovery_queue")
            remaining = self._scalar("SELECT COUNT(*) FROM dnb_discovery_queue WHERE status != 'done'")
            return total > 0 and remaining == 0

    def has_discovery_work(self) -> bool:
        with self._lock:
            return self._scalar("SELECT COUNT(*) FROM dnb_discovery_queue WHERE status != 'done'") > 0

    def segment_count(self) -> int:
        with self._lock:
            return self._scalar("SELECT COUNT(*) FROM dnb_segments")

    def upsert_leaf_segment(
        self,
        *,
        segment_id: str,
        industry_path: str,
        country_iso_two_code: str,
        region_name: str,
        city_name: str,
        expected_count: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO dnb_segments(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, next_page, status, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 1, 'pending', ?)
                ON CONFLICT(segment_id) DO UPDATE SET
                    industry_path = excluded.industry_path,
                    country_iso_two_code = excluded.country_iso_two_code,
                    region_name = excluded.region_name,
                    city_name = excluded.city_name,
                    expected_count = excluded.expected_count,
                    updated_at = excluded.updated_at
                """,
                (
                    segment_id,
                    industry_path,
                    country_iso_two_code,
                    region_name,
                    city_name,
                    max(int(expected_count), 0),
                    now,
                ),
            )
            self._conn.commit()

    def claim_segment(self, page_size: int) -> SegmentCursor | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, next_page
                FROM dnb_segments
                WHERE status = 'pending'
                ORDER BY updated_at ASC, rowid ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE dnb_segments SET status = 'running', updated_at = ? WHERE segment_id = ?",
                (_utc_now(), str(row["segment_id"])),
            )
            self._conn.commit()
            expected = max(int(row["expected_count"]), 0)
            total_pages = max(1, math.ceil(expected / max(page_size, 1)))
            return SegmentCursor(
                segment_id=str(row["segment_id"]),
                industry_path=str(row["industry_path"]),
                country_iso_two_code=str(row["country_iso_two_code"]),
                region_name=str(row["region_name"]),
                city_name=str(row["city_name"]),
                expected_count=expected,
                next_page=max(int(row["next_page"]), 1),
                total_pages=total_pages,
            )

    def next_segment(self, page_size: int) -> SegmentCursor | None:
        return self.claim_segment(page_size)

    def advance_segment(self, segment_id: str, next_page: int, total_pages: int) -> None:
        status = "done" if next_page > total_pages else "pending"
        with self._lock:
            self._conn.execute(
                "UPDATE dnb_segments SET next_page = ?, status = ?, updated_at = ? WHERE segment_id = ?",
                (max(next_page, 1), status, _utc_now(), segment_id),
            )
            self._conn.commit()

    def reset_segment(self, segment_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE dnb_segments SET status = 'pending', updated_at = ? WHERE segment_id = ?",
                (_utc_now(), segment_id),
            )
            self._conn.commit()

    def reset_discovery_node(self, segment_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE dnb_discovery_queue SET status = 'pending', updated_at = ? WHERE segment_id = ?",
                (_utc_now(), segment_id),
            )
            self._conn.commit()

    def is_company_detail_done(self, duns: str) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT detail_done FROM companies WHERE duns = ?", (duns,)).fetchone()
            return row is not None and int(row["detail_done"]) == 1

    def upsert_company_listing(self, record: dict[str, str]) -> None:
        self._upsert_company(
            duns=record["duns"],
            company_name_en_dnb=record["company_name_en_dnb"],
            company_name_url=record["company_name_url"],
            address=record["address"],
            city=record["city"],
            region=record["region"],
            country=record["country"] or "Republic Of Korea",
            postal_code=record["postal_code"],
            sales_revenue=record["sales_revenue"],
        )

    def upsert_company_detail(self, record: dict[str, str]) -> None:
        self._upsert_company(
            duns=record["duns"],
            company_name_en_dnb=record["company_name_en_dnb"],
            company_name_url=record["company_name_url"],
            key_principal=record["key_principal"],
            address=record["address"],
            city=record["city"],
            region=record["region"],
            country=record["country"] or "Republic Of Korea",
            postal_code=record["postal_code"],
            sales_revenue=record["sales_revenue"],
            dnb_website=record["dnb_website"],
            phone=record["phone"],
            detail_done=True,
        )

    def _upsert_company(
        self,
        *,
        duns: str,
        company_name_en_dnb: str = "",
        company_name_url: str = "",
        key_principal: str = "",
        address: str = "",
        city: str = "",
        region: str = "",
        country: str = "Republic Of Korea",
        postal_code: str = "",
        sales_revenue: str = "",
        dnb_website: str = "",
        phone: str = "",
        detail_done: bool | None = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(duns)
            row = {
                "duns": duns.strip(),
                "company_name_en_dnb": _merge_text(current.get("company_name_en_dnb", "") if current else "", company_name_en_dnb),
                "company_name_url": _merge_text(current.get("company_name_url", "") if current else "", company_name_url),
                "key_principal": _merge_text(current.get("key_principal", "") if current else "", key_principal),
                "address": _merge_text(current.get("address", "") if current else "", address),
                "city": _merge_text(current.get("city", "") if current else "", city),
                "region": _merge_text(current.get("region", "") if current else "", region),
                "country": _merge_text(current.get("country", "Republic Of Korea") if current else "Republic Of Korea", country),
                "postal_code": _merge_text(current.get("postal_code", "") if current else "", postal_code),
                "sales_revenue": _merge_text(current.get("sales_revenue", "") if current else "", sales_revenue),
                "dnb_website": _merge_text(current.get("dnb_website", "") if current else "", dnb_website),
                "website": current.get("website", "") if current else "",
                "domain": current.get("domain", "") if current else "",
                "website_source": current.get("website_source", "") if current else "",
                "company_name_en_gmap": current.get("company_name_en_gmap", "") if current else "",
                "company_name_en_site": current.get("company_name_en_site", "") if current else "",
                "company_name_resolved": current.get("company_name_resolved", "") if current else "",
                "site_evidence_url": current.get("site_evidence_url", "") if current else "",
                "site_evidence_quote": current.get("site_evidence_quote", "") if current else "",
                "site_confidence": float(current.get("site_confidence", 0.0) if current else 0.0),
                "phone": _merge_text(current.get("phone", "") if current else "", phone),
                "emails_json": _dump_json_list(current.get("emails", []) if current else []),
                "detail_done": int(current.get("detail_done", 0) if current else 0),
                "gmap_status": current.get("gmap_status", "") if current else "",
                "site_name_status": current.get("site_name_status", "") if current else "",
                "snov_status": current.get("snov_status", "") if current else "",
                "last_error": current.get("last_error", "") if current else "",
                "updated_at": now,
            }
            if detail_done is True:
                row["detail_done"] = 1
            row["company_name_resolved"] = resolve_company_name(
                company_name_en_dnb=row["company_name_en_dnb"],
                company_name_local_gmap=row["company_name_en_gmap"],
                company_name_local_site=row["company_name_en_site"],
            )
            self._conn.execute(
                """
                INSERT INTO companies(
                    duns, company_name_en_dnb, company_name_url, key_principal, address, city, region, country,
                    postal_code, sales_revenue, dnb_website, website, domain, website_source, company_name_en_gmap,
                    company_name_en_site, company_name_resolved, site_evidence_url, site_evidence_quote, site_confidence,
                    phone, emails_json, detail_done, gmap_status, site_name_status, snov_status, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(duns) DO UPDATE SET
                    company_name_en_dnb = excluded.company_name_en_dnb,
                    company_name_url = excluded.company_name_url,
                    key_principal = excluded.key_principal,
                    address = excluded.address,
                    city = excluded.city,
                    region = excluded.region,
                    country = excluded.country,
                    postal_code = excluded.postal_code,
                    sales_revenue = excluded.sales_revenue,
                    dnb_website = excluded.dnb_website,
                    website = excluded.website,
                    domain = excluded.domain,
                    website_source = excluded.website_source,
                    company_name_en_gmap = excluded.company_name_en_gmap,
                    company_name_en_site = excluded.company_name_en_site,
                    company_name_resolved = excluded.company_name_resolved,
                    site_evidence_url = excluded.site_evidence_url,
                    site_evidence_quote = excluded.site_evidence_quote,
                    site_confidence = excluded.site_confidence,
                    phone = excluded.phone,
                    emails_json = excluded.emails_json,
                    detail_done = excluded.detail_done,
                    gmap_status = excluded.gmap_status,
                    site_name_status = excluded.site_name_status,
                    snov_status = excluded.snov_status,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    row["duns"], row["company_name_en_dnb"], row["company_name_url"], row["key_principal"],
                    row["address"], row["city"], row["region"], row["country"], row["postal_code"],
                    row["sales_revenue"], row["dnb_website"], row["website"], row["domain"], row["website_source"],
                    row["company_name_en_gmap"], row["company_name_en_site"], row["company_name_resolved"],
                    row["site_evidence_url"], row["site_evidence_quote"], row["site_confidence"], row["phone"],
                    row["emails_json"], row["detail_done"], row["gmap_status"], row["site_name_status"],
                    row["snov_status"], row["last_error"], row["updated_at"],
                ),
            )
            self._conn.commit()
        if row["detail_done"] == 1:
            self.enqueue_gmap_task(duns)
        self.refresh_final_company(duns)

    def get_company(self, duns: str) -> dict[str, object] | None:
        with self._lock:
            return self._fetch_company_locked(duns)

    def _fetch_company_locked(self, duns: str) -> dict[str, object] | None:
        row = self._conn.execute("SELECT * FROM companies WHERE duns = ?", (duns,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["emails"] = _parse_json_list(str(data.get("emails_json", "[]")))
        return data

    def enqueue_gmap_task(self, duns: str) -> None:
        self._enqueue_task("gmap_queue", duns)

    def enqueue_site_task(self, duns: str) -> None:
        self._enqueue_task("site_queue", duns)

    def enqueue_snov_task(self, duns: str) -> None:
        self._enqueue_task("snov_queue", duns)

    def _enqueue_task(self, table: str, duns: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                f"""
                INSERT INTO {table}(duns, status, retries, next_run_at, last_error, updated_at)
                VALUES(?, 'pending', 0, ?, '', ?)
                ON CONFLICT(duns) DO UPDATE SET
                    status = CASE WHEN {table}.status = 'done' THEN {table}.status ELSE 'pending' END,
                    next_run_at = CASE WHEN {table}.status = 'done' THEN {table}.next_run_at ELSE excluded.next_run_at END,
                    updated_at = excluded.updated_at,
                    last_error = CASE WHEN {table}.status = 'done' THEN {table}.last_error ELSE '' END
                """,
                (duns, now, now),
            )
            self._conn.commit()

    def claim_gmap_task(self) -> GMapTask | None:
        row = self._claim_task("gmap_queue")
        if row is None:
            return None
        return GMapTask(
            duns=str(row["duns"]),
            company_name_en=str(row["company_name_en_dnb"]),
            city=str(row["city"]),
            region=str(row["region"]),
            country=str(row["country"]),
            dnb_website=str(row["dnb_website"]),
            retries=int(row["retries"]),
        )

    def claim_site_task(self) -> SiteTask | None:
        row = self._claim_task("site_queue")
        if row is None:
            return None
        return SiteTask(
            duns=str(row["duns"]),
            company_name_en_dnb=str(row["company_name_en_dnb"]),
            website=str(row["website"]),
            retries=int(row["retries"]),
        )

    def claim_snov_task(self) -> SnovTask | None:
        row = self._claim_task("snov_queue")
        if row is None:
            return None
        return SnovTask(duns=str(row["duns"]), domain=str(row["domain"]), retries=int(row["retries"]))

    def _claim_task(self, table: str) -> sqlite3.Row | None:
        now = _utc_now()
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT q.duns, q.retries, c.company_name_en_dnb, c.city, c.region, c.country, c.dnb_website, c.website, c.domain
                FROM {table} q
                JOIN companies c ON c.duns = q.duns
                WHERE q.status = 'pending' AND q.next_run_at <= ?
                ORDER BY q.next_run_at ASC, q.updated_at ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                f"UPDATE {table} SET status = 'running', updated_at = ? WHERE duns = ?",
                (now, str(row["duns"])),
            )
            self._conn.commit()
            return row

    def mark_gmap_done(
        self,
        *,
        duns: str,
        website: str,
        source: str,
        company_name_local_gmap: str = "",
        phone: str = "",
    ) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            final_website = normalize_website_url(
                website.strip() or str(current.get("website", "")).strip() or str(current.get("dnb_website", "")).strip()
            )
            final_source = source.strip() or str(current.get("website_source", "")).strip() or ("dnb" if final_website else "")
            final_domain = _merge_domain(final_website, str(current.get("domain", "")).strip())
            final_gmap_name = str(current.get("company_name_en_gmap", "")).strip() or company_name_local_gmap.strip()
            final_phone = str(current.get("phone", "")).strip() or phone.strip()
            if final_source == "gmap" and not has_korean_company_name(final_gmap_name):
                final_gmap_name = ""
            assessment = assess_company_domain(
                str(current.get("company_name_en_dnb", "")).strip(),
                final_website,
                source=final_source or "gmap",
            )
            if assessment.blocked:
                final_website = ""
                final_source = ""
                final_domain = ""
                final_gmap_name = ""
                final_phone = ""
            resolved = resolve_company_name(
                company_name_en_dnb=str(current.get("company_name_en_dnb", "")).strip(),
                company_name_local_gmap=final_gmap_name,
                company_name_local_site=str(current.get("company_name_en_site", "")).strip(),
            )
            self._conn.execute(
                """
                UPDATE companies
                SET website = ?, domain = ?, website_source = ?, company_name_en_gmap = ?, company_name_resolved = ?,
                    phone = ?, gmap_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                (final_website, final_domain, final_source, final_gmap_name, resolved, final_phone, now, duns),
            )
            self._conn.execute(
                "UPDATE gmap_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (now, duns),
            )
            self._conn.commit()
        if final_website and not final_gmap_name:
            self.enqueue_site_task(duns)
        if final_domain:
            self.enqueue_snov_task(duns)
        self.refresh_final_company(duns)

    def mark_site_done(
        self,
        *,
        duns: str,
        company_name_local: str,
        evidence_url: str = "",
        evidence_quote: str = "",
        confidence: float = 0.0,
    ) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            site_name = company_name_local.strip() or str(current.get("company_name_en_site", "")).strip()
            resolved = resolve_company_name(
                company_name_en_dnb=str(current.get("company_name_en_dnb", "")).strip(),
                company_name_local_gmap=str(current.get("company_name_en_gmap", "")).strip(),
                company_name_local_site=site_name,
            )
            self._conn.execute(
                """
                UPDATE companies
                SET company_name_en_site = ?, company_name_resolved = ?, site_evidence_url = ?, site_evidence_quote = ?,
                    site_confidence = ?, site_name_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                (
                    site_name,
                    resolved,
                    evidence_url.strip(),
                    evidence_quote.strip(),
                    max(0.0, min(float(confidence), 1.0)),
                    _utc_now(),
                    duns,
                ),
            )
            self._conn.execute(
                "UPDATE site_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (_utc_now(), duns),
            )
            self._conn.commit()
        self.refresh_final_company(duns)

    def mark_snov_done(self, *, duns: str, emails: list[str]) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            merged: list[str] = []
            for item in [*current.get("emails", []), *emails]:
                value = str(item or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
            domain = current.get("domain", "")
            if not domain:
                domain = extract_domain(str(current.get("website", "") or current.get("dnb_website", "")))
            self._conn.execute(
                "UPDATE companies SET emails_json = ?, domain = ?, snov_status = 'done', updated_at = ? WHERE duns = ?",
                (_dump_json_list(merged), domain, _utc_now(), duns),
            )
            self._conn.execute(
                "UPDATE snov_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (_utc_now(), duns),
            )
            self._conn.commit()
        self.refresh_final_company(duns)

    def defer_gmap_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("gmap_queue", duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_site_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("site_queue", duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_snov_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task("snov_queue", duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def _defer_task(self, table: str, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ? WHERE duns = ?",
                (max(retries, 0), _utc_after(delay_seconds), error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def mark_gmap_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed("gmap_queue", "gmap_status", duns=duns, error_text=error_text)

    def mark_site_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed("site_queue", "site_name_status", duns=duns, error_text=error_text)

    def mark_snov_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed("snov_queue", "snov_status", duns=duns, error_text=error_text)
        self.refresh_final_company(duns)

    def _mark_failed(self, table: str, field_name: str, *, duns: str, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.execute(
                f"UPDATE companies SET {field_name} = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def refresh_final_company(self, duns: str) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            company_name = str(current.get("company_name_resolved", "")).strip()
            ceo = str(current.get("key_principal", "")).strip()
            homepage = normalize_website_url(str(current.get("website", "") or current.get("dnb_website", "")).strip())
            emails = current.get("emails", [])
            assessment = assess_company_domain(
                str(current.get("company_name_en_dnb", "")).strip(),
                homepage,
                source=str(current.get("website_source", "")).strip() or ("dnb" if homepage else ""),
            )
            if company_name and ceo and emails and homepage:
                if assessment.blocked:
                    self._conn.execute("DELETE FROM final_companies WHERE duns = ?", (duns,))
                    self._conn.commit()
                    return
                self._conn.execute(
                    """
                    INSERT INTO final_companies(duns, company_name, ceo, homepage, contact_emails, domain, phone, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        ceo = excluded.ceo,
                        homepage = excluded.homepage,
                        contact_emails = excluded.contact_emails,
                        domain = excluded.domain,
                        phone = excluded.phone,
                        updated_at = excluded.updated_at
                    """,
                    (
                        duns, company_name, ceo, homepage, _dump_json_list(emails),
                        str(current.get("domain", "")).strip(), str(current.get("phone", "")).strip(), _utc_now(),
                    ),
                )
            else:
                self._conn.execute("DELETE FROM final_companies WHERE duns = ?", (duns,))
            self._conn.commit()

    def get_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "segments_total": self._scalar("SELECT COUNT(*) FROM dnb_segments"),
                "segments_done": self._scalar("SELECT COUNT(*) FROM dnb_segments WHERE status = 'done'"),
                "companies_total": self._scalar("SELECT COUNT(*) FROM companies"),
                "companies_detail_done": self._scalar("SELECT COUNT(*) FROM companies WHERE detail_done = 1"),
                "gmap_pending": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'pending'"),
                "gmap_running": self._scalar("SELECT COUNT(*) FROM gmap_queue WHERE status = 'running'"),
                "site_pending": self._scalar("SELECT COUNT(*) FROM site_queue WHERE status = 'pending'"),
                "site_running": self._scalar("SELECT COUNT(*) FROM site_queue WHERE status = 'running'"),
                "snov_pending": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'pending'"),
                "snov_running": self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'running'"),
                "final_total": self._scalar("SELECT COUNT(*) FROM final_companies"),
            }

    def export_jsonl_snapshots(self, output_dir: Path) -> None:
        export_snapshot_files(self._db_path, output_dir)

    def requeue_stale_running_tasks(self, *, older_than_seconds: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(int(older_than_seconds), 1))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        now = _utc_now()
        total = 0
        with self._lock:
            discovery_rows = self._conn.execute("SELECT segment_id FROM dnb_discovery_queue WHERE status = 'running' AND updated_at <= ?", (cutoff,)).fetchall()
            if discovery_rows:
                self._conn.execute("UPDATE dnb_discovery_queue SET status = 'pending', updated_at = ? WHERE status = 'running' AND updated_at <= ?", (now, cutoff))
                total += len(discovery_rows)
            for table in ("gmap_queue", "site_queue", "snov_queue"):
                rows = self._conn.execute(f"SELECT duns FROM {table} WHERE status = 'running' AND updated_at <= ?", (cutoff,)).fetchall()
                if not rows:
                    continue
                self._conn.execute(f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running' AND updated_at <= ?", (now, now, cutoff))
                total += len(rows)
            self._conn.commit()
        return total

    def _scalar(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row is not None else 0
