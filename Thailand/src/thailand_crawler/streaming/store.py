"""流式主流程 sqlite 存储。"""

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

from thailand_crawler.domain_quality import assess_company_domain
from thailand_crawler.models import CompanyRecord
from thailand_crawler.models import Segment
from thailand_crawler.streaming.llm_client import resolve_company_name
from thailand_crawler.snov import extract_domain
from thailand_crawler.snov import is_excluded_company_domain


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _utc_after(seconds: float) -> str:
    delay = max(float(seconds), 0.0)
    target = datetime.now(timezone.utc) + timedelta(seconds=delay)
    return target.replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or '[]'))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or '').strip()
        if text and text not in result:
            result.append(text)
    return result


def _dump_json_list(items: list[str]) -> str:
    cleaned: list[str] = []
    for item in items:
        text = str(item or '').strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return json.dumps(cleaned, ensure_ascii=False)


def _merge_text(current: str, incoming: str) -> str:
    return str(incoming or '').strip() or str(current or '').strip()


def _merge_domain(website: str, current_domain: str) -> str:
    extracted = extract_domain(website)
    candidate = extracted or str(current_domain or '').strip()
    return '' if is_excluded_company_domain(candidate) else candidate


@dataclass(slots=True)
class SegmentCursor:
    segment: Segment
    next_page: int
    total_pages: int


@dataclass(slots=True)
class WebsiteTask:
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
    company_name_en: str
    website: str
    retries: int


@dataclass(slots=True)
class SnovTask:
    duns: str
    domain: str
    retries: int


class StreamStore:
    """基于 sqlite 的流式断点与队列存储。"""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = self._new_connection()
        self._init_schema()
        self._repair_runtime_state()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        return conn

    def reconnect(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = self._new_connection()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

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
                    phone TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT 'Thailand',
                    postal_code TEXT NOT NULL DEFAULT '',
                    dnb_website TEXT NOT NULL DEFAULT '',
                    website TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    website_source TEXT NOT NULL DEFAULT '',
                    company_name_th_site TEXT NOT NULL DEFAULT '',
                    company_name_resolved TEXT NOT NULL DEFAULT '',
                    site_evidence_url TEXT NOT NULL DEFAULT '',
                    site_evidence_quote TEXT NOT NULL DEFAULT '',
                    site_confidence REAL NOT NULL DEFAULT 0.0,
                    emails_json TEXT NOT NULL DEFAULT '[]',
                    detail_done INTEGER NOT NULL DEFAULT 0,
                    website_status TEXT NOT NULL DEFAULT '',
                    site_name_status TEXT NOT NULL DEFAULT '',
                    snov_status TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS website_queue (
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
                    company_manager TEXT NOT NULL,
                    contact_emails TEXT NOT NULL,
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
            self._conn.execute(
                "UPDATE dnb_discovery_queue SET status = 'pending', updated_at = ? WHERE status = 'running'",
                (now,),
            )
            for table in ('website_queue', 'site_queue', 'snov_queue'):
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running'",
                    (now, now),
                )
            self._conn.commit()

    def ensure_discovery_seed(self, segment: Segment) -> None:
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
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    segment.segment_id,
                    segment.industry_path,
                    segment.country_iso_two_code,
                    segment.region_name,
                    segment.city_name,
                    max(int(segment.expected_count), 0),
                    now,
                ),
            )
            self._conn.commit()

    def ensure_discovery_seeds(self, segments: list[Segment]) -> int:
        now = _utc_now()
        inserted = 0
        with self._lock:
            for segment in segments:
                cursor = self._conn.execute(
                    """
                    INSERT INTO dnb_discovery_queue(
                        segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, status, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)
                    ON CONFLICT(segment_id) DO NOTHING
                    """,
                    (
                        segment.segment_id,
                        segment.industry_path,
                        segment.country_iso_two_code,
                        segment.region_name,
                        segment.city_name,
                        max(int(segment.expected_count), 0),
                        now,
                    ),
                )
                if int(cursor.rowcount or 0) > 0:
                    inserted += 1
            self._conn.commit()
        return inserted

    def claim_discovery_node(self) -> Segment | None:
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
                (_utc_now(), str(row['segment_id'])),
            )
            self._conn.commit()
            return Segment(
                industry_path=str(row['industry_path']),
                country_iso_two_code=str(row['country_iso_two_code']),
                region_name=str(row['region_name']),
                city_name=str(row['city_name']),
                expected_count=int(row['expected_count'] or 0),
                segment_type='city' if str(row['city_name']) else ('region' if str(row['region_name']) else 'country'),
            )

    def enqueue_discovery_node(self, segment: Segment) -> None:
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
                    segment.segment_id,
                    segment.industry_path,
                    segment.country_iso_two_code,
                    segment.region_name,
                    segment.city_name,
                    max(int(segment.expected_count), 0),
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

    def upsert_leaf_segment(self, segment: Segment) -> None:
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
                    segment.segment_id,
                    segment.industry_path,
                    segment.country_iso_two_code,
                    segment.region_name,
                    segment.city_name,
                    max(int(segment.expected_count), 0),
                    now,
                ),
            )
            self._conn.commit()

    def replace_segments(self, segments: list[Segment]) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute('DELETE FROM dnb_segments')
            for segment in segments:
                self._conn.execute(
                    """
                    INSERT INTO dnb_segments(
                        segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, next_page, status, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, 1, 'pending', ?)
                    """,
                    (
                        segment.segment_id,
                        segment.industry_path,
                        segment.country_iso_two_code,
                        segment.region_name,
                        segment.city_name,
                        max(int(segment.expected_count), 0),
                        now,
                    ),
                )
            self._conn.commit()

    def has_segments(self) -> bool:
        with self._lock:
            row = self._conn.execute('SELECT COUNT(*) AS count FROM dnb_segments').fetchone()
            return int(row['count']) > 0

    def next_segment(self, page_size: int) -> SegmentCursor | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, next_page
                FROM dnb_segments
                WHERE status != 'done'
                ORDER BY rowid ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            expected = max(int(row['expected_count']), 0)
            total_pages = max(1, math.ceil(expected / max(page_size, 1)))
            segment = Segment(
                industry_path=str(row['industry_path']),
                country_iso_two_code=str(row['country_iso_two_code']),
                region_name=str(row['region_name']),
                city_name=str(row['city_name']),
                expected_count=expected,
                segment_type='city' if str(row['city_name']) else ('region' if str(row['region_name']) else 'country'),
            )
            return SegmentCursor(segment=segment, next_page=max(int(row['next_page']), 1), total_pages=total_pages)

    def advance_segment(self, segment_id: str, next_page: int, total_pages: int) -> None:
        now = _utc_now()
        status = 'done' if next_page > total_pages else 'pending'
        with self._lock:
            self._conn.execute(
                'UPDATE dnb_segments SET next_page = ?, status = ?, updated_at = ? WHERE segment_id = ?',
                (max(next_page, 1), status, now, segment_id),
            )
            self._conn.commit()

    def all_segments_done(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS count FROM dnb_segments WHERE status != 'done'").fetchone()
            return int(row['count']) == 0
    def is_company_detail_done(self, duns: str) -> bool:
        with self._lock:
            row = self._conn.execute('SELECT detail_done FROM companies WHERE duns = ?', (duns,)).fetchone()
            return row is not None and int(row['detail_done']) == 1

    def upsert_company_listing(self, record: CompanyRecord) -> None:
        self.upsert_company(
            duns=record.duns,
            company_name_en_dnb=record.company_name,
            company_name_url=record.company_name_url,
            address=record.address,
            city=record.city,
            region=record.region,
            country=record.country or 'Thailand',
            postal_code=record.postal_code,
        )

    def upsert_company_detail(self, record: CompanyRecord) -> None:
        self.upsert_company(
            duns=record.duns,
            company_name_en_dnb=record.company_name,
            company_name_url=record.company_name_url,
            key_principal=record.key_principal,
            phone=record.phone,
            address=record.address,
            city=record.city,
            region=record.region,
            country=record.country or 'Thailand',
            postal_code=record.postal_code,
            dnb_website=record.website,
            detail_done=True,
        )

    def upsert_company(
        self,
        *,
        duns: str,
        company_name_en_dnb: str = '',
        company_name_url: str = '',
        key_principal: str = '',
        phone: str = '',
        address: str = '',
        city: str = '',
        region: str = '',
        country: str = 'Thailand',
        postal_code: str = '',
        dnb_website: str = '',
        detail_done: bool | None = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(duns)
            row = {
                'duns': duns.strip(),
                'company_name_en_dnb': _merge_text(current.get('company_name_en_dnb', '') if current else '', company_name_en_dnb),
                'company_name_url': _merge_text(current.get('company_name_url', '') if current else '', company_name_url),
                'key_principal': _merge_text(current.get('key_principal', '') if current else '', key_principal),
                'phone': _merge_text(current.get('phone', '') if current else '', phone),
                'address': _merge_text(current.get('address', '') if current else '', address),
                'city': _merge_text(current.get('city', '') if current else '', city),
                'region': _merge_text(current.get('region', '') if current else '', region),
                'country': _merge_text(current.get('country', 'Thailand') if current else 'Thailand', country or 'Thailand'),
                'postal_code': _merge_text(current.get('postal_code', '') if current else '', postal_code),
                'dnb_website': _merge_text(current.get('dnb_website', '') if current else '', dnb_website),
                'website': current.get('website', '') if current else '',
                'domain': current.get('domain', '') if current else '',
                'website_source': current.get('website_source', '') if current else '',
                'company_name_th_site': current.get('company_name_th_site', '') if current else '',
                'company_name_resolved': current.get('company_name_resolved', '') if current else '',
                'site_evidence_url': current.get('site_evidence_url', '') if current else '',
                'site_evidence_quote': current.get('site_evidence_quote', '') if current else '',
                'site_confidence': float(current.get('site_confidence', 0.0) if current else 0.0),
                'emails_json': _dump_json_list(current.get('emails', []) if current else []),
                'detail_done': int(current.get('detail_done', 0) if current else 0),
                'website_status': current.get('website_status', '') if current else '',
                'site_name_status': current.get('site_name_status', '') if current else '',
                'snov_status': current.get('snov_status', '') if current else '',
                'last_error': current.get('last_error', '') if current else '',
                'updated_at': now,
            }
            if detail_done is True:
                row['detail_done'] = 1
            row['company_name_resolved'] = resolve_company_name(row['company_name_en_dnb'], row['company_name_th_site'])
            self._conn.execute(
                """
                INSERT INTO companies(
                    duns, company_name_en_dnb, company_name_url, key_principal, phone, address, city, region, country,
                    postal_code, dnb_website, website, domain, website_source, company_name_th_site, company_name_resolved,
                    site_evidence_url, site_evidence_quote, site_confidence, emails_json, detail_done, website_status,
                    site_name_status, snov_status, last_error, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(duns) DO UPDATE SET
                    company_name_en_dnb = excluded.company_name_en_dnb,
                    company_name_url = excluded.company_name_url,
                    key_principal = excluded.key_principal,
                    phone = excluded.phone,
                    address = excluded.address,
                    city = excluded.city,
                    region = excluded.region,
                    country = excluded.country,
                    postal_code = excluded.postal_code,
                    dnb_website = excluded.dnb_website,
                    website = excluded.website,
                    domain = excluded.domain,
                    website_source = excluded.website_source,
                    company_name_th_site = excluded.company_name_th_site,
                    company_name_resolved = excluded.company_name_resolved,
                    site_evidence_url = excluded.site_evidence_url,
                    site_evidence_quote = excluded.site_evidence_quote,
                    site_confidence = excluded.site_confidence,
                    emails_json = excluded.emails_json,
                    detail_done = excluded.detail_done,
                    website_status = excluded.website_status,
                    site_name_status = excluded.site_name_status,
                    snov_status = excluded.snov_status,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    row['duns'], row['company_name_en_dnb'], row['company_name_url'], row['key_principal'], row['phone'],
                    row['address'], row['city'], row['region'], row['country'], row['postal_code'], row['dnb_website'],
                    row['website'], row['domain'], row['website_source'], row['company_name_th_site'], row['company_name_resolved'],
                    row['site_evidence_url'], row['site_evidence_quote'], row['site_confidence'], row['emails_json'], row['detail_done'],
                    row['website_status'], row['site_name_status'], row['snov_status'], row['last_error'], row['updated_at'],
                ),
            )
            self._conn.commit()
        if key_principal.strip() or row['key_principal']:
            self.enqueue_website_task(duns)
        self.refresh_final_company(duns)

    def save_site_result(
        self,
        *,
        duns: str,
        company_name_th: str,
        evidence_url: str = '',
        evidence_quote: str = '',
        confidence: float = 0.0,
    ) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            resolved = resolve_company_name(current.get('company_name_en_dnb', ''), company_name_th)
            self._conn.execute(
                """
                UPDATE companies
                SET company_name_th_site = ?,
                    company_name_resolved = ?,
                    site_evidence_url = ?,
                    site_evidence_quote = ?,
                    site_confidence = ?,
                    site_name_status = 'done',
                    updated_at = ?
                WHERE duns = ?
                """,
                (
                    company_name_th.strip(),
                    resolved,
                    evidence_url.strip(),
                    evidence_quote.strip(),
                    max(0.0, min(float(confidence), 1.0)),
                    _utc_now(),
                    duns,
                ),
            )
            self._conn.commit()
        self.refresh_final_company(duns)

    def save_snov_result(self, *, duns: str, emails: list[str]) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            merged: list[str] = []
            for item in [*current.get('emails', []), *emails]:
                value = str(item or '').strip().lower()
                if value and value not in merged:
                    merged.append(value)
            domain = current.get('domain', '')
            website = current.get('website', '')
            dnb_website = current.get('dnb_website', '')
            if not domain and website:
                domain = extract_domain(website)
            if not domain and dnb_website:
                domain = extract_domain(dnb_website)
            self._conn.execute(
                """
                UPDATE companies
                SET emails_json = ?, domain = ?, snov_status = 'done', updated_at = ?
                WHERE duns = ?
                """,
                (_dump_json_list(merged), domain, _utc_now(), duns),
            )
            self._conn.commit()
        self.refresh_final_company(duns)
    def get_stats(self) -> dict[str, int]:
        with self._lock:
            stats = {
                'segments_total': self._scalar("SELECT COUNT(*) FROM dnb_segments"),
                'segments_done': self._scalar("SELECT COUNT(*) FROM dnb_segments WHERE status = 'done'"),
                'companies_total': self._scalar("SELECT COUNT(*) FROM companies"),
                'companies_detail_done': self._scalar("SELECT COUNT(*) FROM companies WHERE detail_done = 1"),
                'website_pending': self._scalar("SELECT COUNT(*) FROM website_queue WHERE status = 'pending'"),
                'website_running': self._scalar("SELECT COUNT(*) FROM website_queue WHERE status = 'running'"),
                'site_pending': self._scalar("SELECT COUNT(*) FROM site_queue WHERE status = 'pending'"),
                'site_running': self._scalar("SELECT COUNT(*) FROM site_queue WHERE status = 'running'"),
                'snov_pending': self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'pending'"),
                'snov_running': self._scalar("SELECT COUNT(*) FROM snov_queue WHERE status = 'running'"),
                'final_total': self._scalar("SELECT COUNT(*) FROM final_companies"),
            }
        return stats

    def _scalar(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row is not None else 0

    def _domain_company_count_locked(self, domain: str, *, exclude_duns: str = '') -> int:
        value = str(domain or '').strip().lower()
        if not value:
            return 0
        if exclude_duns.strip():
            row = self._conn.execute(
                "SELECT COUNT(*) FROM companies WHERE lower(domain) = ? AND duns != ?",
                (value, exclude_duns.strip()),
            ).fetchone()
            return int(row[0]) if row is not None else 0
        row = self._conn.execute("SELECT COUNT(*) FROM companies WHERE lower(domain) = ?", (value,)).fetchone()
        return int(row[0]) if row is not None else 0

    def _fetch_company_locked(self, duns: str) -> dict[str, object] | None:
        row = self._conn.execute('SELECT * FROM companies WHERE duns = ?', (duns,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data['emails'] = _parse_json_list(str(data.get('emails_json', '[]')))
        return data
    def enqueue_website_task(self, duns: str) -> None:
        self._enqueue_task('website_queue', duns)

    def enqueue_site_task(self, duns: str) -> None:
        self._enqueue_task('site_queue', duns)

    def enqueue_snov_task(self, duns: str) -> None:
        self._enqueue_task('snov_queue', duns)

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

    def claim_website_task(self) -> WebsiteTask | None:
        row = self._claim_task('website_queue')
        if row is None:
            return None
        return WebsiteTask(
            duns=str(row['duns']),
            company_name_en=str(row['company_name_en_dnb']),
            city=str(row['city']),
            region=str(row['region']),
            country=str(row['country']),
            dnb_website=str(row['dnb_website']),
            retries=int(row['retries']),
        )

    def claim_site_task(self) -> SiteTask | None:
        row = self._claim_task('site_queue')
        if row is None:
            return None
        return SiteTask(
            duns=str(row['duns']),
            company_name_en=str(row['company_name_en_dnb']),
            website=str(row['website']),
            retries=int(row['retries']),
        )

    def claim_snov_task(self) -> SnovTask | None:
        row = self._claim_task('snov_queue')
        if row is None:
            return None
        return SnovTask(duns=str(row['duns']), domain=str(row['domain']), retries=int(row['retries']))

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
                (now, str(row['duns'])),
            )
            self._conn.commit()
            return row

    def mark_website_done(self, *, duns: str, website: str, source: str, company_name_th: str = '', phone: str = '') -> None:
        now = _utc_now()
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            final_website = website.strip() or str(current.get('website', '')).strip() or str(current.get('dnb_website', '')).strip()
            blocked = False
            if is_excluded_company_domain(extract_domain(final_website)):
                final_website = ''
                final_source = ''
                blocked = True
            else:
                final_source = source.strip() or str(current.get('website_source', '')).strip() or ('dnb' if str(current.get('dnb_website', '')).strip() and final_website == str(current.get('dnb_website', '')).strip() else '')
            final_domain = _merge_domain(final_website, str(current.get('domain', '')).strip())
            if final_domain:
                assessment = assess_company_domain(
                    company_name=str(current.get('company_name_en_dnb', '')).strip(),
                    domain=final_domain,
                    shared_count=self._domain_company_count_locked(final_domain, exclude_duns=duns) + 1,
                )
                if assessment.blocked:
                    final_website = ''
                    final_domain = ''
                    final_source = ''
                    blocked = True
            incoming_company_name_th = '' if blocked else company_name_th.strip()
            incoming_phone = '' if blocked else phone.strip()
            final_company_name_th = str(current.get('company_name_th_site', '')).strip() or incoming_company_name_th
            final_phone = str(current.get('phone', '')).strip() or incoming_phone
            final_resolved = resolve_company_name(str(current.get('company_name_en_dnb', '')).strip(), final_company_name_th)
            self._conn.execute(
                """
                UPDATE companies
                SET website = ?,
                    domain = ?,
                    website_source = ?,
                    company_name_th_site = ?,
                    company_name_resolved = ?,
                    phone = ?,
                    website_status = 'done',
                    updated_at = ?
                WHERE duns = ?
                """,
                (final_website, final_domain, final_source, final_company_name_th, final_resolved, final_phone, now, duns),
            )
            self._conn.execute(
                "UPDATE website_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (now, duns),
            )
            self._conn.commit()
        if final_website and not final_company_name_th:
            self.enqueue_site_task(duns)
        if final_domain:
            self.enqueue_snov_task(duns)
        self.refresh_final_company(duns)

    def mark_site_done(self, *, duns: str, company_name_th: str, evidence_url: str = '', evidence_quote: str = '', confidence: float = 0.0) -> None:
        self.save_site_result(
            duns=duns,
            company_name_th=company_name_th,
            evidence_url=evidence_url,
            evidence_quote=evidence_quote,
            confidence=confidence,
        )
        with self._lock:
            self._conn.execute(
                "UPDATE site_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (_utc_now(), duns),
            )
            self._conn.commit()

    def mark_snov_done(self, *, duns: str, emails: list[str]) -> None:
        self.save_snov_result(duns=duns, emails=emails)
        with self._lock:
            self._conn.execute(
                "UPDATE snov_queue SET status = 'done', updated_at = ?, last_error = '' WHERE duns = ?",
                (_utc_now(), duns),
            )
            self._conn.commit()

    def defer_website_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task('website_queue', duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_site_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task('site_queue', duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def defer_snov_task(self, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        self._defer_task('snov_queue', duns=duns, retries=retries, delay_seconds=delay_seconds, error_text=error_text)

    def _defer_task(self, table: str, *, duns: str, retries: int, delay_seconds: float, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'pending', retries = ?, next_run_at = ?, last_error = ?, updated_at = ? WHERE duns = ?",
                (max(retries, 0), _utc_after(delay_seconds), error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()
    def mark_website_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed('website_queue', duns=duns, error_text=error_text)
        with self._lock:
            self._conn.execute(
                "UPDATE companies SET website_status = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def mark_site_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed('site_queue', duns=duns, error_text=error_text)
        with self._lock:
            self._conn.execute(
                "UPDATE companies SET site_name_status = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def mark_snov_failed(self, *, duns: str, error_text: str) -> None:
        self._mark_failed('snov_queue', duns=duns, error_text=error_text)
        with self._lock:
            self._conn.execute(
                "UPDATE companies SET snov_status = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()
        self.refresh_final_company(duns)

    def _mark_failed(self, table: str, *, duns: str, error_text: str) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = 'failed', last_error = ?, updated_at = ? WHERE duns = ?",
                (error_text[:500], _utc_now(), duns),
            )
            self._conn.commit()

    def refresh_final_company(self, duns: str) -> None:
        with self._lock:
            current = self._fetch_company_locked(duns)
            if not current:
                return
            company_name = str(current.get('company_name_resolved', '')).strip()
            company_manager = str(current.get('key_principal', '')).strip()
            emails = current.get('emails', [])
            if company_name and company_manager and emails:
                self._conn.execute(
                    """
                    INSERT INTO final_companies(duns, company_name, company_manager, contact_emails, domain, phone, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(duns) DO UPDATE SET
                        company_name = excluded.company_name,
                        company_manager = excluded.company_manager,
                        contact_emails = excluded.contact_emails,
                        domain = excluded.domain,
                        phone = excluded.phone,
                        updated_at = excluded.updated_at
                    """,
                    (
                        duns,
                        company_name,
                        company_manager,
                        _dump_json_list(emails),
                        str(current.get('domain', '')).strip(),
                        str(current.get('phone', '')).strip(),
                        _utc_now(),
                    ),
                )
            else:
                self._conn.execute('DELETE FROM final_companies WHERE duns = ?', (duns,))
            self._conn.commit()

    def get_company(self, duns: str) -> dict[str, object] | None:
        with self._lock:
            return self._fetch_company_locked(duns)

    def get_final_company(self, duns: str) -> dict[str, object] | None:
        with self._lock:
            row = self._conn.execute(
                'SELECT duns, company_name, company_manager, contact_emails, domain, phone FROM final_companies WHERE duns = ?',
                (duns,),
            ).fetchone()
            if row is None:
                return None
            return {
                'duns': str(row['duns']),
                'company_name': str(row['company_name']),
                'company_manager': str(row['company_manager']),
                'contact_emails': _parse_json_list(str(row['contact_emails'])),
                'domain': str(row['domain']),
                'phone': str(row['phone']),
            }

    def fetch_final_rows(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                'SELECT duns, company_name, company_manager, contact_emails, domain, phone FROM final_companies ORDER BY updated_at ASC'
            ).fetchall()
            return [
                {
                    'duns': str(row['duns']),
                    'company_name': str(row['company_name']),
                    'key_principal': str(row['company_manager']),
                    'emails': _parse_json_list(str(row['contact_emails'])),
                    'domain': str(row['domain']),
                    'phone': str(row['phone']),
                }
                for row in rows
            ]

    def requeue_stale_running_tasks(self, *, older_than_seconds: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(int(older_than_seconds), 1))).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        now = _utc_now()
        total = 0
        with self._lock:
            discovery_rows = self._conn.execute(
                "SELECT segment_id FROM dnb_discovery_queue WHERE status = 'running' AND updated_at <= ?",
                (cutoff,),
            ).fetchall()
            if discovery_rows:
                self._conn.execute(
                    "UPDATE dnb_discovery_queue SET status = 'pending', updated_at = ? WHERE status = 'running' AND updated_at <= ?",
                    (now, cutoff),
                )
                total += len(discovery_rows)
            for table in ('website_queue', 'site_queue', 'snov_queue'):
                rows = self._conn.execute(
                    f"SELECT duns FROM {table} WHERE status = 'running' AND updated_at <= ?",
                    (cutoff,),
                ).fetchall()
                if not rows:
                    continue
                self._conn.execute(
                    f"UPDATE {table} SET status = 'pending', next_run_at = ?, updated_at = ? WHERE status = 'running' AND updated_at <= ?",
                    (now, now, cutoff),
                )
                total += len(rows)
            self._conn.commit()
        return total
