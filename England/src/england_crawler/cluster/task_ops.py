"""England 集群任务处理 mixin。"""

from __future__ import annotations

from psycopg.types.json import Jsonb

from england_crawler.dnb.domain_quality import assess_company_domain
from england_crawler.dnb.domain_quality import normalize_website_url
from england_crawler.dnb.naming import resolve_company_name
from england_crawler.google_maps.pipeline import clean_homepage
from england_crawler.snov.client import extract_domain
from england_crawler.snov.client import is_valid_domain


class ClusterTaskOpsMixin:
    """England 集群任务写回逻辑。"""

    def _ensure_jsonb_value(self, value):
        if isinstance(value, Jsonb):
            return value
        return self._dump_json_list(self._parse_json_list(value))

    def _complete_dnb_discovery_locked(self, cur, task, result):
        expected_count = int(result.get("expected_count", 0) or 0)
        payload = dict(task.payload)
        cur.execute(
            """
            UPDATE england_dnb_discovery_nodes
            SET expected_count = %s, task_status = 'done', updated_at = %s
            WHERE segment_id = %s
            """,
            (expected_count, self._utc_now(), task.entity_id),
        )
        if expected_count > 0:
            cur.execute(
                """
                INSERT INTO england_dnb_segments(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name,
                    expected_count, next_page, task_status, task_retries, updated_at
                ) VALUES(%s, %s, %s, %s, %s, %s, 1, 'pending', 0, %s)
                ON CONFLICT(segment_id) DO UPDATE SET
                    expected_count = EXCLUDED.expected_count,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    task.entity_id,
                    str(payload.get("industry_path", "")),
                    str(payload.get("country_iso_two_code", "")),
                    str(payload.get("region_name", "")),
                    str(payload.get("city_name", "")),
                    expected_count,
                    self._utc_now(),
                ),
            )
            self._upsert_task_locked(
                cur,
                pipeline="england_dnb",
                task_type="dnb_list_segment",
                entity_id=task.entity_id,
                payload={**payload, "expected_count": expected_count, "next_page": 1, "page_size": 50},
                force_pending=True,
            )
        children = result.get("children", [])
        for child in children if isinstance(children, list) else []:
            child_payload = dict(child or {})
            segment_id = str(child_payload.get("segment_id", "")).strip()
            if not segment_id:
                continue
            cur.execute(
                """
                INSERT INTO england_dnb_discovery_nodes(
                    segment_id, industry_path, country_iso_two_code, region_name, city_name,
                    expected_count, task_status, task_retries, updated_at
                ) VALUES(%s, %s, %s, %s, %s, %s, 'pending', 0, %s)
                ON CONFLICT(segment_id) DO NOTHING
                """,
                (
                    segment_id,
                    str(child_payload.get("industry_path", "")),
                    str(child_payload.get("country_iso_two_code", "")),
                    str(child_payload.get("region_name", "")),
                    str(child_payload.get("city_name", "")),
                    int(child_payload.get("expected_count", 0) or 0),
                    self._utc_now(),
                ),
            )
            self._upsert_task_locked(
                cur,
                pipeline="england_dnb",
                task_type="dnb_discovery",
                entity_id=segment_id,
                payload=child_payload,
                force_pending=True,
            )
        return "done"

    def _complete_dnb_list_segment_locked(self, cur, task, result):
        rows = result.get("rows", [])
        if not isinstance(rows, list):
            rows = []
        for item in rows:
            incoming = dict(item or {})
            duns = str(incoming.get("duns", "")).strip()
            if not duns:
                continue
            row = self._merge_dnb_company_row(
                current=self._fetch_dnb_company_locked(cur, duns),
                incoming=incoming,
                mark_detail_done=None,
            )
            self._upsert_dnb_company_locked(cur, row)
            if not bool(row.get("detail_done")):
                self._upsert_task_locked(
                    cur,
                    pipeline="england_dnb",
                    task_type="dnb_detail",
                    entity_id=duns,
                    payload=self._build_dnb_detail_payload(row),
                    force_pending=True,
                )
                self._update_dnb_task_state_locked(
                    cur,
                    duns=duns,
                    task_type="dnb_detail",
                    status="pending",
                    retries=0,
                )
        next_page = int(result.get("next_page", task.payload.get("next_page", 1)) or 1)
        total_pages = int(result.get("total_pages", task.payload.get("total_pages", 1)) or 1)
        done = bool(result.get("done"))
        cur.execute(
            """
            UPDATE england_dnb_segments
            SET next_page = %s, task_status = %s, updated_at = %s
            WHERE segment_id = %s
            """,
            (next_page, "done" if done else "pending", self._utc_now(), task.entity_id),
        )
        if done:
            return "done"
        self._reschedule_task_locked(
            cur,
            task.task_id,
            retries=task.retries,
            delay_seconds=0.0,
            error_text="",
            payload={**task.payload, "next_page": next_page, "total_pages": total_pages},
        )
        return "rescheduled"

    def _complete_dnb_detail_locked(self, cur, task, result):
        row = self._merge_dnb_company_row(
            current=self._fetch_dnb_company_locked(cur, task.entity_id),
            incoming=result,
            mark_detail_done=True,
        )
        row["detail_task_status"] = "done"
        row["detail_task_retries"] = task.retries
        self._upsert_dnb_company_locked(cur, row)
        self._upsert_task_locked(
            cur,
            pipeline="england_dnb",
            task_type="dnb_gmap",
            entity_id=task.entity_id,
            payload=self._build_dnb_gmap_payload(row),
            force_pending=True,
        )
        self._update_dnb_task_state_locked(
            cur,
            duns=task.entity_id,
            task_type="dnb_gmap",
            status="pending",
            retries=0,
        )
        return "done"

    def _complete_dnb_gmap_locked(self, cur, task, result):
        current = self._fetch_dnb_company_locked(cur, task.entity_id)
        if current is None:
            raise RuntimeError("DNB 公司不存在。")
        final_website = normalize_website_url(
            str(result.get("website", "")).strip()
            or str(current.get("website", "")).strip()
            or str(current.get("dnb_website", "")).strip()
        )
        final_source = str(result.get("source", "")).strip() or str(current.get("website_source", "")).strip()
        if not final_source:
            final_source = "gmap" if str(result.get("website", "")).strip() else ("dnb" if final_website else "")
        assessment = assess_company_domain(
            str(current.get("company_name_en_dnb", "")).strip(),
            final_website,
            source=final_source or "gmap",
        )
        if assessment.blocked:
            final_website = ""
            final_source = ""
            final_domain = ""
            gmap_name = ""
            final_phone = ""
        else:
            final_domain = extract_domain(final_website) or str(current.get("domain", "")).strip()
            gmap_name = str(result.get("company_name_local_gmap", "")).strip() or str(current.get("company_name_en_gmap", "")).strip()
            final_phone = str(current.get("phone", "")).strip() or str(result.get("phone", "")).strip()
        row = {
            **current,
            "website": final_website,
            "domain": final_domain,
            "website_source": final_source,
            "company_name_en_gmap": gmap_name,
            "company_name_resolved": resolve_company_name(
                company_name_en_dnb=str(current.get("company_name_en_dnb", "")).strip(),
                company_name_local_gmap=gmap_name,
                company_name_local_site=str(current.get("company_name_en_site", "")).strip(),
            ),
            "phone": final_phone,
            "gmap_task_status": "done",
            "gmap_task_retries": task.retries,
            "updated_at": self._utc_now(),
        }
        self._upsert_dnb_company_locked(cur, row)
        if final_domain:
            self._upsert_task_locked(
                cur,
                pipeline="england_dnb",
                task_type="dnb_firecrawl",
                entity_id=task.entity_id,
                payload=self._build_dnb_firecrawl_payload(row),
                force_pending=True,
            )
            self._update_dnb_task_state_locked(
                cur,
                duns=task.entity_id,
                task_type="dnb_firecrawl",
                status="pending",
                retries=0,
            )
        return "done"

    def _complete_dnb_firecrawl_locked(self, cur, task, result):
        self._apply_firecrawl_done_locked(cur, task, self._parse_json_list(result.get("emails", [])))
        return "done"

    def _complete_ch_lookup_locked(self, cur, task, result):
        current = self._fetch_ch_company_locked(cur, task.entity_id)
        if current is None:
            raise RuntimeError("Companies House 公司不存在。")
        row = {
            **current,
            "company_number": str(result.get("company_number", "")).strip(),
            "company_status": str(result.get("company_status", "")).strip(),
            "ceo": str(result.get("ceo", "")).strip(),
            "ch_task_status": "done",
            "ch_task_retries": task.retries,
            "last_error": "",
            "updated_at": self._utc_now(),
        }
        self._upsert_ch_company_locked(cur, row)
        self._queue_ch_firecrawl_if_ready_locked(cur, row)
        return "done"

    def _complete_ch_gmap_locked(self, cur, task, result):
        current = self._fetch_ch_company_locked(cur, task.entity_id)
        if current is None:
            raise RuntimeError("Companies House 公司不存在。")
        homepage = clean_homepage(str(result.get("homepage", "")).strip())
        row = {
            **current,
            "homepage": homepage,
            "domain": extract_domain(homepage),
            "phone": str(result.get("phone", "")).strip(),
            "gmap_task_status": "done",
            "gmap_task_retries": task.retries,
            "last_error": "",
            "updated_at": self._utc_now(),
        }
        self._upsert_ch_company_locked(cur, row)
        self._queue_ch_firecrawl_if_ready_locked(cur, row)
        return "done"

    def _complete_ch_firecrawl_locked(self, cur, task, result):
        self._apply_firecrawl_done_locked(cur, task, self._parse_json_list(result.get("emails", [])))
        return "done"

    def _apply_task_failure_side_effect(self, cur, task, error_text: str, retry_delay_seconds: float, terminal: bool):
        message = self._clip_text(error_text)
        if task.task_type.startswith("dnb_"):
            self._update_dnb_task_state_locked(
                cur, duns=task.entity_id, task_type=task.task_type, status="failed" if terminal else "pending", retries=task.retries + 1, last_error=message
            )
        elif task.task_type.startswith("ch_"):
            self._update_ch_task_state_locked(
                cur, comp_id=task.entity_id, task_type=task.task_type, status="failed" if terminal else "pending", retries=task.retries + 1, last_error=message
            )
        if task.task_type in {"dnb_firecrawl", "ch_firecrawl"}:
            domain = str(task.payload.get("domain", "")).strip().lower() or extract_domain(str(task.payload.get("homepage", "")).strip())
            if domain:
                cur.execute(
                    """
                    INSERT INTO england_firecrawl_domain_cache(
                        domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at
                    ) VALUES(%s, 'pending', '[]'::jsonb, %s, '', NULL, %s, %s)
                    ON CONFLICT(domain) DO UPDATE SET
                        status = 'pending',
                        next_retry_at = excluded.next_retry_at,
                        lease_owner = '',
                        lease_expires_at = NULL,
                        last_error = excluded.last_error,
                        updated_at = excluded.updated_at
                    """,
                    (domain, self._utc_after(retry_delay_seconds), message, self._utc_now()),
                )

    def _apply_firecrawl_done_locked(self, cur, task, emails: list[str]):
        domain = str(task.payload.get("domain", "")).strip().lower()
        if task.task_type == "dnb_firecrawl":
            current = self._fetch_dnb_company_locked(cur, task.entity_id)
            if current is None:
                return
            merged = self._parse_json_list(current.get("emails_json", [])) + emails
            row = {
                **current,
                "domain": domain or str(current.get("domain", "")).strip() or extract_domain(str(current.get("website", "")).strip()),
                "emails_json": self._dump_json_list(merged),
                "firecrawl_task_status": "done",
                "firecrawl_task_retries": task.retries,
                "last_error": "",
                "updated_at": self._utc_now(),
            }
            self._upsert_dnb_company_locked(cur, row)
        else:
            current = self._fetch_ch_company_locked(cur, task.entity_id)
            if current is None:
                return
            merged = self._parse_json_list(current.get("emails_json", [])) + emails
            row = {
                **current,
                "domain": domain or str(current.get("domain", "")).strip() or extract_domain(str(current.get("homepage", "")).strip()),
                "emails_json": self._dump_json_list(merged),
                "firecrawl_task_status": "done",
                "firecrawl_task_retries": task.retries,
                "last_error": "",
                "updated_at": self._utc_now(),
            }
            self._upsert_ch_company_locked(cur, row)
        if domain:
            cur.execute(
                """
                INSERT INTO england_firecrawl_domain_cache(
                    domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at
                ) VALUES(%s, 'done', %s, NULL, '', NULL, '', %s)
                ON CONFLICT(domain) DO UPDATE SET
                    status = 'done',
                    emails_json = excluded.emails_json,
                    next_retry_at = NULL,
                    lease_owner = '',
                    lease_expires_at = NULL,
                    last_error = '',
                    updated_at = excluded.updated_at
                """,
                (domain, self._dump_json_list(emails), self._utc_now()),
            )

    def _mark_entity_firecrawl_failed_locked(self, cur, task, error_text: str):
        if task.task_type == "dnb_firecrawl":
            self._update_dnb_task_state_locked(cur, duns=task.entity_id, task_type="dnb_firecrawl", status="failed", retries=task.retries, last_error=error_text)
        else:
            self._update_ch_task_state_locked(cur, comp_id=task.entity_id, task_type="ch_firecrawl", status="failed", retries=task.retries, last_error=error_text)

    def _mark_task_done_locked(self, cur, task_id: str):
        cur.execute(
            """
            UPDATE england_cluster_tasks
            SET status = 'done', lease_owner = '', lease_expires_at = NULL, updated_at = %s, last_error = ''
            WHERE task_id = %s
            """,
            (self._utc_now(), task_id),
        )

    def _reschedule_task_locked(self, cur, task_id: str, *, retries: int, delay_seconds: float, error_text: str, payload: dict[str, object]):
        cur.execute(
            """
            UPDATE england_cluster_tasks
            SET status = 'pending',
                retries = %s,
                next_run_at = %s,
                lease_owner = '',
                lease_expires_at = NULL,
                last_error = %s,
                payload_json = %s,
                updated_at = %s
            WHERE task_id = %s
            """,
            (retries, self._utc_after(delay_seconds), self._clip_text(error_text), Jsonb(payload), self._utc_now(), task_id),
        )

    def _upsert_task_locked(self, cur, *, pipeline: str, task_type: str, entity_id: str, payload: dict[str, object], force_pending: bool) -> int:
        task_id = self._build_task_id(pipeline, task_type, entity_id)
        cur.execute("SELECT status FROM england_cluster_tasks WHERE task_id = %s", (task_id,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """
                INSERT INTO england_cluster_tasks(
                    task_id, pipeline, task_type, entity_id, status, retries,
                    next_run_at, lease_owner, lease_expires_at, last_error, payload_json, created_at, updated_at
                ) VALUES(%s, %s, %s, %s, 'pending', 0, %s, '', NULL, '', %s, %s, %s)
                """,
                (task_id, pipeline, task_type, entity_id, self._utc_now(), Jsonb(payload), self._utc_now(), self._utc_now()),
            )
            return 1
        if force_pending and str(row["status"]) != "done":
            cur.execute(
                """
                UPDATE england_cluster_tasks
                SET status = 'pending', next_run_at = %s, lease_owner = '', lease_expires_at = NULL,
                    last_error = '', payload_json = %s, updated_at = %s
                WHERE task_id = %s
                """,
                (self._utc_now(), Jsonb(payload), self._utc_now(), task_id),
            )
        return 0

    def _build_dnb_detail_payload(self, row: dict[str, object]) -> dict[str, object]:
        return {
            "duns": str(row.get("duns", "")).strip(),
            "company_name_en_dnb": str(row.get("company_name_en_dnb", "")).strip(),
            "company_name_url": str(row.get("company_name_url", "")).strip(),
            "address": str(row.get("address", "")).strip(),
            "city": str(row.get("city", "")).strip(),
            "region": str(row.get("region", "")).strip(),
            "country": str(row.get("country", "")).strip(),
            "postal_code": str(row.get("postal_code", "")).strip(),
            "sales_revenue": str(row.get("sales_revenue", "")).strip(),
        }

    def _build_dnb_gmap_payload(self, row: dict[str, object]) -> dict[str, object]:
        return {
            "duns": str(row.get("duns", "")).strip(),
            "company_name_en": str(row.get("company_name_en_dnb", "")).strip(),
            "city": str(row.get("city", "")).strip(),
            "region": str(row.get("region", "")).strip(),
            "country": str(row.get("country", "")).strip(),
            "dnb_website": str(row.get("dnb_website", "")).strip(),
        }

    def _build_dnb_firecrawl_payload(self, row: dict[str, object]) -> dict[str, object]:
        homepage = str(row.get("website", "")).strip() or str(row.get("dnb_website", "")).strip()
        return {
            "duns": str(row.get("duns", "")).strip(),
            "company_name_en_dnb": str(row.get("company_name_en_dnb", "")).strip(),
            "homepage": homepage,
            "domain": str(row.get("domain", "")).strip() or extract_domain(homepage),
        }

    def _queue_ch_firecrawl_if_ready_locked(self, cur, row: dict[str, object]):
        domain = str(row.get("domain", "")).strip() or extract_domain(str(row.get("homepage", "")).strip())
        if not str(row.get("ceo", "")).strip() or not is_valid_domain(domain):
            return
        payload = {
            "comp_id": str(row.get("comp_id", "")).strip(),
            "company_name": str(row.get("company_name", "")).strip(),
            "company_number": str(row.get("company_number", "")).strip(),
            "homepage": str(row.get("homepage", "")).strip(),
            "domain": domain,
        }
        self._upsert_task_locked(cur, pipeline="england_companies_house", task_type="ch_firecrawl", entity_id=str(row.get("comp_id", "")).strip(), payload=payload, force_pending=True)
        self._update_ch_task_state_locked(cur, comp_id=str(row.get("comp_id", "")).strip(), task_type="ch_firecrawl", status="pending", retries=0)

    def _update_dnb_task_state_locked(self, cur, *, duns: str, task_type: str, status: str, retries: int, last_error: str = ""):
        mapping = {
            "dnb_detail": ("detail_task_status", "detail_task_retries"),
            "dnb_gmap": ("gmap_task_status", "gmap_task_retries"),
            "dnb_firecrawl": ("firecrawl_task_status", "firecrawl_task_retries"),
        }
        target = mapping.get(task_type)
        if target is None:
            return
        cur.execute(
            f"UPDATE england_dnb_companies SET {target[0]} = %s, {target[1]} = %s, last_error = %s, updated_at = %s WHERE duns = %s",
            (status, retries, self._clip_text(last_error), self._utc_now(), duns),
        )

    def _update_ch_task_state_locked(self, cur, *, comp_id: str, task_type: str, status: str, retries: int, last_error: str = ""):
        mapping = {
            "ch_lookup": ("ch_task_status", "ch_task_retries"),
            "ch_gmap": ("gmap_task_status", "gmap_task_retries"),
            "ch_firecrawl": ("firecrawl_task_status", "firecrawl_task_retries"),
        }
        target = mapping.get(task_type)
        if target is None:
            return
        cur.execute(
            f"UPDATE england_ch_companies SET {target[0]} = %s, {target[1]} = %s, last_error = %s, updated_at = %s WHERE comp_id = %s",
            (status, retries, self._clip_text(last_error), self._utc_now(), comp_id),
        )

    def _fetch_dnb_company_locked(self, cur, duns: str):
        cur.execute("SELECT * FROM england_dnb_companies WHERE duns = %s", (duns,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def _fetch_ch_company_locked(self, cur, comp_id: str):
        cur.execute("SELECT * FROM england_ch_companies WHERE comp_id = %s", (comp_id,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def _merge_dnb_company_row(self, *, current, incoming: dict[str, object], mark_detail_done: bool | None):
        current = current or {}
        row = {
            "duns": self._merge_text(current.get("duns", ""), incoming.get("duns", "")),
            "company_name_en_dnb": self._merge_text(current.get("company_name_en_dnb", ""), incoming.get("company_name_en_dnb", "")),
            "company_name_url": self._merge_text(current.get("company_name_url", ""), incoming.get("company_name_url", "")),
            "key_principal": self._merge_text(current.get("key_principal", ""), incoming.get("key_principal", "")),
            "address": self._merge_text(current.get("address", ""), incoming.get("address", "")),
            "city": self._merge_text(current.get("city", ""), incoming.get("city", "")),
            "region": self._merge_text(current.get("region", ""), incoming.get("region", "")),
            "country": self._merge_text(current.get("country", "United Kingdom"), incoming.get("country", "United Kingdom")),
            "postal_code": self._merge_text(current.get("postal_code", ""), incoming.get("postal_code", "")),
            "sales_revenue": self._merge_text(current.get("sales_revenue", ""), incoming.get("sales_revenue", "")),
            "dnb_website": self._merge_text(current.get("dnb_website", ""), incoming.get("dnb_website", "")),
            "website": self._merge_text(current.get("website", ""), incoming.get("website", "")),
            "domain": self._merge_text(current.get("domain", ""), incoming.get("domain", "")),
            "website_source": self._merge_text(current.get("website_source", ""), incoming.get("website_source", "")),
            "company_name_en_gmap": self._merge_text(current.get("company_name_en_gmap", ""), incoming.get("company_name_en_gmap", "")),
            "company_name_en_site": self._merge_text(current.get("company_name_en_site", ""), incoming.get("company_name_en_site", "")),
            "company_name_resolved": "",
            "site_evidence_url": self._merge_text(current.get("site_evidence_url", ""), incoming.get("site_evidence_url", "")),
            "site_evidence_quote": self._merge_text(current.get("site_evidence_quote", ""), incoming.get("site_evidence_quote", "")),
            "site_confidence": float(incoming.get("site_confidence", current.get("site_confidence", 0.0)) or 0.0),
            "phone": self._merge_text(current.get("phone", ""), incoming.get("phone", "")),
            "emails_json": self._dump_json_list(self._parse_json_list(incoming.get("emails", current.get("emails_json", [])))),
            "detail_done": bool(current.get("detail_done", False)),
            "detail_task_status": str(current.get("detail_task_status", "")).strip(),
            "detail_task_retries": int(current.get("detail_task_retries", 0) or 0),
            "gmap_task_status": str(current.get("gmap_task_status", "")).strip(),
            "gmap_task_retries": int(current.get("gmap_task_retries", 0) or 0),
            "firecrawl_task_status": str(current.get("firecrawl_task_status", "")).strip(),
            "firecrawl_task_retries": int(current.get("firecrawl_task_retries", 0) or 0),
            "last_error": str(current.get("last_error", "")).strip(),
            "updated_at": self._utc_now(),
        }
        if mark_detail_done is True:
            row["detail_done"] = True
        elif mark_detail_done is False:
            row["detail_done"] = False
        row["company_name_resolved"] = resolve_company_name(
            company_name_en_dnb=row["company_name_en_dnb"],
            company_name_local_gmap=row["company_name_en_gmap"],
            company_name_local_site=row["company_name_en_site"],
        )
        return row

    def _upsert_dnb_company_locked(self, cur, row: dict[str, object]):
        payload = dict(row)
        payload["emails_json"] = self._ensure_jsonb_value(payload.get("emails_json", []))
        cur.execute(
            """
            INSERT INTO england_dnb_companies(
                duns, company_name_en_dnb, company_name_url, key_principal, address, city, region, country,
                postal_code, sales_revenue, dnb_website, website, domain, website_source, company_name_en_gmap,
                company_name_en_site, company_name_resolved, site_evidence_url, site_evidence_quote, site_confidence,
                phone, emails_json, detail_done, detail_task_status, detail_task_retries, gmap_task_status,
                gmap_task_retries, firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
            ) VALUES(
                %(duns)s, %(company_name_en_dnb)s, %(company_name_url)s, %(key_principal)s, %(address)s, %(city)s,
                %(region)s, %(country)s, %(postal_code)s, %(sales_revenue)s, %(dnb_website)s, %(website)s,
                %(domain)s, %(website_source)s, %(company_name_en_gmap)s, %(company_name_en_site)s,
                %(company_name_resolved)s, %(site_evidence_url)s, %(site_evidence_quote)s, %(site_confidence)s,
                %(phone)s, %(emails_json)s, %(detail_done)s, %(detail_task_status)s, %(detail_task_retries)s,
                %(gmap_task_status)s, %(gmap_task_retries)s, %(firecrawl_task_status)s, %(firecrawl_task_retries)s,
                %(last_error)s, %(updated_at)s
            )
            ON CONFLICT(duns) DO UPDATE SET
                company_name_en_dnb = EXCLUDED.company_name_en_dnb,
                company_name_url = EXCLUDED.company_name_url,
                key_principal = EXCLUDED.key_principal,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code,
                sales_revenue = EXCLUDED.sales_revenue,
                dnb_website = EXCLUDED.dnb_website,
                website = EXCLUDED.website,
                domain = EXCLUDED.domain,
                website_source = EXCLUDED.website_source,
                company_name_en_gmap = EXCLUDED.company_name_en_gmap,
                company_name_en_site = EXCLUDED.company_name_en_site,
                company_name_resolved = EXCLUDED.company_name_resolved,
                site_evidence_url = EXCLUDED.site_evidence_url,
                site_evidence_quote = EXCLUDED.site_evidence_quote,
                site_confidence = EXCLUDED.site_confidence,
                phone = EXCLUDED.phone,
                emails_json = EXCLUDED.emails_json,
                detail_done = EXCLUDED.detail_done,
                detail_task_status = EXCLUDED.detail_task_status,
                detail_task_retries = EXCLUDED.detail_task_retries,
                gmap_task_status = EXCLUDED.gmap_task_status,
                gmap_task_retries = EXCLUDED.gmap_task_retries,
                firecrawl_task_status = EXCLUDED.firecrawl_task_status,
                firecrawl_task_retries = EXCLUDED.firecrawl_task_retries,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            """,
            payload,
        )

    def _upsert_ch_company_locked(self, cur, row: dict[str, object]):
        payload = dict(row)
        payload["emails_json"] = self._ensure_jsonb_value(payload.get("emails_json", []))
        cur.execute(
            """
            INSERT INTO england_ch_companies(
                comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
            ) VALUES(
                %(comp_id)s, %(company_name)s, %(normalized_name)s, %(company_number)s, %(company_status)s,
                %(ceo)s, %(homepage)s, %(domain)s, %(phone)s, %(emails_json)s, %(ch_task_status)s,
                %(ch_task_retries)s, %(gmap_task_status)s, %(gmap_task_retries)s, %(firecrawl_task_status)s,
                %(firecrawl_task_retries)s, %(last_error)s, %(updated_at)s
            )
            ON CONFLICT(comp_id) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                normalized_name = EXCLUDED.normalized_name,
                company_number = EXCLUDED.company_number,
                company_status = EXCLUDED.company_status,
                ceo = EXCLUDED.ceo,
                homepage = EXCLUDED.homepage,
                domain = EXCLUDED.domain,
                phone = EXCLUDED.phone,
                emails_json = EXCLUDED.emails_json,
                ch_task_status = EXCLUDED.ch_task_status,
                ch_task_retries = EXCLUDED.ch_task_retries,
                gmap_task_status = EXCLUDED.gmap_task_status,
                gmap_task_retries = EXCLUDED.gmap_task_retries,
                firecrawl_task_status = EXCLUDED.firecrawl_task_status,
                firecrawl_task_retries = EXCLUDED.firecrawl_task_retries,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            """,
            payload,
        )
