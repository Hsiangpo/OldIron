"""England 集群 Postgres schema。"""

from __future__ import annotations

from england_crawler.cluster.db import ClusterDb


def _cluster_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS cluster_workers (
        worker_id TEXT PRIMARY KEY,
        host_name TEXT NOT NULL,
        platform TEXT NOT NULL,
        capabilities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        git_commit TEXT NOT NULL DEFAULT '',
        python_version TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'online',
        last_heartbeat_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_cluster_tasks (
        task_id TEXT PRIMARY KEY,
        pipeline TEXT NOT NULL,
        task_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        status TEXT NOT NULL,
        retries INTEGER NOT NULL DEFAULT 0,
        next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_expires_at TIMESTAMPTZ,
        last_error TEXT NOT NULL DEFAULT '',
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (pipeline, task_type, entity_id)
    );

    CREATE INDEX IF NOT EXISTS idx_england_cluster_tasks_claim
    ON england_cluster_tasks(status, next_run_at, updated_at, task_type, entity_id);

    CREATE TABLE IF NOT EXISTS england_cluster_task_attempts (
        attempt_id BIGSERIAL PRIMARY KEY,
        task_id TEXT NOT NULL REFERENCES england_cluster_tasks(task_id) ON DELETE CASCADE,
        worker_id TEXT NOT NULL DEFAULT '',
        result_status TEXT NOT NULL,
        error_text TEXT NOT NULL DEFAULT '',
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """


def _dnb_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS england_dnb_discovery_nodes (
        segment_id TEXT PRIMARY KEY,
        industry_path TEXT NOT NULL,
        country_iso_two_code TEXT NOT NULL,
        region_name TEXT NOT NULL DEFAULT '',
        city_name TEXT NOT NULL DEFAULT '',
        expected_count INTEGER NOT NULL DEFAULT 0,
        task_status TEXT NOT NULL DEFAULT '',
        task_retries INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_dnb_segments (
        segment_id TEXT PRIMARY KEY,
        industry_path TEXT NOT NULL,
        country_iso_two_code TEXT NOT NULL,
        region_name TEXT NOT NULL DEFAULT '',
        city_name TEXT NOT NULL DEFAULT '',
        expected_count INTEGER NOT NULL DEFAULT 0,
        next_page INTEGER NOT NULL DEFAULT 1,
        task_status TEXT NOT NULL DEFAULT '',
        task_retries INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_dnb_companies (
        duns TEXT PRIMARY KEY,
        company_name_en_dnb TEXT NOT NULL DEFAULT '',
        company_name_url TEXT NOT NULL DEFAULT '',
        key_principal TEXT NOT NULL DEFAULT '',
        address TEXT NOT NULL DEFAULT '',
        city TEXT NOT NULL DEFAULT '',
        region TEXT NOT NULL DEFAULT '',
        country TEXT NOT NULL DEFAULT 'United Kingdom',
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
        site_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
        phone TEXT NOT NULL DEFAULT '',
        emails_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        detail_done BOOLEAN NOT NULL DEFAULT FALSE,
        detail_task_status TEXT NOT NULL DEFAULT '',
        detail_task_retries INTEGER NOT NULL DEFAULT 0,
        gmap_task_status TEXT NOT NULL DEFAULT '',
        gmap_task_retries INTEGER NOT NULL DEFAULT 0,
        firecrawl_task_status TEXT NOT NULL DEFAULT '',
        firecrawl_task_retries INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_england_dnb_companies_domain
    ON england_dnb_companies(domain);
    """


def _companies_house_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS england_ch_source_files (
        source_path TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        total_rows INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_ch_companies (
        comp_id TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        normalized_name TEXT NOT NULL,
        company_number TEXT NOT NULL DEFAULT '',
        company_status TEXT NOT NULL DEFAULT '',
        ceo TEXT NOT NULL DEFAULT '',
        homepage TEXT NOT NULL DEFAULT '',
        domain TEXT NOT NULL DEFAULT '',
        phone TEXT NOT NULL DEFAULT '',
        emails_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        ch_task_status TEXT NOT NULL DEFAULT '',
        ch_task_retries INTEGER NOT NULL DEFAULT 0,
        gmap_task_status TEXT NOT NULL DEFAULT '',
        gmap_task_retries INTEGER NOT NULL DEFAULT 0,
        firecrawl_task_status TEXT NOT NULL DEFAULT '',
        firecrawl_task_retries INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_england_ch_companies_normalized_name
    ON england_ch_companies(normalized_name);
    CREATE INDEX IF NOT EXISTS idx_england_ch_companies_domain
    ON england_ch_companies(domain);
    """


def _firecrawl_and_delivery_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS england_firecrawl_domain_cache (
        domain TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        emails_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        next_retry_at TIMESTAMPTZ,
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_expires_at TIMESTAMPTZ,
        last_error TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_firecrawl_keys (
        key_hash TEXT PRIMARY KEY,
        key_value TEXT NOT NULL,
        state TEXT NOT NULL,
        failure_count INTEGER NOT NULL DEFAULT 0,
        in_flight INTEGER NOT NULL DEFAULT 0,
        cooldown_until TIMESTAMPTZ,
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_expires_at TIMESTAMPTZ,
        disabled_reason TEXT NOT NULL DEFAULT '',
        last_used_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_delivery_runs (
        run_id BIGSERIAL PRIMARY KEY,
        day_number INTEGER NOT NULL UNIQUE,
        baseline_day INTEGER NOT NULL DEFAULT 0,
        total_current_companies INTEGER NOT NULL DEFAULT 0,
        delta_companies INTEGER NOT NULL DEFAULT 0,
        generated_at TIMESTAMPTZ NOT NULL,
        keys_text TEXT NOT NULL DEFAULT '',
        summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS england_delivery_items (
        item_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT NOT NULL REFERENCES england_delivery_runs(run_id) ON DELETE CASCADE,
        company_name TEXT NOT NULL DEFAULT '',
        ceo TEXT NOT NULL DEFAULT '',
        homepage TEXT NOT NULL DEFAULT '',
        domain TEXT NOT NULL DEFAULT '',
        phone TEXT NOT NULL DEFAULT '',
        emails_text TEXT NOT NULL DEFAULT '',
        row_index INTEGER NOT NULL DEFAULT 0
    );
    """


def initialize_schema(db: ClusterDb) -> None:
    sql_statements = [
        _cluster_sql(),
        _dnb_sql(),
        _companies_house_sql(),
        _firecrawl_and_delivery_sql(),
    ]
    with db.transaction() as conn:
        with conn.cursor() as cur:
            for statement in sql_statements:
                cur.execute(statement)
