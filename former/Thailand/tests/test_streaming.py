from __future__ import annotations

from thailand_crawler.models import Segment
from thailand_crawler.models import CompanyRecord
from thailand_crawler.streaming.industry_catalog import build_country_industry_segments
from thailand_crawler.streaming.config import StreamPipelineConfig
from thailand_crawler.streaming.firecrawl_client import FirecrawlClientConfig
from thailand_crawler.streaming.firecrawl_client import audit_firecrawl_keys
from thailand_crawler.streaming.llm_client import contains_thai_text
from thailand_crawler.streaming.pipeline import _discover_stable_segments
from thailand_crawler.streaming.pipeline import _page_signature
from thailand_crawler.streaming.pipeline import SiteServiceHealthGate
from thailand_crawler.streaming.llm_client import resolve_company_name
from thailand_crawler.streaming.store import StreamStore



def test_resolve_company_name_prefers_thai_and_falls_back_to_english() -> None:
    assert contains_thai_text("บริษัท เอซีเอ็มอี จำกัด") is True
    assert contains_thai_text("ACME Co., Ltd.") is False
    assert resolve_company_name("ACME Co., Ltd.", "บริษัท เอซีเอ็มอี จำกัด") == "บริษัท เอซีเอ็มอี จำกัด"
    assert resolve_company_name("ACME Co., Ltd.", "") == "ACME Co., Ltd."
    assert resolve_company_name("ACME Co., Ltd.", "ACME Thailand") == "ACME Co., Ltd."


def test_build_country_industry_segments_covers_all_top_levels_and_subcategories() -> None:
    segments = build_country_industry_segments('th')
    segment_ids = {segment.segment_id for segment in segments}

    assert len(segments) == 327
    assert 'construction|th||' in segment_ids
    assert 'general_medical_and_surgical_hospitals|th||' in segment_ids
    assert 'mining_quarrying_and_oil_and_gas_extraction|th||' in segment_ids
    assert 'management_of_companies_and_enterprises|th||' in segment_ids


def test_page_signature_detects_page_loop_on_same_rows() -> None:
    rows = [
        CompanyRecord(duns='A1', company_name='Alpha'),
        CompanyRecord(duns='B2', company_name='Beta'),
    ]
    same_rows = [
        CompanyRecord(duns='A1', company_name='Alpha'),
        CompanyRecord(duns='B2', company_name='Beta'),
    ]
    other_rows = [
        CompanyRecord(duns='C3', company_name='Gamma'),
        CompanyRecord(duns='D4', company_name='Delta'),
    ]

    assert _page_signature(rows) == _page_signature(same_rows)
    assert _page_signature(rows) != _page_signature(other_rows)



def test_stream_store_only_outputs_final_when_name_principal_email_complete(tmp_path) -> None:
    store = StreamStore(tmp_path / "store.db")
    store.upsert_company(
        duns="1",
        company_name_en_dnb="ACME Co., Ltd.",
        company_name_url="acme",
        key_principal="Alice",
        phone="021234567",
        address="Bangkok",
        city="Bangkok",
        region="Bangkok",
        dnb_website="https://acme.example.com",
    )

    assert store.get_final_company("1") is None

    store.save_site_result(duns="1", company_name_th="")
    assert store.get_final_company("1") is None

    store.save_snov_result(duns="1", emails=["sales@acme.example.com"])
    fallback_row = store.get_final_company("1")
    assert fallback_row is not None
    assert fallback_row["company_name"] == "ACME Co., Ltd."
    assert fallback_row["company_manager"] == "Alice"
    assert fallback_row["contact_emails"] == ["sales@acme.example.com"]
    assert fallback_row["domain"] == "acme.example.com"
    assert fallback_row["phone"] == "021234567"

    store.save_site_result(duns="1", company_name_th="บริษัท เอซีเอ็มอี จำกัด")
    thai_row = store.get_final_company("1")
    assert thai_row is not None
    assert thai_row["company_name"] == "บริษัท เอซีเอ็มอี จำกัด"


class _FakeRequestSession:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.headers = {}

    def request(self, method: str, url: str, headers: dict | None = None, json: dict | None = None, timeout: float = 0) -> object:
        if not self.outcomes:
            raise AssertionError('缺少预设返回')
        current = self.outcomes.pop(0)
        if isinstance(current, Exception):
            raise current
        return current


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = '') -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = {}

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, plan: dict[str, list[_FakeResponse]]) -> None:
        self.plan = {key: list(value) for key, value in plan.items()}
        self.headers = {}

    def get(self, url: str, headers: dict | None = None, timeout: float = 0) -> _FakeResponse:
        key = str((headers or {}).get('Authorization', '')).replace('Bearer ', '')
        return self.plan[key].pop(0)

    def post(self, url: str, headers: dict | None = None, json: dict | None = None, timeout: float = 0) -> _FakeResponse:
        key = str((headers or {}).get('Authorization', '')).replace('Bearer ', '')
        return self.plan[key].pop(0)


def test_audit_firecrawl_keys_prunes_bad_keys(monkeypatch, tmp_path) -> None:
    key_file = tmp_path / 'firecrawl_keys.txt'
    key_file.write_text('good\nzero\nunauth\nrate402\nratekeep\n', encoding='utf-8')

    def fake_session(*args, **kwargs):
        return _FakeSession(
            {
                'good': [_FakeResponse(200, {'data': {'remainingCredits': 3}})],
                'zero': [_FakeResponse(200, {'data': {'remainingCredits': 0}})],
                'unauth': [_FakeResponse(401, text='unauthorized')],
                'rate402': [_FakeResponse(429, text='rate'), _FakeResponse(402, text='insufficient')],
                'ratekeep': [_FakeResponse(429, text='rate'), _FakeResponse(429, text='rate')],
            }
        )

    monkeypatch.setattr('thailand_crawler.streaming.firecrawl_client.cffi_requests.Session', fake_session)

    summary = audit_firecrawl_keys(
        key_file=key_file,
        config=FirecrawlClientConfig(base_url='https://api.firecrawl.dev/v2/', timeout_seconds=10.0),
    )

    kept = [line.strip() for line in key_file.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert kept == ['good', 'ratekeep']
    assert summary.total == 5
    assert summary.usable == 1
    assert summary.removed_no_credit == 2
    assert summary.removed_unauthorized == 1
    assert summary.kept_rate_limited == 1


def test_stream_config_reads_inline_firecrawl_keys(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv('FIRECRAWL_KEYS', 'k1,k2\nk3')
    monkeypatch.setenv('LLM_API_KEY', 'llm')
    monkeypatch.setenv('LLM_MODEL', 'model')
    monkeypatch.setenv('SNOV_CLIENT_ID', 'sid')
    monkeypatch.setenv('SNOV_CLIENT_SECRET', 'ssecret')

    config = StreamPipelineConfig.from_env(
        project_root=tmp_path,
        output_dir=tmp_path / 'output' / 'dnb_stream',
        max_companies=0,
        dnb_workers=4,
        website_workers=4,
        site_workers=2,
        snov_workers=4,
    )

    assert config.firecrawl_keys_inline == ['k1', 'k2', 'k3']


def test_site_name_service_writes_inline_keys(tmp_path) -> None:
    from thailand_crawler.streaming.site_name_service import SiteNameService

    target = tmp_path / 'firecrawl_keys.txt'
    SiteNameService.ensure_keys_file(target, ['k1', 'k2', 'k1'])

    assert target.read_text(encoding='utf-8').splitlines() == ['k1', 'k2']


class _FakeDiscoveryClient:
    def __init__(self) -> None:
        self.payloads = {
            ('construction', 'th', '', ''): {
                'candidatesMatchedQuantityInt': 120,
                'companyInformationGeos': [
                    {'href': 'th.bangkok', 'quantity': '60'},
                    {'href': 'th.chiang_mai', 'quantity': '40'},
                ],
                'relatedIndustries': {},
            },
            ('construction', 'th', 'bangkok', ''): {
                'candidatesMatchedQuantityInt': 60,
                'companyInformationGeos': [],
                'relatedIndustries': {},
            },
            ('construction', 'th', 'chiang_mai', ''): {
                'candidatesMatchedQuantityInt': 40,
                'companyInformationGeos': [],
                'relatedIndustries': {},
            },
        }

    def fetch_company_listing_page(self, segment: Segment, page_number: int = 1) -> dict:
        assert page_number == 1
        key = (segment.industry_path, segment.country_iso_two_code, segment.region_name, segment.city_name)
        return self.payloads[key]


def test_discovery_resume_keeps_progress_in_sqlite(tmp_path) -> None:
    store = StreamStore(tmp_path / 'store.db')
    root = Segment(industry_path='construction', country_iso_two_code='th', expected_count=117112, segment_type='country')
    client = _FakeDiscoveryClient()
    stop_event = __import__('threading').Event()

    store.ensure_discovery_seed(root)
    processed_first = _discover_stable_segments(store=store, client=client, stop_event=stop_event, max_leaf_records=50, max_nodes=1)

    assert processed_first == 1
    assert store.discovery_done() is False
    assert store.segment_count() == 1

    processed_second = _discover_stable_segments(store=store, client=client, stop_event=stop_event, max_leaf_records=50)

    assert processed_second == 2
    assert store.discovery_done() is True
    assert store.segment_count() == 3


def test_requeue_stale_running_tasks_handles_discovery_queue(tmp_path) -> None:
    store = StreamStore(tmp_path / 'store.db')
    root = Segment(industry_path='construction', country_iso_two_code='th', expected_count=117112, segment_type='country')
    store.ensure_discovery_seed(root)
    claimed = store.claim_discovery_node()

    assert claimed is not None

    with store._lock:
        store._conn.execute(
            "UPDATE dnb_discovery_queue SET updated_at = '2000-01-01T00:00:00Z' WHERE segment_id = ?",
            (root.segment_id,),
        )
        store._conn.commit()

    recovered = store.requeue_stale_running_tasks(older_than_seconds=1)

    assert recovered == 1
    claimed_again = store.claim_discovery_node()
    assert claimed_again is not None
    assert claimed_again.segment_id == root.segment_id


def test_mark_website_done_prefers_gmap_name_and_fills_phone_when_missing(tmp_path) -> None:
    store = StreamStore(tmp_path / 'store.db')
    store.upsert_company(
        duns='2',
        company_name_en_dnb='SEAFCO PUBLIC COMPANY LIMITED',
        company_name_url='seafco',
        key_principal='Boss',
        phone='',
        address='Bangkok',
        city='Bangkok',
        region='Bangkok',
        dnb_website='https://www.seafco.co.th',
        detail_done=True,
    )

    store.mark_website_done(
        duns='2',
        website='https://www.seafco.co.th',
        source='gmap',
        company_name_th='บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่',
        phone='+66 2 919 0090',
    )

    row = store.get_company('2')
    assert row is not None
    assert row['company_name_resolved'] == 'บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่'
    assert row['phone'] == '+66 2 919 0090'
    assert row['website_source'] == 'gmap'

    with store._lock:
        site_pending = store._conn.execute("select count(*) from site_queue where duns = '2'").fetchone()[0]
        snov_pending = store._conn.execute("select count(*) from snov_queue where duns = '2'").fetchone()[0]
    assert site_pending == 0
    assert snov_pending == 1


def test_mark_website_done_uses_dnb_website_for_site_fallback(tmp_path) -> None:
    store = StreamStore(tmp_path / 'store.db')
    store.upsert_company(
        duns='3',
        company_name_en_dnb='ACME ENGINEERING COMPANY LIMITED',
        company_name_url='acme',
        key_principal='Boss',
        phone='',
        address='Bangkok',
        city='Bangkok',
        region='Bangkok',
        dnb_website='https://www.acme.example.com',
        detail_done=True,
    )

    store.mark_website_done(duns='3', website='', source='', company_name_th='', phone='02 123 4567')

    row = store.get_company('3')
    assert row is not None
    assert row['website'] == 'https://www.acme.example.com'
    assert row['website_source'] == 'dnb'
    assert row['phone'] == '02 123 4567'

    with store._lock:
        site_pending = store._conn.execute("select count(*) from site_queue where duns = '3'").fetchone()[0]
        snov_pending = store._conn.execute("select count(*) from snov_queue where duns = '3'").fetchone()[0]
    assert site_pending == 1
    assert snov_pending == 1


def test_firecrawl_5xx_does_not_cooldown_key(monkeypatch, tmp_path) -> None:
    from thailand_crawler.streaming.firecrawl_client import FirecrawlClient
    from thailand_crawler.streaming.firecrawl_client import FirecrawlError
    from thailand_crawler.streaming.key_pool import FirecrawlKeyPool

    key_file = tmp_path / 'firecrawl_keys.txt'
    key_file.write_text('k1\n', encoding='utf-8')
    pool = FirecrawlKeyPool(keys=['k1'], key_file=key_file, db_path=tmp_path / 'keys.db')
    client = FirecrawlClient(
        key_pool=pool,
        config=FirecrawlClientConfig(base_url='https://api.firecrawl.dev/v2/', timeout_seconds=5.0, max_retries=0),
    )
    client._session = _FakeRequestSession([_FakeResponse(500) for _ in range(5)])

    for _ in range(5):
        try:
            client.scrape_page('https://example.com')
        except FirecrawlError as exc:
            assert exc.code == 'firecrawl_5xx'
        else:
            raise AssertionError('预期抛出 firecrawl_5xx')

    with pool._connect() as conn:
        row = conn.execute("select state, failure_count, cooldown_until from keys where key='k1'").fetchone()
    assert row['state'] == 'active'
    assert row['cooldown_until'] is None


def test_firecrawl_request_error_does_not_cooldown_key(monkeypatch, tmp_path) -> None:
    from thailand_crawler.streaming.firecrawl_client import FirecrawlClient
    from thailand_crawler.streaming.firecrawl_client import FirecrawlError
    from thailand_crawler.streaming.key_pool import FirecrawlKeyPool

    key_file = tmp_path / 'firecrawl_keys.txt'
    key_file.write_text('k1\n', encoding='utf-8')
    pool = FirecrawlKeyPool(keys=['k1'], key_file=key_file, db_path=tmp_path / 'keys.db')
    client = FirecrawlClient(
        key_pool=pool,
        config=FirecrawlClientConfig(base_url='https://api.firecrawl.dev/v2/', timeout_seconds=5.0, max_retries=0),
    )
    client._session = _FakeRequestSession([RuntimeError('boom') for _ in range(5)])

    for _ in range(5):
        try:
            client.scrape_page('https://example.com')
        except FirecrawlError as exc:
            assert exc.code == 'firecrawl_request_failed'
        else:
            raise AssertionError('预期抛出 firecrawl_request_failed')

    with pool._connect() as conn:
        row = conn.execute("select state, failure_count, cooldown_until from keys where key='k1'").fetchone()
    assert row['state'] == 'active'
    assert row['cooldown_until'] is None


def test_site_service_health_gate_pauses_after_consecutive_upstream_failures() -> None:
    gate = SiteServiceHealthGate(failure_threshold=3, base_backoff_seconds=15.0, cap_backoff_seconds=60.0)

    assert gate.record_failure(now=100.0) == 0.0
    assert gate.record_failure(now=101.0) == 0.0
    delay = gate.record_failure(now=102.0)

    assert delay == 15.0
    assert gate.wait_seconds(now=110.0) == 7.0

    next_delay = gate.record_failure(now=111.0)
    assert next_delay == 30.0
    assert gate.wait_seconds(now=120.0) == 21.0


def test_site_service_health_gate_resets_after_success() -> None:
    gate = SiteServiceHealthGate(failure_threshold=2, base_backoff_seconds=10.0, cap_backoff_seconds=60.0)

    gate.record_failure(now=10.0)
    gate.record_failure(now=11.0)
    assert gate.wait_seconds(now=12.0) == 9.0

    gate.record_success()

    assert gate.wait_seconds(now=12.0) == 0.0
    assert gate.record_failure(now=20.0) == 0.0


def test_site_service_health_gate_allows_only_one_probe_when_unhealthy() -> None:
    gate = SiteServiceHealthGate(failure_threshold=2, base_backoff_seconds=10.0, cap_backoff_seconds=60.0)

    gate.record_failure(now=10.0)
    gate.record_failure(now=11.0)

    assert gate.acquire_attempt(now=15.0) is False
    assert gate.acquire_attempt(now=21.0) is True
    assert gate.acquire_attempt(now=21.0) is False

    gate.cancel_attempt()

    assert gate.acquire_attempt(now=21.0) is True


def test_site_service_health_gate_success_clears_probe_lock() -> None:
    gate = SiteServiceHealthGate(failure_threshold=2, base_backoff_seconds=10.0, cap_backoff_seconds=60.0)

    gate.record_failure(now=10.0)
    gate.record_failure(now=11.0)
    assert gate.acquire_attempt(now=21.0) is True

    gate.record_success()

    assert gate.wait_seconds(now=21.0) == 0.0
    assert gate.acquire_attempt(now=21.0) is True


def test_mark_website_done_skips_excluded_domain_for_downstream(tmp_path) -> None:
    store = StreamStore(tmp_path / 'store.db')
    store.upsert_company(
        duns='skip-domain',
        company_name_en_dnb='THUMBS UP COMPANY LIMITED',
        company_name_url='thumbs-up',
        key_principal='Pimchanok',
        phone='',
        address='Khon Kaen',
        city='Khon Kaen',
        region='Khon Kaen',
        dnb_website='',
        detail_done=True,
    )

    store.mark_website_done(
        duns='skip-domain',
        website='https://www.kkmuni.go.th',
        source='gmap',
        company_name_th='',
        phone='',
    )

    row = store.get_company('skip-domain')
    assert row is not None
    assert row['website'] == ''
    assert row['domain'] == ''
    assert row['website_source'] == ''

    with store._lock:
        site_pending = store._conn.execute("select count(*) from site_queue where duns = 'skip-domain'").fetchone()[0]
        snov_pending = store._conn.execute("select count(*) from snov_queue where duns = 'skip-domain'").fetchone()[0]
    assert site_pending == 0
    assert snov_pending == 0
