import csv
import sqlite3
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from oldiron_core.snov import SnovClientConfig
from oldiron_core.snov import SnovCredential
from oldiron_core.snov import SnovProspect
from oldiron_core.snov import SnovService
from oldiron_core.snov import SnovServiceSettings
from oldiron_core.snov.client import _extract_first_domain
from oldiron_core.snov.client import _extract_task_hash
from oldiron_core.snov.service import _build_candidates
from unitedarabemirates_crawler.delivery import build_delivery_bundle
from unitedarabemirates_crawler.sites.common.store import UaeCompanyStore
from unitedarabemirates_crawler.sites.wiza.snov_pipeline import run_pipeline_snov


class _FakeLlm:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def _call_json(self, prompt: str) -> dict[str, object]:
        self.last_prompt = prompt
        return self._payload

    def close(self) -> None:
        return None


class _FakeSnovClient:
    def __init__(self) -> None:
        self.domain_by_name_calls: list[str] = []
        self.prospect_email_calls: list[str] = []

    def close(self) -> None:
        return None

    def get_domain_emails_count(self, domain: str) -> int:
        self.last_count_domain = domain
        return 5

    def company_domain_by_name(self, company_name: str) -> str:
        self.domain_by_name_calls.append(company_name)
        return "acmeholdings.ae"

    def fetch_domain_emails(self, domain: str) -> list[str]:
        return ["info@acmeholdings.ae", "sales@acmeholdings.ae"]

    def fetch_generic_contacts(self, domain: str) -> list[str]:
        return ["support@acmeholdings.ae"]

    def fetch_prospects(self, domain: str) -> list[SnovProspect]:
        return [
            SnovProspect(
                name="Alice Chief",
                title="Chief Executive Officer",
                prospect_hash="ceo",
                email_lookup_path="",
                source_page="https://example.com/about",
            ),
            SnovProspect(
                name="Bob Finance",
                title="Chief Financial Officer",
                prospect_hash="cfo",
                email_lookup_path="",
                source_page="https://example.com/about",
            ),
            SnovProspect(
                name="Cara Accounts",
                title="Accounting Manager",
                prospect_hash="acct",
                email_lookup_path="",
                source_page="https://example.com/about",
            ),
        ]

    def fetch_prospect_emails(self, prospect: SnovProspect) -> list[str]:
        self.prospect_email_calls.append(prospect.prospect_hash)
        return [f"{prospect.prospect_hash}@acmeholdings.ae"]


class _PipelineFakeClient:
    def __init__(self, config) -> None:  # noqa: ANN001
        self.config = config

    def close(self) -> None:
        return None


class _PipelineFakeService:
    def __init__(self, settings, client=None) -> None:  # noqa: ANN001
        self.settings = settings
        self.client = client

    def close(self) -> None:
        return None

    def discover_company(self, *, company_name: str, homepage: str):  # noqa: ANN201
        class _Result:
            website = homepage or "https://resolved.example.com"
            domain_emails = ["info@acmeholdings.ae", "ceo@acmeholdings.ae"]
            representative_names = "Alice Chief;Bob Finance"
            people_json = (
                '[{"name":"Alice Chief","title_zh":"首席执行官","emails":["ceo@acmeholdings.ae"]},'
                '{"name":"Bob Finance","title_zh":"首席财务官","emails":["cfo@acmeholdings.ae"]}]'
            )
            people = [1, 2]

        return _Result()


class WizaSnovTests(unittest.TestCase):
    def test_extract_task_hash_reads_nested_data(self) -> None:
        payload = {"data": {"task_hash": "abc123"}, "meta": {"names": ["Example LLC"]}}
        self.assertEqual(_extract_task_hash(payload), "abc123")

    def test_extract_first_domain_reads_nested_result_domain(self) -> None:
        payload = {
            "status": "completed",
            "data": [
                {
                    "name": "Example LLC",
                    "result": {
                        "domain": "example.ae",
                    },
                }
            ],
        }
        self.assertEqual(_extract_first_domain(payload), "example.ae")

    def test_build_candidates_rejects_ceo_office_false_positive(self) -> None:
        prospects = [
            SnovProspect(
                name="Office Support",
                title="Chief Executive Officer Office Assistance",
                prospect_hash="office",
                email_lookup_path="",
                source_page="https://example.com/about",
            ),
            SnovProspect(
                name="Real Chief",
                title="Chief Executive Officer",
                prospect_hash="real",
                email_lookup_path="",
                source_page="https://example.com/about",
            ),
        ]
        candidates = _build_candidates(prospects)
        self.assertEqual([item.name for item in candidates], ["Real Chief"])

    def test_snov_service_builds_people_json_and_fallback_domain(self) -> None:
        settings = SnovServiceSettings(
            client_config=SnovClientConfig(credentials=(SnovCredential("id", "secret"),)),
            llm_api_key="test-key",
            llm_base_url="https://gpt-agent.cc/v1",
            llm_model="claude-sonnet-4-6",
            llm_reasoning_effort="",
            llm_api_style="chat",
            llm_timeout_seconds=60,
        )
        llm = _FakeLlm(
            {
                "leaders": [{"name": "Alice Chief", "raw_title": "Chief Executive Officer", "title_zh": "首席执行官"}],
                "finance": {"name": "Bob Finance", "raw_title": "Chief Financial Officer", "title_zh": "首席财务官"},
                "accounting": {"name": "Cara Accounts", "raw_title": "Accounting Manager", "title_zh": "会计经理"},
            }
        )
        client = _FakeSnovClient()
        service = SnovService(settings, client=client, llm_client=llm)
        try:
            result = service.discover_company(company_name="Example LLC", homepage="")
        finally:
            service.close()
        self.assertEqual(result.website, "https://acmeholdings.ae")
        self.assertEqual(result.representative_names, "Alice Chief;Bob Finance;Cara Accounts")
        self.assertIn("support@acmeholdings.ae", result.domain_emails)
        self.assertIn("ceo@acmeholdings.ae", result.people_json)
        self.assertEqual(client.domain_by_name_calls, ["Example LLC"])
        self.assertEqual(client.prospect_email_calls, ["ceo", "cfo", "acct"])

    def test_run_pipeline_snov_saves_people_json_to_store(self) -> None:
        settings = SnovServiceSettings(
            client_config=SnovClientConfig(credentials=(SnovCredential("id", "secret"),)),
            llm_api_key="test-key",
            llm_base_url="https://gpt-agent.cc/v1",
            llm_model="claude-sonnet-4-6",
            llm_reasoning_effort="",
            llm_api_style="chat",
            llm_timeout_seconds=60,
        )
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = UaeCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example LLC",
                        "website": "",
                        "source_pdl_id": "pdl_123",
                        "p1_status": "done",
                        "emails": "",
                    }
                ]
            )
            with patch(
                "unitedarabemirates_crawler.sites.wiza.snov_pipeline.SnovServiceSettings.from_env",
                return_value=settings,
            ), patch(
                "unitedarabemirates_crawler.sites.wiza.snov_pipeline.SnovClient",
                _PipelineFakeClient,
            ), patch(
                "unitedarabemirates_crawler.sites.wiza.snov_pipeline.SnovService",
                _PipelineFakeService,
            ):
                stats = run_pipeline_snov(output_dir=output_dir, max_items=1, concurrency=1)

            conn = sqlite3.connect(str(output_dir / "companies.db"))
            try:
                row = conn.execute(
                    "SELECT emails, representative_final, people_json, email_status, website, gmap_status "
                    "FROM companies WHERE record_id = 'examplellc'"
                ).fetchone()
            finally:
                conn.close()
        self.assertEqual(stats, {"processed": 1, "found": 1})
        self.assertIsNotNone(row)
        self.assertEqual(row[3], "done")
        self.assertIn("info@acmeholdings.ae", row[0])
        self.assertEqual(row[1], "Alice Chief;Bob Finance")
        self.assertIn("首席执行官", row[2])
        self.assertEqual(row[4], "https://resolved.example.com")
        self.assertEqual(row[5], "done")

    def test_save_email_result_preserves_existing_representative_when_rerun_is_empty(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = UaeCompanyStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example LLC",
                        "website": "https://example.ae",
                        "representative_final": "Alice Chief",
                    }
                ]
            )
            store.save_email_result(
                "examplellc",
                ["info@example.ae"],
                "Alice Chief",
                "Alice Chief",
                "https://example.ae",
                people_json='[{"name":"Alice Chief","title_zh":"首席执行官","emails":["ceo@example.ae"]}]',
                website="https://example.ae",
                mark_done=True,
            )
            store.save_email_result(
                "examplellc",
                [],
                "",
                "",
                "",
                people_json="",
                website="",
                mark_done=False,
            )
            conn = sqlite3.connect(str(output_dir / "companies.db"))
            try:
                row = conn.execute(
                    "SELECT representative_p3, representative_final, people_json, email_status "
                    "FROM companies WHERE record_id = 'examplellc'"
                ).fetchone()
            finally:
                conn.close()
        self.assertEqual(row[0], "Alice Chief")
        self.assertEqual(row[1], "Alice Chief")
        self.assertIn("首席执行官", row[2])
        self.assertEqual(row[3], "done")

    def test_uae_delivery_wiza_uses_people_json_columns(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            data_root = root / "data"
            delivery_root = root / "delivery"
            site_dir = data_root / "wiza"
            site_dir.mkdir(parents=True, exist_ok=True)
            store = UaeCompanyStore(site_dir / "companies.db")
            store.upsert_companies(
                [
                    {
                        "company_name": "Example LLC",
                        "website": "",
                        "emails": "info@acmeholdings.ae",
                        "people_json": '[{"name":"Alice Chief","title_zh":"首席执行官","emails":["ceo@acmeholdings.ae"]}]',
                        "representative_final": "Alice Chief",
                    }
                ]
            )
            store.save_email_result(
                "examplellc",
                ["info@acmeholdings.ae"],
                "Alice Chief",
                "Alice Chief",
                "https://acmeholdings.ae",
                people_json='[{"name":"Alice Chief","title_zh":"首席执行官","emails":["ceo@acmeholdings.ae"]}]',
                website="https://acmeholdings.ae",
                mark_done=True,
            )
            conn = sqlite3.connect(str(site_dir / "companies.db"))
            try:
                statuses = conn.execute(
                    "SELECT gmap_status, email_status, website FROM companies WHERE record_id = 'examplellc'"
                ).fetchone()
            finally:
                conn.close()
            summary = build_delivery_bundle(data_root, delivery_root, "day1")
            csv_path = delivery_root / "UnitedArabEmirates_day001" / "wiza.csv"
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.reader(fp))
        self.assertEqual(summary["delta_companies"], 1)
        self.assertEqual(statuses[0], "done")
        self.assertEqual(statuses[1], "done")
        self.assertEqual(statuses[2], "https://acmeholdings.ae")
        self.assertEqual(rows[0], ["company_name", "website", "people_json", "emails", "phone"])
        self.assertEqual(rows[1][0], "Example LLC")
        self.assertIn("首席执行官", rows[1][2])


if __name__ == "__main__":
    unittest.main()
