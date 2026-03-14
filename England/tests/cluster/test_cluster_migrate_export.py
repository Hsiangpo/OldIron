import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


class ClusterPostgresCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.port = _free_port()
        cls.container_name = f"oldiron-test-{os.getpid()}-{cls.port}"
        run = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                cls.container_name,
                "-e",
                "POSTGRES_PASSWORD=postgres",
                "-e",
                "POSTGRES_DB=oldiron_test",
                "-p",
                f"{cls.port}:5432",
                "postgres:16-alpine",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if run.returncode != 0:
            raise unittest.SkipTest(f"docker postgres 启动失败：{run.stderr.strip()}")
        cls.dsn = f"postgresql://postgres:postgres@127.0.0.1:{cls.port}/oldiron_test"
        from england_crawler.cluster.db import ClusterDb

        db = ClusterDb(cls.dsn)
        for _ in range(30):
            try:
                db.test_connection()
                return
            except Exception:
                time.sleep(1.0)
        raise unittest.SkipTest("测试 Postgres 未就绪。")

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.run(["docker", "rm", "-f", cls.container_name], check=False, capture_output=True)
        super().tearDownClass()


class ClusterMigrationExportTests(ClusterPostgresCase):
    def _prepare_sample_output(self, tmp: Path) -> None:
        output = tmp / "output"
        (output / "dnb").mkdir(parents=True, exist_ok=True)
        (output / "companies_house").mkdir(parents=True, exist_ok=True)
        (output / "cache").mkdir(parents=True, exist_ok=True)
        (output / "delivery" / "England_day001").mkdir(parents=True, exist_ok=True)

        self._build_dnb_sqlite(output / "dnb" / "store.db")
        self._build_ch_sqlite(output / "companies_house" / "store.db")
        self._build_firecrawl_cache(output / "firecrawl_cache.db")
        self._build_firecrawl_keys(output / "cache" / "firecrawl_keys.db")
        (output / "delivery" / "England_day001" / "summary.json").write_text(
            json.dumps(
                {
                    "day": 1,
                    "baseline_day": 0,
                    "total_current_companies": 2,
                    "delta_companies": 2,
                    "generated_at": "2026-03-15T00:00:00Z",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (output / "delivery" / "England_day001" / "keys.txt").write_text("name|alpha\nname|beta\n", encoding="utf-8")
        (output / "delivery" / "England_day001" / "companies.csv").write_text(
            "company_name,ceo,homepage,domain,phone,emails\n"
            "Alpha Ltd,Alice,https://alpha.test,alpha.test,123,alice@alpha.test\n",
            encoding="utf-8",
        )

    def _build_dnb_sqlite(self, path: Path) -> None:
        import sqlite3

        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE companies (
                duns TEXT PRIMARY KEY,
                company_name_en_dnb TEXT, company_name_url TEXT, key_principal TEXT,
                address TEXT, city TEXT, region TEXT, country TEXT, postal_code TEXT,
                sales_revenue TEXT, dnb_website TEXT, website TEXT, domain TEXT,
                website_source TEXT, company_name_en_gmap TEXT, company_name_en_site TEXT,
                company_name_resolved TEXT, site_evidence_url TEXT, site_evidence_quote TEXT,
                site_confidence REAL, phone TEXT, emails_json TEXT, detail_done INTEGER,
                gmap_status TEXT, site_name_status TEXT, snov_status TEXT, last_error TEXT, updated_at TEXT
            );
            CREATE TABLE detail_queue (duns TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE dnb_discovery_queue (segment_id TEXT PRIMARY KEY, industry_path TEXT, country_iso_two_code TEXT, region_name TEXT, city_name TEXT, expected_count INTEGER, status TEXT, updated_at TEXT);
            CREATE TABLE dnb_segments (segment_id TEXT PRIMARY KEY, industry_path TEXT, country_iso_two_code TEXT, region_name TEXT, city_name TEXT, expected_count INTEGER, next_page INTEGER, status TEXT, updated_at TEXT);
            CREATE TABLE gmap_queue (duns TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE site_queue (duns TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE snov_queue (duns TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            """
        )
        conn.execute(
            """
            INSERT INTO companies VALUES(
                'd1','Alpha Ltd','alpha-ltd','Alice','Addr','London','London','United Kingdom','E1','100',
                'https://alpha.test','','','','','','','','',0,'','[]',1,'done','','pending','','2026-03-15T00:00:00Z'
            )
            """
        )
        conn.execute("INSERT INTO gmap_queue VALUES('d1','done',0,'2026-03-15T00:00:00Z','','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO detail_queue VALUES('d1','done',0,'2026-03-15T00:00:00Z','','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO snov_queue VALUES('d1','failed',2,'2026-03-15T00:00:00Z','boom','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO dnb_discovery_queue VALUES('seg1','construction','gb','','',10,'pending','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO dnb_segments VALUES('seg1','construction','gb','','',10,3,'pending','2026-03-15T00:00:00Z')")
        conn.commit()
        conn.close()

    def _build_ch_sqlite(self, path: Path) -> None:
        import sqlite3

        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE companies (
                comp_id TEXT PRIMARY KEY, company_name TEXT, normalized_name TEXT, company_number TEXT,
                company_status TEXT, ceo TEXT, homepage TEXT, domain TEXT, phone TEXT, emails_json TEXT,
                ch_status TEXT, gmap_status TEXT, snov_status TEXT, last_error TEXT, updated_at TEXT
            );
            CREATE TABLE ch_queue (comp_id TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE gmap_queue (comp_id TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE snov_queue (comp_id TEXT PRIMARY KEY, status TEXT, retries INTEGER, next_run_at TEXT, last_error TEXT, updated_at TEXT);
            CREATE TABLE source_files (source_path TEXT PRIMARY KEY, fingerprint TEXT, total_rows INTEGER, updated_at TEXT);
            """
        )
        conn.execute(
            """
            INSERT INTO companies VALUES(
                'c1','Beta Ltd','BETA LTD','','','Bob','https://beta.test','beta.test','321','[]',
                'done','done','pending','','2026-03-15T00:00:00Z'
            )
            """
        )
        conn.execute("INSERT INTO ch_queue VALUES('c1','done',0,'2026-03-15T00:00:00Z','','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO gmap_queue VALUES('c1','done',0,'2026-03-15T00:00:00Z','','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO snov_queue VALUES('c1','pending',1,'2026-03-15T00:00:00Z','','2026-03-15T00:00:00Z')")
        conn.execute("INSERT INTO source_files VALUES('docs/英国.xlsx','abc',1,'2026-03-15T00:00:00Z')")
        conn.commit()
        conn.close()

    def _build_firecrawl_cache(self, path: Path) -> None:
        import sqlite3

        conn = sqlite3.connect(path)
        conn.execute(
            """
            CREATE TABLE firecrawl_domain_cache(
                domain TEXT PRIMARY KEY, status TEXT, emails_json TEXT, next_retry_at TEXT, updated_at TEXT, last_error TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO firecrawl_domain_cache VALUES('cached.test','done','[\"x@cached.test\"]','','2026-03-15T00:00:00Z','')"
        )
        conn.commit()
        conn.close()

    def _build_firecrawl_keys(self, path: Path) -> None:
        import sqlite3

        conn = sqlite3.connect(path)
        conn.execute(
            """
            CREATE TABLE keys(
                key TEXT PRIMARY KEY, idx INTEGER, state TEXT, cooldown_until REAL,
                failure_count INTEGER, in_flight INTEGER, disabled_reason TEXT, last_used REAL
            )
            """
        )
        conn.execute("INSERT INTO keys VALUES('fc-test-key',0,'active',NULL,0,0,'',NULL)")
        conn.commit()
        conn.close()

    def test_migrate_and_export_keep_product_compatible(self) -> None:
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.export import export_cluster_snapshots
        from england_crawler.cluster.migrate import migrate_england_history
        from england_crawler.delivery import build_delivery_bundle

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._prepare_sample_output(root)
            db = ClusterDb(self.dsn)
            migrate_england_history(db, root / "output")
            export_cluster_snapshots(db, root / "output", include_delivery=True)
            summary = build_delivery_bundle(root / "output", root / "output" / "delivery", "day1")
            self.assertEqual(summary["day"], 1)
            dnb_text = (root / "output" / "dnb" / "final_companies.jsonl").read_text(encoding="utf-8")
            ch_text = (root / "output" / "companies_house" / "final_companies.jsonl").read_text(encoding="utf-8")
            self.assertIn("Alpha Ltd", dnb_text)
            self.assertIn("Beta Ltd", ch_text)

    def test_repository_claim_and_complete(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.repository import ClusterRepository
        from england_crawler.cluster.schema import initialize_schema

        config = ClusterConfig.from_env(ROOT)
        config.postgres_dsn = self.dsn
        db = ClusterDb(self.dsn)
        initialize_schema(db)
        repo = ClusterRepository(db, config)
        with db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO england_ch_companies(
                        comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                        phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                        firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES('cx','Gamma Ltd','GAMMA LTD','','','','https://gamma.test','gamma.test','','[]'::jsonb,'pending',0,'done',0,'',0,'',NOW())
                    ON CONFLICT(comp_id) DO NOTHING
                    """
                )
                cur.execute(
                    """
                    INSERT INTO england_cluster_tasks(task_id, pipeline, task_type, entity_id, status, retries, next_run_at, payload_json, created_at, updated_at)
                    VALUES('t1','england_companies_house','ch_firecrawl','cx','pending',0,NOW(),%s,NOW(),NOW())
                    ON CONFLICT(task_id) DO NOTHING
                    """,
                    (Jsonb({"comp_id": "cx", "company_name": "Gamma Ltd", "homepage": "https://gamma.test", "domain": "gamma.test"}),),
                )
        task = repo.claim_task("worker-1", ["ch_firecrawl"])
        self.assertIsNotNone(task)
        repo.complete_task(task_id=task.task_id, worker_id="worker-1", result={"emails": ["hello@gamma.test"]})
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT firecrawl_task_status, emails_json FROM england_ch_companies WHERE comp_id = 'cx'")
                row = cur.fetchone()
        self.assertEqual(row["firecrawl_task_status"], "done")
        self.assertIn("hello@gamma.test", row["emails_json"])
