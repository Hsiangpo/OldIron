import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook
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
    def _build_companies_house_xlsx(self, path: Path, names: list[str]) -> None:
        workbook = Workbook()
        sheet = workbook.active
        sheet["A1"] = "Company Name"
        for index, name in enumerate(names, start=2):
            sheet.cell(row=index, column=1, value=name)
        path.parent.mkdir(parents=True, exist_ok=True)
        workbook.save(path)

    def _run_cluster_command(self, argv: list[str]) -> tuple[int, str]:
        from england_crawler.cluster.cli import run_cluster

        buffer = io.StringIO()
        with patch.dict(os.environ, {"ENGLAND_CLUSTER_POSTGRES_DSN": self.dsn}, clear=False):
            with redirect_stdout(buffer):
                code = run_cluster(argv)
        return code, buffer.getvalue()

    def _reset_cluster_tables(self) -> None:
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.schema import initialize_schema

        db = ClusterDb(self.dsn)
        initialize_schema(db)
        with db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE england_cluster_task_attempts RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_cluster_tasks RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_dnb_discovery_nodes RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_dnb_segments RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_dnb_companies RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_source_files RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_companies RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_firecrawl_domain_cache RESTART IDENTITY CASCADE")

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
                cur.execute("TRUNCATE england_cluster_task_attempts RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_cluster_tasks RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_companies RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_firecrawl_domain_cache RESTART IDENTITY CASCADE")
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
        self.assertEqual(task.task_id, "t1")
        repo.complete_task(task_id=task.task_id, worker_id="worker-1", result={"emails": ["hello@gamma.test"]})
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT firecrawl_task_status, emails_json FROM england_ch_companies WHERE comp_id = 'cx'")
                row = cur.fetchone()
        self.assertEqual(row["firecrawl_task_status"], "done")
        self.assertIn("hello@gamma.test", row["emails_json"])

    def test_submit_companies_house_skips_unchanged_source(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.repository import ClusterRepository
        from england_crawler.cluster.schema import initialize_schema

        with tempfile.TemporaryDirectory() as tmp:
            self._reset_cluster_tables()
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd", "Beta Ltd"])

            config = ClusterConfig.from_env(ROOT)
            config.postgres_dsn = self.dsn
            db = ClusterDb(self.dsn)
            initialize_schema(db)
            repo = ClusterRepository(db, config)

            first_inserted = repo.submit_companies_house_input(input_xlsx)
            self.assertEqual(first_inserted, 2)

            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'leased', lease_owner = 'worker-x', updated_at = NOW()
                        WHERE pipeline = 'england_companies_house'
                          AND task_type = 'ch_lookup'
                          AND entity_id = (
                              SELECT comp_id
                              FROM england_ch_companies
                              WHERE normalized_name = 'ALPHA LTD'
                          )
                        """
                    )

            second_inserted = repo.submit_companies_house_input(input_xlsx)
            self.assertEqual(second_inserted, 0)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status
                        FROM england_cluster_tasks
                        WHERE pipeline = 'england_companies_house'
                          AND task_type = 'ch_lookup'
                          AND entity_id = (
                              SELECT comp_id
                              FROM england_ch_companies
                              WHERE normalized_name = 'ALPHA LTD'
                          )
                        """
                    )
                    task_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM england_ch_source_files
                        WHERE source_path = %s
                        """,
                        (f"{input_xlsx.resolve()}|full",),
                    )
                    source_row = cur.fetchone()
            self.assertIsNotNone(task_row)
            self.assertEqual(task_row["status"], "leased")
            self.assertEqual(int(source_row["count"]), 1)

    def test_submit_companies_house_skips_done_stage_for_existing_company(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.repository import ClusterRepository
        from england_crawler.cluster.schema import initialize_schema

        with tempfile.TemporaryDirectory() as tmp:
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd", "Beta Ltd"])

            config = ClusterConfig.from_env(ROOT)
            config.postgres_dsn = self.dsn
            db = ClusterDb(self.dsn)
            initialize_schema(db)
            repo = ClusterRepository(db, config)

            self.assertEqual(repo.submit_companies_house_input(input_xlsx, max_companies=1), 1)
            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE england_ch_companies
                        SET ch_task_status = 'done', gmap_task_status = 'done', updated_at = NOW()
                        WHERE normalized_name = 'ALPHA LTD'
                        """
                    )
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'done', updated_at = NOW()
                        WHERE pipeline = 'england_companies_house'
                          AND entity_id = (
                              SELECT comp_id FROM england_ch_companies WHERE normalized_name = 'ALPHA LTD'
                          )
                        """
                    )

            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd", "Beta Ltd", "Gamma Ltd"])
            inserted = repo.submit_companies_house_input(input_xlsx, max_companies=3)
            self.assertEqual(inserted, 2)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT task_type, status, COUNT(*) AS count
                        FROM england_cluster_tasks
                        WHERE pipeline = 'england_companies_house'
                        GROUP BY task_type, status
                        ORDER BY task_type, status
                        """
                    )
                    rows = cur.fetchall()
            summary = {(str(row["task_type"]), str(row["status"])): int(row["count"]) for row in rows}
            self.assertEqual(summary[("ch_lookup", "done")], 1)
            self.assertEqual(summary[("ch_gmap", "done")], 1)
            self.assertEqual(summary[("ch_lookup", "pending")], 2)
            self.assertEqual(summary[("ch_gmap", "pending")], 2)

    def test_submit_england_skips_done_dnb_and_submits_companies_house(self) -> None:
        from england_crawler.cluster.db import ClusterDb

        with tempfile.TemporaryDirectory() as tmp:
            self._reset_cluster_tables()
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd", "Beta Ltd"])

            db = ClusterDb(self.dsn)
            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO england_dnb_discovery_nodes(
                            segment_id, industry_path, country_iso_two_code, region_name, city_name,
                            expected_count, task_status, task_retries, updated_at
                        ) VALUES('seg-done','construction','gb','','',10,'done',0,NOW())
                        """
                    )
                    cur.execute(
                        """
                        INSERT INTO england_dnb_segments(
                            segment_id, industry_path, country_iso_two_code, region_name, city_name,
                            expected_count, next_page, task_status, task_retries, updated_at
                        ) VALUES('seg-done','construction','gb','','',10,1,'done',0,NOW())
                        """
                    )
                    cur.execute(
                        """
                        INSERT INTO england_dnb_companies(
                            duns, company_name_en_dnb, company_name_url, key_principal, address, city, region, country,
                            postal_code, sales_revenue, dnb_website, website, domain, website_source, company_name_en_gmap,
                            company_name_en_site, company_name_resolved, site_evidence_url, site_evidence_quote, site_confidence,
                            phone, emails_json, detail_done, detail_task_status, detail_task_retries, gmap_task_status,
                            gmap_task_retries, firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                        ) VALUES(
                            'dnb-done','Done Ltd','','','','','','United Kingdom','','','','https://done.test','done.test','gmap',
                            '','','','','',0,'','[]'::jsonb,TRUE,'done',0,'done',0,'done',0,'',NOW()
                        )
                        """
                    )

            code, output = self._run_cluster_command(
                ["submit", "England", "--input-xlsx", str(input_xlsx)]
            )
            self.assertEqual(code, 0)
            self.assertIn("DNB | 已完成，跳过", output)
            self.assertIn("Companies House | 新增任务 2", output)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM england_cluster_tasks
                        WHERE pipeline = 'england_companies_house' AND status = 'pending'
                        """
                    )
                    row = cur.fetchone()
            self.assertEqual(int(row["count"]), 4)

    def test_submit_england_requeues_failed_source(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.repository import ClusterRepository
        from england_crawler.cluster.schema import initialize_schema

        with tempfile.TemporaryDirectory() as tmp:
            self._reset_cluster_tables()
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd"])

            config = ClusterConfig.from_env(ROOT)
            config.postgres_dsn = self.dsn
            db = ClusterDb(self.dsn)
            initialize_schema(db)
            repo = ClusterRepository(db, config)
            self.assertEqual(repo.submit_companies_house_input(input_xlsx), 1)

            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'failed', retries = 5, last_error = 'boom'
                        WHERE pipeline = 'england_companies_house'
                        """
                    )
                    cur.execute(
                        """
                        UPDATE england_ch_companies
                        SET ch_task_status = 'failed', ch_task_retries = 5, gmap_task_status = 'failed', gmap_task_retries = 5,
                            last_error = 'boom', updated_at = NOW()
                        WHERE normalized_name = 'ALPHA LTD'
                        """
                    )

            code, output = self._run_cluster_command(
                ["submit", "England", "--input-xlsx", str(input_xlsx)]
            )
            self.assertEqual(code, 0)
            self.assertIn("Companies House | 已重挂失败任务 2", output)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, retries, last_error
                        FROM england_cluster_tasks
                        WHERE pipeline = 'england_companies_house'
                        ORDER BY task_type
                        """
                    )
                    task_rows = cur.fetchall()
            self.assertEqual([str(row["status"]) for row in task_rows], ["pending", "pending"])
            self.assertEqual([int(row["retries"]) for row in task_rows], [0, 0])
            self.assertEqual([str(row["last_error"]) for row in task_rows], ["", ""])

    def test_submit_england_prefers_new_companies_house_source_over_failed_only(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.db import ClusterDb
        from england_crawler.cluster.repository import ClusterRepository
        from england_crawler.cluster.schema import initialize_schema

        with tempfile.TemporaryDirectory() as tmp:
            self._reset_cluster_tables()
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd"])

            config = ClusterConfig.from_env(ROOT)
            config.postgres_dsn = self.dsn
            db = ClusterDb(self.dsn)
            initialize_schema(db)
            repo = ClusterRepository(db, config)
            self.assertEqual(repo.submit_companies_house_input(input_xlsx), 1)

            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE england_cluster_tasks
                        SET status = 'failed', retries = 5, last_error = 'boom'
                        WHERE pipeline = 'england_companies_house'
                        """
                    )
                    cur.execute(
                        """
                        UPDATE england_ch_companies
                        SET ch_task_status = 'failed', ch_task_retries = 5, gmap_task_status = 'failed', gmap_task_retries = 5,
                            last_error = 'boom', updated_at = NOW()
                        WHERE normalized_name = 'ALPHA LTD'
                        """
                    )

            time.sleep(0.01)
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd", "Beta Ltd"])

            code, output = self._run_cluster_command(
                ["submit", "England", "--input-xlsx", str(input_xlsx)]
            )
            self.assertEqual(code, 0)
            self.assertIn("Companies House | 新增任务 1", output)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT normalized_name, ch_task_status
                        FROM england_ch_companies
                        ORDER BY normalized_name
                        """
                    )
                    company_rows = cur.fetchall()
            self.assertEqual(
                [(str(row["normalized_name"]), str(row["ch_task_status"])) for row in company_rows],
                [("ALPHA LTD", "failed"), ("BETA LTD", "")],
            )

    def test_submit_england_reconciles_stale_done_ch_tasks_before_state_detection(self) -> None:
        from england_crawler.cluster.db import ClusterDb

        with tempfile.TemporaryDirectory() as tmp:
            self._reset_cluster_tables()
            input_xlsx = Path(tmp) / "docs" / "英国.xlsx"
            self._build_companies_house_xlsx(input_xlsx, ["Alpha Ltd"])

            db = ClusterDb(self.dsn)
            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO england_ch_companies(
                            comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                            phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                            firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                        ) VALUES(
                            'c-done','Alpha Ltd','ALPHA LTD','123','','Alice','https://alpha.test','alpha.test','',
                            '[]'::jsonb,'done',0,'done',0,'done',0,'',NOW()
                        )
                        """
                    )
                    cur.execute(
                        """
                        INSERT INTO england_ch_source_files(source_path, fingerprint, total_rows, updated_at)
                        VALUES(%s, %s, 1, NOW())
                        """,
                        (f"{input_xlsx.resolve()}|full", f"{input_xlsx.stat().st_mtime_ns}:{input_xlsx.stat().st_size}"),
                    )
                    cur.execute(
                        """
                        INSERT INTO england_cluster_tasks(task_id, pipeline, task_type, entity_id, status, retries, next_run_at, payload_json, created_at, updated_at)
                        VALUES
                        ('stale-ch-lookup','england_companies_house','ch_lookup','c-done','pending',0,NOW(),%s,NOW(),NOW()),
                        ('stale-ch-gmap','england_companies_house','ch_gmap','c-done','pending',0,NOW(),%s,NOW(),NOW())
                        """,
                        (Jsonb({"comp_id": "c-done", "company_name": "Alpha Ltd"}), Jsonb({"comp_id": "c-done", "company_name": "Alpha Ltd"})),
                    )

            code, output = self._run_cluster_command(["submit", "England", "--input-xlsx", str(input_xlsx)])
            self.assertEqual(code, 0)
            self.assertIn("Companies House | 已完成，跳过", output)

            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, COUNT(*) AS count
                        FROM england_cluster_tasks
                        WHERE pipeline = 'england_companies_house'
                        GROUP BY status
                        ORDER BY status
                        """
                    )
                    rows = cur.fetchall()
            self.assertEqual({str(row["status"]): int(row["count"]) for row in rows}, {"done": 2})

    def test_start_pools_does_not_construct_cluster_db(self) -> None:
        from england_crawler.cluster.cli import run_cluster

        buffer = io.StringIO()
        with patch("england_crawler.cluster.cli.ClusterDb", side_effect=AssertionError("start-pools 不该创建 ClusterDb")):
            with patch(
                "england_crawler.cluster.cli._start_local_worker_pools",
                return_value=(0, 0, [], []),
            ):
                with redirect_stdout(buffer):
                    code = run_cluster(["start-pools"])
        self.assertEqual(code, 0)
        self.assertIn("England 本机 worker 池已启动：新增 0，已在运行 0", buffer.getvalue())

    def test_worker_does_not_construct_cluster_db(self) -> None:
        from england_crawler.cluster.cli import run_cluster

        class _WorkerExit(RuntimeError):
            pass

        buffer = io.StringIO()
        with patch("england_crawler.cluster.cli.ClusterDb", side_effect=AssertionError("worker 不该创建 ClusterDb")):
            with patch("england_crawler.cluster.worker.ClusterWorkerRuntime.run_forever", side_effect=_WorkerExit):
                with redirect_stdout(buffer):
                    with self.assertRaises(_WorkerExit):
                        run_cluster(["worker", "ch-lookup"])

    def test_gmap_worker_init_does_not_touch_dnb_cookie_provider(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.worker import ClusterWorkerRuntime

        config = ClusterConfig.from_env(ROOT)
        with patch(
            "england_crawler.cluster.worker.DnbCookieProvider",
            side_effect=AssertionError("gmap worker 不该读取 9222 cookie"),
        ):
            worker = ClusterWorkerRuntime(config, role="gmap")

        self.assertIsNone(worker._dnb_client)

    def test_dnb_worker_requires_cookie_from_9222(self) -> None:
        from england_crawler.cluster.config import ClusterConfig
        from england_crawler.cluster.worker import ClusterWorkerRuntime

        class _Provider:
            def __init__(self, *args, **kwargs) -> None:
                return None

            def get(self, *, force_refresh: bool = False) -> str:
                return ""

        config = ClusterConfig.from_env(ROOT)
        with patch("england_crawler.cluster.worker.DnbCookieProvider", _Provider):
            with self.assertRaisesRegex(RuntimeError, "9222 浏览器未提供 DNB cookie"):
                ClusterWorkerRuntime(config, role="dnb-detail")

    def test_claim_task_marks_stale_done_task_without_execution(self) -> None:
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
                cur.execute("TRUNCATE england_cluster_task_attempts RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_cluster_tasks RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_companies RESTART IDENTITY CASCADE")
                cur.execute(
                    """
                    INSERT INTO england_ch_companies(
                        comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                        phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                        firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES('c-stale','Alpha Ltd','ALPHA LTD','123','','Alice','','','',
                             '[]'::jsonb,'done',0,'pending',0,'',0,'',NOW())
                    """
                )
                cur.execute(
                    """
                    INSERT INTO england_cluster_tasks(task_id, pipeline, task_type, entity_id, status, retries, next_run_at, payload_json, created_at, updated_at)
                    VALUES('t-stale','england_companies_house','ch_lookup','c-stale','pending',0,NOW(),%s,NOW(),NOW())
                    """,
                    (Jsonb({"comp_id": "c-stale", "company_name": "Alpha Ltd"}),),
                )

        task = repo.claim_task("worker-1", ["ch_lookup"])
        self.assertIsNone(task)
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM england_cluster_tasks WHERE task_id = 't-stale'")
                row = cur.fetchone()
        self.assertEqual("done", str(row["status"]))

    def test_renew_task_lease_extends_firecrawl_domain_cache_lease(self) -> None:
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
                cur.execute("TRUNCATE england_cluster_task_attempts RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_cluster_tasks RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_companies RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_firecrawl_domain_cache RESTART IDENTITY CASCADE")
                cur.execute(
                    """
                    INSERT INTO england_ch_companies(
                        comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                        phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                        firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES('cx','Gamma Ltd','GAMMA LTD','','','Bob','https://gamma.test','gamma.test','','[]'::jsonb,'done',0,'done',0,'pending',0,'',NOW())
                    """
                )
                cur.execute(
                    """
                    INSERT INTO england_cluster_tasks(task_id, pipeline, task_type, entity_id, status, retries, next_run_at, lease_owner, lease_expires_at, payload_json, created_at, updated_at)
                    VALUES('t-renew','england_companies_house','ch_firecrawl','cx','leased',0,NOW(),'worker-1',NOW() + interval '30 seconds',%s,NOW(),NOW())
                    """,
                    (Jsonb({"comp_id": "cx", "company_name": "Gamma Ltd", "homepage": "https://gamma.test", "domain": "gamma.test"}),),
                )
                cur.execute(
                    """
                    INSERT INTO england_firecrawl_domain_cache(domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at)
                    VALUES('gamma.test','running','[]'::jsonb,NULL,'worker-1',NOW() + interval '30 seconds','',NOW())
                    """
                )

        repo.renew_task_lease(task_id="t-renew", worker_id="worker-1")

        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT lease_expires_at FROM england_cluster_tasks WHERE task_id = 't-renew'")
                task_row = cur.fetchone()
                cur.execute("SELECT lease_expires_at FROM england_firecrawl_domain_cache WHERE domain = 'gamma.test'")
                cache_row = cur.fetchone()
        self.assertIsNotNone(task_row["lease_expires_at"])
        self.assertIsNotNone(cache_row["lease_expires_at"])

    def test_firecrawl_claim_respects_next_retry_at(self) -> None:
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
                cur.execute("TRUNCATE england_cluster_task_attempts RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_cluster_tasks RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_ch_companies RESTART IDENTITY CASCADE")
                cur.execute("TRUNCATE england_firecrawl_domain_cache RESTART IDENTITY CASCADE")
                cur.execute(
                    """
                    INSERT INTO england_ch_companies(
                        comp_id, company_name, normalized_name, company_number, company_status, ceo, homepage, domain,
                        phone, emails_json, ch_task_status, ch_task_retries, gmap_task_status, gmap_task_retries,
                        firecrawl_task_status, firecrawl_task_retries, last_error, updated_at
                    ) VALUES('cy','Delta Ltd','DELTA LTD','','','Bob','https://delta.test','delta.test','','[]'::jsonb,'done',0,'done',0,'pending',0,'',NOW())
                    """
                )
                cur.execute(
                    """
                    INSERT INTO england_cluster_tasks(task_id, pipeline, task_type, entity_id, status, retries, next_run_at, payload_json, created_at, updated_at)
                    VALUES('t-wait','england_companies_house','ch_firecrawl','cy','pending',0,NOW(),%s,NOW(),NOW())
                    """,
                    (Jsonb({"comp_id": "cy", "company_name": "Delta Ltd", "homepage": "https://delta.test", "domain": "delta.test"}),),
                )
                cur.execute(
                    """
                    INSERT INTO england_firecrawl_domain_cache(domain, status, emails_json, next_retry_at, lease_owner, lease_expires_at, last_error, updated_at)
                    VALUES('delta.test','pending','[]'::jsonb,NOW() + interval '120 seconds','',NULL,'boom',NOW())
                    """
                )

        task = repo.claim_task("worker-1", ["ch_firecrawl"])
        self.assertIsNone(task)
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status, next_run_at FROM england_cluster_tasks WHERE task_id = 't-wait'")
                row = cur.fetchone()
        self.assertEqual("pending", row["status"])
        self.assertIsNotNone(row["next_run_at"])
