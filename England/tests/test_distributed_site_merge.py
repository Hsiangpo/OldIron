import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DistributedSiteMergeTests(unittest.TestCase):
    def test_merge_site_runs_prefers_richer_duplicate_record(self) -> None:
        from england_crawler.distributed.site_merge import merge_site_runs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_a = root / "run-a"
            run_b = root / "run-b"
            run_a.mkdir()
            run_b.mkdir()
            (run_a / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "https://alpha.co.uk",
                        "domain": "alpha.co.uk",
                        "emails": ["alice@alpha.co.uk"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )
            (run_b / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "https://alpha.co.uk",
                        "domain": "alpha.co.uk",
                        "emails": ["alice@alpha.co.uk", "team@alpha.co.uk"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )

            output_dir = root / "merged"
            summary = merge_site_runs([run_a, run_b], output_dir)
            rows = [
                json.loads(line)
                for line in (output_dir / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(1, summary["merged_companies"])
        self.assertEqual(1, len(rows))
        self.assertEqual(2, len(rows[0]["emails"]))

    def test_merge_site_runs_keeps_different_names_with_same_domain(self) -> None:
        from england_crawler.distributed.site_merge import merge_site_runs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_a = root / "run-a"
            run_b = root / "run-b"
            run_a.mkdir()
            run_b.mkdir()
            (run_a / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Parent Holdings Ltd",
                        "ceo": "Alice",
                        "homepage": "https://example.com",
                        "domain": "example.com",
                        "emails": ["a@example.com"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )
            (run_b / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Parent Trading Ltd",
                        "ceo": "Bob",
                        "homepage": "https://example.com",
                        "domain": "example.com",
                        "emails": ["b@example.com"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )

            output_dir = root / "merged"
            summary = merge_site_runs([run_a, run_b], output_dir)
            rows = [
                json.loads(line)
                for line in (output_dir / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(2, summary["merged_companies"])
        self.assertEqual(2, len(rows))

    def test_merge_site_runs_merges_same_company_with_and_without_domain(self) -> None:
        from england_crawler.distributed.site_merge import merge_site_runs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_a = root / "run-a"
            run_b = root / "run-b"
            run_a.mkdir()
            run_b.mkdir()
            (run_a / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "",
                        "domain": "",
                        "emails": [],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )
            (run_b / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "https://alpha.co.uk",
                        "domain": "alpha.co.uk",
                        "emails": ["alice@alpha.co.uk"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )

            output_dir = root / "merged"
            summary = merge_site_runs([run_a, run_b], output_dir)
            rows = [
                json.loads(line)
                for line in (output_dir / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(1, summary["merged_companies"])
        self.assertEqual(1, len(rows))
        self.assertEqual(["alice@alpha.co.uk"], rows[0]["emails"])

    def test_merge_site_runs_prefers_companies_with_emails_snapshot(self) -> None:
        from england_crawler.distributed.site_merge import merge_site_runs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "run"
            run_dir.mkdir()
            (run_dir / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "https://alpha.co.uk",
                        "domain": "alpha.co.uk",
                        "emails": [],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )
            (run_dir / "companies_with_emails.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "Alpha Ltd",
                        "ceo": "Alice",
                        "homepage": "https://alpha.co.uk",
                        "domain": "alpha.co.uk",
                        "emails": ["alice@alpha.co.uk"],
                    },
                    ensure_ascii=False,
                ) + "\n",
                encoding="utf-8",
            )

            output_dir = root / "merged"
            summary = merge_site_runs([run_dir], output_dir)
            rows = [
                json.loads(line)
                for line in (output_dir / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(1, summary["merged_companies"])
        self.assertEqual(["alice@alpha.co.uk"], rows[0]["emails"])


if __name__ == "__main__":
    unittest.main()
