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


if __name__ == "__main__":
    unittest.main()
