import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DistributedDnbPlannerTests(unittest.TestCase):
    def test_build_leaf_seed_rows_uses_leaf_industries_only(self) -> None:
        from england_crawler.distributed.dnb_planner import build_leaf_seed_rows

        rows = build_leaf_seed_rows("gb")
        industry_paths = {str(row["industry_path"]) for row in rows}

        self.assertEqual(308, len(rows))
        self.assertNotIn("construction", industry_paths)
        self.assertIn("general_medical_and_surgical_hospitals", industry_paths)
        self.assertIn("management_of_companies_and_enterprises", industry_paths)

    def test_plan_dnb_shards_writes_manifest_and_segments(self) -> None:
        from england_crawler.distributed.dnb_planner import plan_dnb_shards

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "plan"
            summary = plan_dnb_shards(output_dir, shard_count=3, country_iso_two_code="gb")

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            shard_files = sorted(output_dir.glob("shard-*.segments.jsonl"))

        self.assertEqual(308, summary["total_segments"])
        self.assertEqual(3, manifest["shard_count"])
        self.assertEqual(3, len(shard_files))


if __name__ == "__main__":
    unittest.main()
