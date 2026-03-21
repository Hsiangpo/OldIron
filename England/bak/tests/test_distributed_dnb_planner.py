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
    def test_build_seed_rows_uses_root_catalog(self) -> None:
        from england_crawler.distributed.dnb_planner import build_seed_rows
        from england_crawler.dnb.catalog import INDUSTRY_CATEGORY_COUNT

        rows = build_seed_rows("gb")
        segment_ids = {str(row["segment_id"]) for row in rows}

        self.assertEqual(INDUSTRY_CATEGORY_COUNT, len(rows))
        self.assertIn("construction|gb||", segment_ids)
        self.assertIn("management_of_companies_and_enterprises|gb||", segment_ids)
        self.assertNotIn("general_medical_and_surgical_hospitals|gb||", segment_ids)

    def test_plan_dnb_shards_writes_manifest_and_segments(self) -> None:
        from england_crawler.distributed.dnb_planner import plan_dnb_shards
        from england_crawler.dnb.catalog import INDUSTRY_CATEGORY_COUNT

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "plan"
            summary = plan_dnb_shards(output_dir, shard_count=3, country_iso_two_code="gb")

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            shard_files = sorted(output_dir.glob("shard-*.segments.jsonl"))
            shard_segment_ids: list[str] = []
            for shard_file in shard_files:
                for line in shard_file.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    shard_segment_ids.append(str(json.loads(line)["segment_id"]))

        self.assertEqual(INDUSTRY_CATEGORY_COUNT, summary["total_segments"])
        self.assertEqual(3, manifest["shard_count"])
        self.assertEqual(3, len(shard_files))
        self.assertEqual(INDUSTRY_CATEGORY_COUNT, len(shard_segment_ids))
        self.assertEqual(len(shard_segment_ids), len(set(shard_segment_ids)))


if __name__ == "__main__":
    unittest.main()
