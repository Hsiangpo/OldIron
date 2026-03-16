import json
import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DistributedChPlannerTests(unittest.TestCase):
    def test_plan_companies_house_shards_writes_manifest_and_shards(self) -> None:
        from england_crawler.distributed.ch_planner import plan_companies_house_shards

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "英国.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["CompanyName"])
            sheet.append(["ALPHA LTD"])
            sheet.append(["BETA LTD"])
            sheet.append(["GAMMA LTD"])
            workbook.save(source_path)

            output_dir = root / "plan"
            summary = plan_companies_house_shards(source_path, output_dir, shard_count=2)

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            shard_files = sorted(output_dir.glob("shard-*.txt"))

        self.assertEqual(3, summary["total_companies"])
        self.assertEqual(2, manifest["shard_count"])
        self.assertEqual(2, len(shard_files))


if __name__ == "__main__":
    unittest.main()
