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
        from england_crawler.companies_house.client import normalize_company_name

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
            shard_names: list[str] = []
            for shard_file in shard_files:
                shard_names.extend(
                    [line.strip() for line in shard_file.read_text(encoding="utf-8").splitlines() if line.strip()]
                )

        self.assertEqual(3, summary["total_companies"])
        self.assertEqual(2, manifest["shard_count"])
        self.assertEqual(2, len(shard_files))
        self.assertEqual(
            {"ALPHA LTD", "BETA LTD", "GAMMA LTD"},
            {normalize_company_name(name) for name in shard_names},
        )
        self.assertEqual(
            len(shard_names),
            len({normalize_company_name(name) for name in shard_names}),
        )


if __name__ == "__main__":
    unittest.main()
