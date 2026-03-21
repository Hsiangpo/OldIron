import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from korea_crawler.delivery import build_delivery_bundle  # noqa: E402


class DeliveryDomainPriorityTests(unittest.TestCase):
    def test_build_delivery_bundle_deduplicates_by_domain_across_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            catch_dir = data_root / "catch"
            dnb_dir = data_root / "dnbkorea"
            catch_dir.mkdir(parents=True)
            dnb_dir.mkdir(parents=True)

            catch_row = {
                "comp_id": "C1",
                "company_name": "삼성물산",
                "ceo": "오세철",
                "homepage": "https://www.samsungcnt.com",
                "emails": ["info@samsungcnt.com"],
            }
            dnb_row = {
                "duns": "D1",
                "company_name": "Samsung C&T Corporation",
                "ceo": "Se Cheol O",
                "homepage": "https://www.samsungcnt.com",
                "emails": ["global@samsungcnt.com"],
            }

            (catch_dir / "final_companies.jsonl").write_text(
                json.dumps(catch_row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            (dnb_dir / "final_companies.jsonl").write_text(
                json.dumps(dnb_row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(
                data_root=data_root,
                delivery_root=delivery_root,
                day_label="day1",
            )

            self.assertEqual(1, summary["total_current_companies"])
            csv_path = delivery_root / "SouthKorea_day001" / "companies.csv"
            with csv_path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("삼성물산", rows[0]["company_name"])

    def test_build_delivery_bundle_accepts_legacy_baseline_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            previous_dir = delivery_root / "SouthKorea_day002"
            previous_dir.mkdir(parents=True)
            dnb_dir = data_root / "dnbkorea"
            dnb_dir.mkdir(parents=True)

            # 历史 keys.txt 格式：公司名标准化|域名
            (previous_dir / "keys.txt").write_text(
                "삼성물산|samsungcnt.com\n",
                encoding="utf-8",
            )
            (previous_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "day": 2,
                        "baseline_day": 1,
                        "total_current_companies": 1,
                        "delta_companies": 0,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            dnb_row = {
                "duns": "D1",
                "company_name": "Samsung C&T Corporation",
                "ceo": "Se Cheol O",
                "homepage": "https://www.samsungcnt.com",
                "emails": ["global@samsungcnt.com"],
            }
            (dnb_dir / "final_companies.jsonl").write_text(
                json.dumps(dnb_row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(
                data_root=data_root,
                delivery_root=delivery_root,
                day_label="day3",
            )

            self.assertEqual(1, summary["total_current_companies"])
            self.assertEqual(0, summary["delta_companies"])
            csv_path = delivery_root / "SouthKorea_day003" / "companies.csv"
            with csv_path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(0, len(rows))


if __name__ == "__main__":
    unittest.main()
