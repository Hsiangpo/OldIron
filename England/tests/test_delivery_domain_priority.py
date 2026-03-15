import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from england_crawler.delivery import build_delivery_bundle  # noqa: E402


class DeliveryDomainPriorityTests(unittest.TestCase):
    def test_build_delivery_bundle_deduplicates_by_company_name_across_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            catch_dir = data_root / "catch"
            dnb_dir = data_root / "dnb"
            catch_dir.mkdir(parents=True)
            dnb_dir.mkdir(parents=True)

            catch_row = {
                "comp_id": "C1",
                "company_name": "Acme Services Ltd",
                "ceo": "Alice",
                "homepage": "https://acme-services.co.uk",
                "emails": ["info@acme-services.co.uk"],
            }
            dnb_row = {
                "duns": "D1",
                "company_name": "Acme Services Ltd",
                "ceo": "Bob",
                "homepage": "https://acme-group.co.uk",
                "emails": ["global@acme-group.co.uk"],
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
            csv_path = delivery_root / "England_day001" / "companies.csv"
            with csv_path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Acme Services Ltd", rows[0]["company_name"])

    def test_build_delivery_bundle_keeps_different_company_names_with_same_domain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            site_a = data_root / "a"
            site_b = data_root / "b"
            site_a.mkdir(parents=True)
            site_b.mkdir(parents=True)

            row_a = {
                "comp_id": "A1",
                "company_name": "Parent Holdings Ltd",
                "ceo": "Alice",
                "homepage": "https://example.com",
                "emails": ["a@example.com"],
            }
            row_b = {
                "comp_id": "B1",
                "company_name": "Parent Trading Ltd",
                "ceo": "Bob",
                "homepage": "https://example.com",
                "emails": ["b@example.com"],
            }
            (site_a / "final_companies.jsonl").write_text(
                json.dumps(row_a, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            (site_b / "final_companies.jsonl").write_text(
                json.dumps(row_b, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(
                data_root=data_root,
                delivery_root=delivery_root,
                day_label="day1",
            )

            self.assertEqual(2, summary["total_current_companies"])

    def test_build_delivery_bundle_accepts_legacy_baseline_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            previous_dir = delivery_root / "England_day002"
            previous_dir.mkdir(parents=True)
            dnb_dir = data_root / "dnb"
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
            csv_path = delivery_root / "England_day003" / "companies.csv"
            with csv_path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(0, len(rows))

    def test_build_delivery_bundle_filters_suspicious_foreign_match_for_uk_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            site_dir = data_root / "companies_house"
            site_dir.mkdir(parents=True)

            bad_row = {
                "comp_id": "C1",
                "company_name": "Leader (UK) Ltd",
                "ceo": "Alice",
                "homepage": "https://www.jointleader.com.hk",
                "phone": "+852 2111 2884",
                "emails": ["info@jointleader.com.hk"],
            }
            good_row = {
                "comp_id": "C2",
                "company_name": "Acme Services Ltd",
                "ceo": "Bob",
                "homepage": "https://acme-services.co.uk",
                "phone": "020 7946 0958",
                "emails": ["info@acme-services.co.uk"],
            }

            (site_dir / "final_companies.jsonl").write_text(
                json.dumps(bad_row, ensure_ascii=False) + "\n" + json.dumps(good_row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(
                data_root=data_root,
                delivery_root=delivery_root,
                day_label="day1",
            )

            self.assertEqual(1, summary["total_current_companies"])
            csv_path = delivery_root / "England_day001" / "companies.csv"
            with csv_path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Acme Services Ltd", rows[0]["company_name"])

    def test_build_delivery_bundle_overwrites_locked_day_dir_by_reusing_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_root = root / "output"
            delivery_root = data_root / "delivery"
            site_dir = data_root / "companies_house"
            day_dir = delivery_root / "England_day001"
            site_dir.mkdir(parents=True)
            day_dir.mkdir(parents=True)
            (day_dir / "companies.csv").write_text("old", encoding="utf-8")
            (day_dir / "keys.txt").write_text("old", encoding="utf-8")
            (day_dir / "summary.json").write_text("{}", encoding="utf-8")

            row = {
                "comp_id": "C1",
                "company_name": "Acme Services Ltd",
                "ceo": "Alice",
                "homepage": "https://acme-services.co.uk",
                "emails": ["info@acme-services.co.uk"],
            }
            (site_dir / "final_companies.jsonl").write_text(
                json.dumps(row, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            with patch("england_crawler.delivery.shutil.rmtree", side_effect=PermissionError("locked")):
                summary = build_delivery_bundle(
                    data_root=data_root,
                    delivery_root=delivery_root,
                    day_label="day1",
                )

            self.assertEqual(1, summary["total_current_companies"])
            with (day_dir / "companies.csv").open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
            self.assertEqual(1, len(rows))
            self.assertEqual("Acme Services Ltd", rows[0]["company_name"])


if __name__ == "__main__":
    unittest.main()
