from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from italy_crawler.delivery import build_delivery_bundle
from italy_crawler.sites.wiza.client import COMPANY_FILTER
from italy_crawler.sites.wiza.pipeline import run_pipeline_list as run_wiza_pipeline_list
from italy_crawler.sites.wiza.store import ItalyWizaStore


class ItalyWizaTests(unittest.TestCase):
    def test_company_filter_targets_italy(self) -> None:
        self.assertEqual(COMPANY_FILTER["v"], "italy")

    def test_wiza_list_usage_limit_still_exports_current_websites_txt(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = ItalyWizaStore(output_dir / "companies.db")

            class FakeClient:
                def __init__(self, output_dir: Path, proxy: str) -> None:
                    del output_dir, proxy
                    self._calls = 0

                def search_companies(self, *, search_after, page_size):
                    del search_after, page_size
                    self._calls += 1
                    if self._calls == 1:
                        return SimpleNamespace(
                            items=[{"name": "Example SRL", "website": "example.it"}],
                            total=0,
                            total_relation="gte",
                            page_size=100,
                            last_sort=["next-page"],
                        )
                    if self._calls == 2:
                        raise RuntimeError("Wiza 当前账号搜索额度已用尽，暂时无法继续抓公司列表。")
                    return SimpleNamespace(
                        items=[],
                        total=0,
                        total_relation="gte",
                        page_size=100,
                        last_sort=[],
                    )

                def close(self) -> None:
                    return None

            with patch("italy_crawler.sites.wiza.pipeline.WizaClient", FakeClient):
                with patch("time.sleep", return_value=None):
                    result = run_wiza_pipeline_list(
                        output_dir=output_dir,
                        request_delay=0,
                        proxy="",
                        max_pages=0,
                        concurrency=1,
                    )

            self.assertEqual(store.get_company_count(), 1)
            lines = (output_dir / "websites.txt").read_text(encoding="utf-8").splitlines()

        self.assertEqual(result["pages"], 1)
        self.assertEqual(lines, ["https://example.it"])

    def test_websites_delivery_uses_independent_day_sequence(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            data_root = root / "output"
            delivery_root = root / "delivery"
            wiza_dir = data_root / "wiza"
            wiza_dir.mkdir(parents=True, exist_ok=True)
            (wiza_dir / "websites.txt").write_text(
                "https://example.it\nhttps://example.it\nhttps://another.it\n",
                encoding="utf-8",
            )

            summary = build_delivery_bundle(data_root, delivery_root, "day1", delivery_kind="websites")

            package_dir = delivery_root / "Italy_websites_day001"
            with (package_dir / "wiza.csv").open("r", encoding="utf-8-sig", newline="") as fp:
                rows = list(csv.DictReader(fp))

        self.assertEqual(summary["day"], 1)
        self.assertEqual(summary["baseline_day"], 0)
        self.assertEqual(summary["delta_websites"], 2)
        self.assertEqual(summary["total_current_websites"], 2)
        self.assertEqual(rows, [
            {"website": "https://another.it"},
            {"website": "https://example.it"},
        ])
