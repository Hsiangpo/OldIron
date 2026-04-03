from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SHARED_ROOT = ROOT / "shared"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))


class PrepareDeliveryDirTests(unittest.TestCase):
    def test_prepare_delivery_dir_moves_existing_dir_to_recycle_before_recreate(self) -> None:
        from oldiron_core.delivery.engine import prepare_delivery_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            delivery_dir = root / "England_day001"
            recycle_dir = root / "recycle_bin"
            recycle_dir.mkdir()
            delivery_dir.mkdir()
            (delivery_dir / "companies.csv").write_text("old", encoding="utf-8")

            def fake_move(target: Path) -> None:
                shutil.move(str(target), str(recycle_dir / target.name))

            with patch("oldiron_core.delivery.engine.move_path_to_recycle_bin", side_effect=fake_move) as mocked:
                prepare_delivery_dir(delivery_dir)

            self.assertEqual(1, mocked.call_count)
            self.assertTrue(delivery_dir.exists())
            self.assertEqual([], list(delivery_dir.iterdir()))
            self.assertTrue((recycle_dir / "England_day001" / "companies.csv").exists())


if __name__ == "__main__":
    unittest.main()
