from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class EmailConcurrencyDefaultsTests(unittest.TestCase):
    def test_email_worker_defaults_are_128(self) -> None:
        expectations = {
            "England/src/england_crawler/sites/companyname/cli.py": 'default=128',
            "England/src/england_crawler/sites/companyname/config.py": "firecrawl_workers: int = 128",
            "England/src/england_crawler/sites/companyname/config.py::from_env": "firecrawl_workers: int = 128",
            "Denmark/src/denmark_crawler/sites/proff/cli.py": 'default=128',
            "Denmark/src/denmark_crawler/sites/virk/cli.py": 'default=128',
            "Denmark/src/denmark_crawler/sites/virk/config.py": "firecrawl_workers: int = 128",
            "Denmark/src/denmark_crawler/sites/virk/config.py::from_env": "firecrawl_workers: int = 128",
            "Finland/src/finland_crawler/sites/duunitori/cli.py": 'default=128',
            "Finland/src/finland_crawler/sites/jobly/cli.py": 'default=128',
            "Finland/src/finland_crawler/sites/tyomarkkinatori/cli.py": 'default=128',
            "Japan/src/japan_crawler/sites/bizmaps/cli.py": 'default=128',
            "Japan/src/japan_crawler/sites/hellowork/cli.py": 'default=128',
            "Brazil/src/brazil_crawler/sites/dnb/cli.py": 'default=128',
            "UnitedStates/src/unitedstates_crawler/sites/dnb/cli.py": 'default=128',
        }

        for label, needle in expectations.items():
            file_path = label.split("::", 1)[0]
            content = (ROOT / file_path).read_text(encoding="utf-8")
            self.assertIn(needle, content, msg=f"{label} should contain {needle!r}")

    def test_email_worker_stagger_is_0_1_seconds(self) -> None:
        files = [
            "England/src/england_crawler/sites/companyname/pipeline.py",
            "Denmark/src/denmark_crawler/sites/proff/pipeline.py",
            "Denmark/src/denmark_crawler/sites/virk/pipeline.py",
            "Finland/src/finland_crawler/sites/duunitori/pipeline.py",
            "Finland/src/finland_crawler/sites/jobly/pipeline.py",
            "Finland/src/finland_crawler/sites/tyomarkkinatori/pipeline.py",
        ]

        for relative_path in files:
            content = (ROOT / relative_path).read_text(encoding="utf-8")
            self.assertIn("time.sleep(0.1)", content, msg=f"{relative_path} should stagger email workers by 0.1s")


if __name__ == "__main__":
    unittest.main()
