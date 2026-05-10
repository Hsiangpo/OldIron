"""CNPJ Biz 专用 Chrome 启动器测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.browser_launcher import CnpjBizChromeLauncher


class BrowserLauncherTests(unittest.TestCase):
    def test_launch_builds_expected_open_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CnpjBizChromeLauncher(
                debug_port=9226,
                profile_dir=tmpdir,
                proxy_url="http://127.0.0.1:7893",
                seed_url="https://cnpj.biz/empresas/estado/SP",
            )
            with patch("brazil_crawler.sites.cnpjbiz.browser_launcher.subprocess.Popen") as popen:
                launcher.launch()
            args = popen.call_args.args[0]
            self.assertIn("--remote-debugging-port=9226", args)
            self.assertIn("--proxy-server=http://127.0.0.1:7893", args)
            self.assertEqual("https://cnpj.biz/empresas/estado/SP", args[-1])
