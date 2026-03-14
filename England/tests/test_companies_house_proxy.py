import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class CompaniesHouseProxyTests(unittest.TestCase):
    def test_build_blurpath_proxy_username_includes_region_and_sticky_session(self) -> None:
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        config = BlurpathProxyConfig(
            enabled=True,
            host="blurpath.net",
            port=15138,
            username="kytqhwcsfml",
            password="secret",
            region="GB",
            sticky_minutes=10,
        )

        username = config.build_username("AbCd")

        self.assertEqual(
            "kytqhwcsfml-zone-resi-region-GB-st--city--session-AbCd-sessionTime-10",
            username,
        )

    def test_build_proxy_url_embeds_generated_username(self) -> None:
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        config = BlurpathProxyConfig(
            enabled=True,
            host="blurpath.net",
            port=15138,
            username="kytqhwcsfml",
            password="secret",
            region="GB",
            sticky_minutes=10,
        )

        proxy_url = config.build_proxy_url("AbCd")

        self.assertEqual(
            "http://kytqhwcsfml-zone-resi-region-GB-st--city--session-AbCd-sessionTime-10:secret@blurpath.net:15138",
            proxy_url,
        )

    def test_build_curl_options_includes_preproxy(self) -> None:
        from curl_cffi import CurlOpt
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        config = BlurpathProxyConfig(
            enabled=True,
            host="blurpath.net",
            port=15138,
            username="kytqhwcsfml",
            password="secret",
            region="GB",
            sticky_minutes=10,
            preproxy_url="socks5h://127.0.0.1:7897",
        )

        self.assertEqual(
            {CurlOpt.PRE_PROXY: "socks5h://127.0.0.1:7897"},
            config.build_curl_options(),
        )

    def test_config_treats_zero_proxy_enabled_as_false(self) -> None:
        from england_crawler.companies_house.config import CompaniesHouseConfig

        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"BLURPATH_CH_PROXY_ENABLED": "0"},
            clear=False,
        ):
            root = Path(tmp)
            config = CompaniesHouseConfig.from_env(
                project_root=root,
                input_xlsx=root / "英国.xlsx",
                output_dir=root / "output",
                max_companies=0,
                ch_workers=1,
                gmap_workers=1,
                snov_workers=1,
            )

        self.assertFalse(config.ch_proxy.enabled)
