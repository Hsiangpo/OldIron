from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandGMapTests(unittest.TestCase):
    def test_domain_match_score_rewards_exact_company_label(self) -> None:
        from england_crawler.google_maps.client import _domain_match_score

        self.assertGreaterEqual(
            _domain_match_score("Daewon SPIC Co., Ltd.", "https://www.daewonspic.com"),
            80,
        )
        self.assertEqual(
            0,
            _domain_match_score("Daewon SPIC Co., Ltd.", "https://www.wikipedia.org"),
        )

    def test_pick_best_candidate_accepts_strong_domain_even_without_local_name(self) -> None:
        from england_crawler.google_maps.client import _pick_best_candidate

        picked = _pick_best_candidate(
            [
                {
                    "name": "Daewon SPIC Co., Ltd.",
                    "company_name_local": "",
                    "phone": "",
                    "website": "https://www.daewonspic.com",
                },
                {
                    "name": "Daewon Logistics",
                    "company_name_local": "",
                    "phone": "",
                    "website": "https://www.example.com",
                },
            ],
            "Daewon SPIC Co., Ltd.",
        )

        self.assertIsNotNone(picked)
        self.assertEqual("https://www.daewonspic.com", picked["website"])

    def test_pick_best_candidate_rejects_low_match_domains(self) -> None:
        from england_crawler.google_maps.client import _pick_best_candidate

        picked = _pick_best_candidate(
            [
                {
                    "name": "Directory Listing",
                    "company_name_local": "",
                    "phone": "",
                    "website": "https://www.example-directory.com",
                }
            ],
            "Hanmi Information Technology Co., Ltd.",
        )

        self.assertIsNone(picked)

    def test_extract_place_candidates_parses_website_and_phone(self) -> None:
        from england_crawler.google_maps.client import _extract_place_candidates

        payload = [
            [
                "Daewon SPIC Co., Ltd.",
                [
                    "0x0:0x1",
                    "Daewon SPIC Co., Ltd.",
                    "https://www.daewonspic.com",
                    "+44 20 7946 0958",
                ],
            ]
        ]

        candidates = _extract_place_candidates(payload, "Daewon SPIC Co., Ltd.")

        self.assertTrue(candidates)
        self.assertEqual("https://www.daewonspic.com", candidates[0]["website"])
        self.assertEqual("+44 20 7946 0958", candidates[0]["phone"])

    def test_google_maps_defaults_use_uk_locale(self) -> None:
        from england_crawler.google_maps.client import GoogleMapsConfig

        config = GoogleMapsConfig()

        self.assertEqual("en", config.hl)
        self.assertEqual("gb", config.gl)

    def test_google_maps_default_proxy_points_to_local_7897(self) -> None:
        from england_crawler.google_maps.client import GoogleMapsConfig

        with patch.dict(os.environ, {}, clear=True):
            config = GoogleMapsConfig()

        self.assertEqual("socks5h://127.0.0.1:7897", config.proxy_url)

    def test_google_maps_session_uses_configured_proxy(self) -> None:
        from england_crawler.google_maps.client import GoogleMapsClient
        from england_crawler.google_maps.client import GoogleMapsConfig

        created: dict[str, object] = {}

        class _FakeSession:
            def __init__(self, *, impersonate: str) -> None:
                created["impersonate"] = impersonate
                self.trust_env = True
                self.proxies = {}

        with patch("england_crawler.google_maps.client.cffi_requests.Session", _FakeSession):
            client = GoogleMapsClient(
                GoogleMapsConfig(proxy_url="socks5h://127.0.0.1:7897")
            )

        self.assertEqual("chrome", created["impersonate"])
        self.assertIs(False, client.session.trust_env)
        self.assertEqual(
            {"http": "socks5h://127.0.0.1:7897", "https": "socks5h://127.0.0.1:7897"},
            client.session.proxies,
        )

    def test_pick_best_candidate_rejects_foreign_phone_and_hk_domain_for_uk_company(self) -> None:
        from england_crawler.google_maps.client import _pick_best_candidate

        picked = _pick_best_candidate(
            [
                {
                    "name": "UKAP (EK) LIMITED",
                    "company_name_local": "",
                    "phone": "+852 2735 7268",
                    "website": "https://ukea.org",
                },
                {
                    "name": "UKAP (EK) LIMITED",
                    "company_name_local": "",
                    "phone": "+852 2111 2884",
                    "website": "https://jointleader.com.hk",
                },
            ],
            "UKAP (EK) LIMITED",
        )

        self.assertIsNone(picked)


if __name__ == "__main__":
    unittest.main()
