"""Google Maps 共享过滤规则测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from oldiron_core.google_maps.client import _candidate_score
from oldiron_core.google_maps.client import GoogleMapsClient
from oldiron_core.google_maps.client import GoogleMapsConfig
from oldiron_core.google_maps.client import _is_blocked_host
from oldiron_core.google_maps.client import _looks_like_query_artifact_name
from oldiron_core.google_maps.client import _normalize_url
from oldiron_core.google_maps.client import _unwrap_google_url


class GoogleMapsClientTests(unittest.TestCase):
    def test_normalize_url_rejects_invalid_host(self) -> None:
        self.assertEqual("", _normalize_url("https://Show..."))
        self.assertEqual("", _normalize_url("https://obg."))
        self.assertEqual("", _normalize_url("https://$0.00"))
        self.assertEqual("", _normalize_url("https://[SolèaAtelier]"))

    def test_unwrap_google_url_rejects_invalid_ipv6_like_url(self) -> None:
        self.assertEqual("", _unwrap_google_url("http://[broken"))

    def test_blocked_host_filters_portal_and_gov(self) -> None:
        self.assertTrue(_is_blocked_host("booking.com"))
        self.assertTrue(_is_blocked_host("media.staticontent.com"))
        self.assertTrue(_is_blocked_host("www.amazonas.am.gov.br"))
        self.assertTrue(_is_blocked_host("www.viaverdeshopping.com.br"))

    def test_query_artifact_name_is_detected(self) -> None:
        self.assertTrue(
            _looks_like_query_artifact_name(
                "MARLON RAMALIO NASCIMENTO SANTOS",
                "MARLON RAMALIO NASCIMENTO SANTOS Maragogi Alagoas Brazil",
            )
        )
        self.assertFalse(
            _looks_like_query_artifact_name(
                "MLABS SOFTWARE SA",
                "mLabs",
            )
        )

    def test_candidate_score_penalizes_query_artifact_with_weak_domain(self) -> None:
        score = _candidate_score(
            "MARLON RAMALIO NASCIMENTO SANTOS",
            {
                "name": "MARLON RAMALIO NASCIMENTO SANTOS Maragogi Alagoas Brazil",
                "website": "https://bluepillow.com",
                "phone": "+55 82 99177-6903",
            },
        )
        self.assertLess(score, 45)

    def test_candidate_score_keeps_official_domain(self) -> None:
        score = _candidate_score(
            "VILA DE TAIPA EXCLUSIVE HOTEL LTDA",
            {
                "name": "VILA DE TAIPA EXCLUSIVE HOTEL LTDA Japaratinga Alagoas Brazil",
                "website": "https://www.viladetaipa.com.br",
                "phone": "+55 82 99120-4553",
            },
        )
        self.assertGreaterEqual(score, 45)

    def test_sleep_does_not_apply_count_based_long_rest(self) -> None:
        client = GoogleMapsClient.__new__(GoogleMapsClient)
        client.config = GoogleMapsConfig(
            min_delay=0.1,
            max_delay=0.1,
            long_rest_interval=1,
            long_rest_seconds=99.0,
        )
        client._request_count = 0

        with (
            patch("oldiron_core.google_maps.client.random.uniform", return_value=0.1),
            patch("oldiron_core.google_maps.client.time.sleep") as sleep_mock,
        ):
            GoogleMapsClient._sleep(client)

        sleep_mock.assert_called_once_with(0.1)
        self.assertEqual(1, client._request_count)


if __name__ == "__main__":
    unittest.main()
