import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, post_responses: list[_FakeResponse], get_responses: list[_FakeResponse]) -> None:
        self.post_responses = post_responses
        self.get_responses = get_responses
        self.post_calls: list[tuple[str, dict]] = []
        self.get_calls: list[tuple[str, dict]] = []
        self.trust_env = False

    def post(self, url: str, data: dict, timeout: float):
        self.post_calls.append((url, dict(data)))
        return self.post_responses.pop(0)

    def get(self, url: str, params: dict, timeout: float):
        self.get_calls.append((url, dict(params)))
        return self.get_responses.pop(0)


class SnovClientTests(unittest.TestCase):
    def test_switches_to_secondary_key_when_primary_has_no_credits(self) -> None:
        from england_crawler.snov.client import SnovClient
        from england_crawler.snov.client import SnovConfig
        from england_crawler.snov.client import SnovCredential

        client = SnovClient(
            SnovConfig(
                timeout=1,
                retry_delay=0,
                max_retries=1,
                credentials=(
                    SnovCredential("primary-id", "primary-secret"),
                    SnovCredential("backup-id", "backup-secret"),
                ),
            )
        )
        client.session = _FakeSession(
            post_responses=[
                _FakeResponse(200, {"access_token": "token-1"}),
                _FakeResponse(200, {"status": "not_enough_credits"}),
                _FakeResponse(200, {"access_token": "token-2"}),
                _FakeResponse(200, {"links": {"result": "https://result.example.com"}}),
            ],
            get_responses=[_FakeResponse(200, {"data": [{"email": "boss@example.com"}]})],
        )

        emails = client.get_domain_emails("example.com")

        self.assertEqual(["boss@example.com"], emails)
        self.assertEqual("token-2", client._access_tokens["backup-id"])

    def test_load_credentials_reads_numbered_backup_keys(self) -> None:
        from england_crawler.snov.client import SnovCredential
        from england_crawler.snov.client import load_snov_credentials_from_env

        with patch.dict(
            os.environ,
            {
                "SNOV_CLIENT_ID": "primary-id",
                "SNOV_CLIENT_SECRET": "primary-secret",
                "SNOV_CLIENT_ID_2": "backup-id",
                "SNOV_CLIENT_SECRET_2": "backup-secret",
            },
            clear=False,
        ):
            credentials = load_snov_credentials_from_env()

        self.assertEqual(
            (
                SnovCredential("primary-id", "primary-secret"),
                SnovCredential("backup-id", "backup-secret"),
            ),
            credentials,
        )

    def test_masked_only_emails_are_treated_as_no_credit(self) -> None:
        from england_crawler.snov.client import SnovClient
        from england_crawler.snov.client import SnovConfig
        from england_crawler.snov.client import SnovCredential
        from england_crawler.snov.client import SnovNoCreditError

        client = SnovClient(
            SnovConfig(
                timeout=1,
                retry_delay=0,
                max_retries=1,
                credentials=(SnovCredential("primary-id", "primary-secret"),),
            )
        )
        client.session = _FakeSession(
            post_responses=[
                _FakeResponse(200, {"access_token": "token-1"}),
                _FakeResponse(200, {"links": {"result": "https://result.example.com"}}),
            ],
            get_responses=[_FakeResponse(200, {"data": [{"email": "118********@qq.com"}]})],
        )

        with self.assertRaises(SnovNoCreditError):
            client.get_domain_emails("example.com")

    def test_mixed_masked_and_real_emails_only_returns_real_emails(self) -> None:
        from england_crawler.snov.client import SnovClient
        from england_crawler.snov.client import SnovConfig
        from england_crawler.snov.client import SnovCredential

        client = SnovClient(
            SnovConfig(
                timeout=1,
                retry_delay=0,
                max_retries=1,
                credentials=(SnovCredential("primary-id", "primary-secret"),),
            )
        )
        client.session = _FakeSession(
            post_responses=[
                _FakeResponse(200, {"access_token": "token-1"}),
                _FakeResponse(200, {"links": {"result": "https://result.example.com"}}),
            ],
            get_responses=[
                _FakeResponse(
                    200,
                    {
                        "data": [
                            {"email": "118********@qq.com"},
                            {"email": "boss@example.com"},
                        ]
                    },
                )
            ],
        )

        emails = client.get_domain_emails("example.com")

        self.assertEqual(["boss@example.com"], emails)


if __name__ == "__main__":
    unittest.main()
