from __future__ import annotations

from thailand_crawler.snov import SnovClient
from thailand_crawler.snov import SnovCredential
from thailand_crawler.snov import SnovConfig
from thailand_crawler.snov import load_snov_credentials_from_env
from thailand_crawler.snov import SnovRateLimitError
from thailand_crawler.snov import is_excluded_company_domain


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, post_responses: list[FakeResponse], get_responses: list[FakeResponse]) -> None:
        self.post_responses = post_responses
        self.get_responses = get_responses
        self.post_calls: list[tuple[str, dict]] = []
        self.get_calls: list[tuple[str, dict]] = []

    def post(self, url: str, data: dict, timeout: float):
        self.post_calls.append((url, dict(data)))
        return self.post_responses.pop(0)

    def get(self, url: str, params: dict, timeout: float):
        self.get_calls.append((url, dict(params)))
        return self.get_responses.pop(0)


def test_snov_client_refreshes_token_after_401() -> None:
    client = SnovClient(SnovConfig(client_id="id", client_secret="secret", timeout=1, retry_delay=0, max_retries=3))
    client.session = FakeSession(
        post_responses=[
            FakeResponse(200, {"access_token": "token-1"}),
            FakeResponse(401, {}),
            FakeResponse(200, {"access_token": "token-2"}),
            FakeResponse(200, {"links": {"result": "https://result.example.com"}}),
        ],
        get_responses=[FakeResponse(200, {"data": [{"email": "boss@example.com"}]})],
    )

    emails = client.get_domain_emails("example.com")

    assert emails == ["boss@example.com"]
    assert client._access_tokens["id"] == "token-2"


def test_snov_client_raises_rate_limit_after_continuous_429() -> None:
    client = SnovClient(SnovConfig(client_id="id", client_secret="secret", timeout=1, retry_delay=0, max_retries=3))
    client.session = FakeSession(
        post_responses=[
            FakeResponse(429, {}),
            FakeResponse(429, {}),
            FakeResponse(429, {}),
        ],
        get_responses=[],
    )

    try:
        client.get_domain_emails("example.com")
    except SnovRateLimitError:
        pass
    else:
        raise AssertionError("应抛出 SnovRateLimitError")


def test_excluded_company_domains_cover_public_portals() -> None:
    assert is_excluded_company_domain('kkmuni.go.th') is True
    assert is_excluded_company_domain('minisite.airports.go.th') is True
    assert is_excluded_company_domain('booking.com') is True
    assert is_excluded_company_domain('sutheethaiconstruction.wordpress.com') is True
    assert is_excluded_company_domain('traveloka.com') is True
    assert is_excluded_company_domain('trip.com') is True
    assert is_excluded_company_domain('kr.bluepillow.com') is True
    assert is_excluded_company_domain('laterooms.com') is True
    assert is_excluded_company_domain('trivago.co.kr') is True
    assert is_excluded_company_domain('fb.me') is True
    assert is_excluded_company_domain('centarahotelsresorts.com') is True
    assert is_excluded_company_domain('expedia.com') is True
    assert is_excluded_company_domain('agoda.com') is True
    assert is_excluded_company_domain('bit.ly') is True
    assert is_excluded_company_domain('example.com') is False


def test_snov_client_skips_excluded_company_domain() -> None:
    client = SnovClient(SnovConfig(client_id='id', client_secret='secret', timeout=1, retry_delay=0, max_retries=3))
    client.session = FakeSession(post_responses=[], get_responses=[])

    assert client.get_domain_emails('kkmuni.go.th') == []
    assert client.session.post_calls == []
    assert client.session.get_calls == []


def test_snov_client_switches_to_secondary_key_when_primary_has_no_credits() -> None:
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
    client.session = FakeSession(
        post_responses=[
            FakeResponse(200, {"access_token": "token-1"}),
            FakeResponse(200, {"status": "not_enough_credits"}),
            FakeResponse(200, {"access_token": "token-2"}),
            FakeResponse(200, {"links": {"result": "https://result.example.com"}}),
        ],
        get_responses=[FakeResponse(200, {"data": [{"email": "boss@example.com"}]})],
    )

    emails = client.get_domain_emails("example.com")

    assert emails == ["boss@example.com"]
    assert client._access_tokens["backup-id"] == "token-2"
    assert any(call[1].get("client_id") == "backup-id" for call in client.session.post_calls)


def test_load_snov_credentials_from_env_reads_numbered_backup_keys(monkeypatch) -> None:
    monkeypatch.setenv("SNOV_CLIENT_ID", "primary-id")
    monkeypatch.setenv("SNOV_CLIENT_SECRET", "primary-secret")
    monkeypatch.setenv("SNOV_CLIENT_ID_2", "backup-id")
    monkeypatch.setenv("SNOV_CLIENT_SECRET_2", "backup-secret")

    credentials = load_snov_credentials_from_env()

    assert credentials == (
        SnovCredential("primary-id", "primary-secret"),
        SnovCredential("backup-id", "backup-secret"),
    )
