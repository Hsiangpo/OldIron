from malaysia_crawler.manager_agent.llm_client import ManagerLlmClient


class _DummyResponse:
    def __init__(self, output_text: str) -> None:
        self.output_text = output_text


class _DummyResponses:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> _DummyResponse:
        self.calls.append(kwargs)
        if len(self.calls) == 1 and "response_format" in kwargs:
            raise TypeError("Responses.create() got an unexpected keyword argument 'response_format'")
        return _DummyResponse('{"selected_urls":["https://example.com/about"]}')


class _DummyClient:
    def __init__(self) -> None:
        self.responses = _DummyResponses()


def test_call_json_fallback_when_response_format_not_supported() -> None:
    client = ManagerLlmClient(
        api_key="test",
        base_url="",
        model="dummy-model",
        reasoning_effort="high",
        timeout_seconds=30.0,
    )
    dummy = _DummyClient()
    client._client = dummy  # type: ignore[attr-defined]

    payload = client._call_json("test prompt")

    assert payload == {"selected_urls": ["https://example.com/about"]}
    assert len(dummy.responses.calls) == 2
    assert "response_format" in dummy.responses.calls[0]
    assert "response_format" not in dummy.responses.calls[1]


class _DummyListInputResponses:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> _DummyResponse:
        self.calls.append(kwargs)
        if not isinstance(kwargs.get("input"), list):
            raise ValueError("Input must be a list")
        return _DummyResponse('{"selected_urls":["https://example.com/team"]}')


class _DummyListInputClient:
    def __init__(self) -> None:
        self.responses = _DummyListInputResponses()


def test_call_json_fallback_when_gateway_requires_list_input() -> None:
    client = ManagerLlmClient(
        api_key="test",
        base_url="",
        model="dummy-model",
        reasoning_effort="high",
        timeout_seconds=30.0,
    )
    dummy = _DummyListInputClient()
    client._client = dummy  # type: ignore[attr-defined]

    payload = client._call_json("test prompt")

    assert payload == {"selected_urls": ["https://example.com/team"]}
    assert len(dummy.responses.calls) >= 2
    assert isinstance(dummy.responses.calls[-1].get("input"), list)
