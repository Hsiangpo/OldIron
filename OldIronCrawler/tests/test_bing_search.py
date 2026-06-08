from __future__ import annotations

from types import SimpleNamespace

from oldironcrawler import runner as runner_module
from oldironcrawler.extractor.bing_search import BingSearchClient, parse_bing_results


_SAMPLE_HTML = """
<html><body><ol id="b_results">
<li class="b_algo">
  <h2><a href="https://en.wikipedia.org/wiki/Tim_Cook">Tim Cook - Wikipedia</a></h2>
  <div class="b_caption"><p>Timothy Donald Cook is the chief executive officer (CEO) of Apple.</p></div>
</li>
<li class="b_algo">
  <h2><a href="https://www.apple.com/leadership/">Apple Leadership - Apple</a></h2>
  <div class="b_caption"><p>Tim Cook leads Apple as its CEO.</p></div>
</li>
</ol></body></html>
"""


def test_parse_bing_results_extracts_title_url_content() -> None:
    rows = parse_bing_results(_SAMPLE_HTML, max_results=5)
    assert len(rows) == 2
    assert rows[0]["title"] == "Tim Cook - Wikipedia"
    assert rows[0]["url"] == "https://en.wikipedia.org/wiki/Tim_Cook"
    assert "CEO" in rows[0]["content"]


def test_parse_bing_results_respects_max_results() -> None:
    assert len(parse_bing_results(_SAMPLE_HTML, max_results=1)) == 1


def test_parse_bing_results_handles_empty_or_captcha() -> None:
    assert parse_bing_results("", 5) == []
    assert parse_bing_results("<html><body>captcha challenge</body></html>", 5) == []


def _search_config(**overrides) -> SimpleNamespace:
    base = dict(
        search_backend="bing",
        tavily_max_results=5,
        tavily_timeout_seconds=20.0,
        tavily_search_depth="basic",
        tavily_api_key="",
        proxy_url="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_build_search_client_defaults_to_bing() -> None:
    client = runner_module._build_search_client(_search_config())
    assert isinstance(client, BingSearchClient)


def test_build_search_client_tavily_requires_key() -> None:
    assert runner_module._build_search_client(_search_config(search_backend="tavily")) is None
    client = runner_module._build_search_client(_search_config(search_backend="tavily", tavily_api_key="tvly-x"))
    assert client is not None and not isinstance(client, BingSearchClient)
