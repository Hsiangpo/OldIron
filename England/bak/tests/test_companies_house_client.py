import sys
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


SEARCH_HTML = textwrap.dedent(
    """
    <html>
      <body>
        <ul id="results" class="results-list">
          <li class="type-company">
            <h3><a class="govuk-link" href="/company/00000001">ZZZ DEVELOPMENTS LTD</a></h3>
            <p class="meta crumbtrail">00000001 - Dissolved on 20 April 2024</p>
            <p>Old Street, London, United Kingdom</p>
          </li>
          <li class="type-company">
            <h3><a class="govuk-link" href="/company/00000002">ZZZ DEVELOPMENTS LTD.</a></h3>
            <p class="meta crumbtrail">00000002 - Incorporated on 20 April 2020</p>
            <p>New Street, Manchester, United Kingdom</p>
          </li>
          <li class="type-company">
            <h3><a class="govuk-link" href="/company/00000003">ZZZ DEVELOPMENTS GROUP LTD</a></h3>
            <p class="meta crumbtrail">00000003 - Incorporated on 20 April 2021</p>
            <p>Liverpool, United Kingdom</p>
          </li>
        </ul>
      </body>
    </html>
    """
).strip()

SEARCH_HTML_WITH_PREVIOUS_NAMES = textwrap.dedent(
    """
    <html>
      <body>
        <ul id="results" class="results-list">
          <li class="type-company">
            <h3><a class="govuk-link" href="/company/04524087">TREASURE SOLUTIONS LIMITED</a></h3>
            <p class="meta crumbtrail">Matching previous names: TTS SOLUTIONS LIMITED · TREASURE TRANSPORT SERVICES LIMITED 04524087 - Incorporated on 2 September 2002</p>
            <p>London, United Kingdom</p>
          </li>
        </ul>
      </body>
    </html>
    """
).strip()

OFFICERS_HTML = textwrap.dedent(
    """
    <html>
      <body>
        <div class="appointment-1">
          <h2><span id="officer-name-1"><a class="govuk-link">OLD, Alice</a></span></h2>
          <div class="grid-row">
            <dl class="column-quarter">
              <dt>Role <span id="officer-status-tag-1" class="status-tag font-xsmall">Resigned</span></dt>
              <dd id="officer-role-1" class="data">Director</dd>
            </dl>
          </div>
        </div>
        <div class="appointment-2">
          <h2><span id="officer-name-2"><a class="govuk-link">CHARRO, Jorge Manrique</a></span></h2>
          <div class="grid-row">
            <dl class="column-quarter">
              <dt>Role <span id="officer-status-tag-2" class="status-tag font-xsmall">Active</span></dt>
              <dd id="officer-role-2" class="data">Director</dd>
            </dl>
          </div>
        </div>
        <div class="appointment-3">
          <h2><span id="officer-name-3"><a class="govuk-link">SECRETARY, Bob</a></span></h2>
          <div class="grid-row">
            <dl class="column-quarter">
              <dt>Role <span id="officer-status-tag-3" class="status-tag font-xsmall">Active</span></dt>
              <dd id="officer-role-3" class="data">Secretary</dd>
            </dl>
          </div>
        </div>
      </body>
    </html>
    """
).strip()


class CompaniesHouseClientTests(unittest.TestCase):
    def test_get_text_retries_after_403(self) -> None:
        from england_crawler.companies_house.client import CompaniesHouseClient

        class _FakeResponse:
            def __init__(self, status_code: int, text: str) -> None:
                self.status_code = status_code
                self.text = text

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP Error {self.status_code}:")

        class _FakeSession:
            def __init__(self, first_status: int = 403) -> None:
                self.calls = 0
                self.first_status = first_status

            def get(self, url: str, headers: dict, timeout: float):
                self.calls += 1
                if self.calls == 1 and self.first_status == 403:
                    return _FakeResponse(403, "")
                return _FakeResponse(200, SEARCH_HTML)

            def close(self) -> None:
                return None

        client = CompaniesHouseClient(timeout=1, max_retries=2)
        first = _FakeSession(403)
        second = _FakeSession(200)
        client.session = first
        with (
            patch("england_crawler.companies_house.client.time.sleep", return_value=None),
            patch.object(client, "_build_session", return_value=second),
        ):
            text = client._get_text("/search/companies?q=ZZZ")

        self.assertIn("ZZZ DEVELOPMENTS LTD", text)

    def test_proxy_session_rotates_after_403(self) -> None:
        from england_crawler.companies_house.client import CompaniesHouseClient
        from england_crawler.companies_house.proxy import BlurpathProxyConfig

        class _FakeResponse:
            def __init__(self, status_code: int, text: str) -> None:
                self.status_code = status_code
                self.text = text

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP Error {self.status_code}:")

        class _FakeSession:
            def __init__(self, responses):
                self.responses = responses
                self.proxies = {}

            def get(self, url: str, headers: dict, timeout: float):
                return self.responses.pop(0)

            def close(self) -> None:
                return None

        proxy = BlurpathProxyConfig(
            enabled=True,
            host="blurpath.net",
            port=15138,
            username="kytqhwcsfml",
            password="secret",
            region="GB",
            sticky_minutes=10,
        )
        client = CompaniesHouseClient(timeout=1, max_retries=2, proxy_config=proxy, worker_label="CH-1")
        old_session = client.session_id
        first = _FakeSession([_FakeResponse(403, "")])
        second = _FakeSession([_FakeResponse(200, SEARCH_HTML)])
        created = [first, second]
        client.session = first

        with (
            patch("england_crawler.companies_house.client.time.sleep", return_value=None),
            patch.object(client, "_build_session", side_effect=lambda: created.pop(1)),
        ):
            text = client._get_text("/search/companies?q=ZZZ")

        self.assertIn("ZZZ DEVELOPMENTS LTD", text)
        self.assertNotEqual(old_session, client.session_id)

    def test_parse_search_results_extracts_candidates(self) -> None:
        from england_crawler.companies_house.client import parse_search_results

        results = parse_search_results(SEARCH_HTML)

        self.assertEqual(3, len(results))
        self.assertEqual("ZZZ DEVELOPMENTS LTD", results[0].company_name)
        self.assertEqual("00000001", results[0].company_number)
        self.assertEqual("/company/00000001", results[0].detail_path)
        self.assertIn("Dissolved", results[0].status_text)

    def test_parse_search_results_uses_href_company_number_when_previous_names_exist(self) -> None:
        from england_crawler.companies_house.client import parse_search_results

        results = parse_search_results(SEARCH_HTML_WITH_PREVIOUS_NAMES)

        self.assertEqual(1, len(results))
        self.assertEqual("04524087", results[0].company_number)
        self.assertTrue(results[0].status_text.startswith("Matching previous names:"))

    def test_select_best_candidate_prefers_active_exact_match(self) -> None:
        from england_crawler.companies_house.client import parse_search_results
        from england_crawler.companies_house.client import select_best_candidate

        results = parse_search_results(SEARCH_HTML)
        picked = select_best_candidate("ZZZ DEVELOPMENTS LTD", results)

        self.assertIsNotNone(picked)
        self.assertEqual("00000002", picked.company_number)
        self.assertEqual("ZZZ DEVELOPMENTS LTD.", picked.company_name)

    def test_parse_first_active_director_returns_first_current_director(self) -> None:
        from england_crawler.companies_house.client import parse_first_active_director

        director = parse_first_active_director(OFFICERS_HTML)

        self.assertEqual("CHARRO, Jorge Manrique", director)


if __name__ == "__main__":
    unittest.main()
