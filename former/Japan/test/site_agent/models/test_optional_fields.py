from site_agent.models import PageContent
from site_agent.pipeline import _apply_heuristic_extraction


def test_extract_capital_and_employees_from_company_table() -> None:
    html = """
    <table>
      <tr><th>資本金</th><td>1,000万円</td></tr>
      <tr><th>従業員数</th><td>25名</td></tr>
    </table>
    """
    page = PageContent(
        url="https://example.co.jp/company",
        markdown="",
        raw_html=html,
        fit_markdown="",
        title="会社概要",
        links=[],
    )
    info = _apply_heuristic_extraction({}, {page.url: page}, required_fields=["company_name", "representative", "email"])
    assert info.get("capital") == "1,000万円"
    assert info.get("employees") == "25名"

    evidence = info.get("evidence")
    assert isinstance(evidence, dict)
    assert evidence.get("capital", {}).get("url") == page.url
    assert "1,000万円" in (evidence.get("capital", {}).get("quote") or "")
    assert evidence.get("employees", {}).get("url") == page.url
    assert "25名" in (evidence.get("employees", {}).get("quote") or "")

