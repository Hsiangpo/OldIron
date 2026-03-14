from __future__ import annotations

from site_agent.models import PageContent
from site_agent.pipeline import _apply_heuristic_extraction


def test_heuristic_extract_representative_from_table() -> None:
    html = """
    <table>
      <tr><th>代表取締役</th><td>山田 太郎</td></tr>
    </table>
    """
    visited = {
        "https://example.com/company": PageContent(
            url="https://example.com/company",
            markdown="",
            raw_html=html,
            success=True,
        )
    }
    info = _apply_heuristic_extraction({}, visited, required_fields=["representative"])
    assert info.get("representative") == "山田 太郎"


def test_heuristic_extract_representative_from_multiline_label() -> None:
    markdown = "会社概要\n代表取締役\n山田 太郎\n"
    visited = {
        "https://example.com/company": PageContent(
            url="https://example.com/company",
            markdown=markdown,
            raw_html="",
            success=True,
        )
    }
    info = _apply_heuristic_extraction({}, visited, required_fields=["representative"])
    assert info.get("representative") == "山田 太郎"


def test_heuristic_representative_ignores_non_name_candidate() -> None:
    markdown = "会社概要\n代表取締役\nお問い合わせ窓口\n"
    visited = {
        "https://example.com/company": PageContent(
            url="https://example.com/company",
            markdown=markdown,
            raw_html="",
            success=True,
        )
    }
    info = _apply_heuristic_extraction({}, visited, required_fields=["representative"])
    assert info.get("representative") is None
