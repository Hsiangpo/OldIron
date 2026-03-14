from __future__ import annotations

from site_agent.pipeline import _backfill_rep_evidence, _is_rep_evidence_strong
from site_agent.models import PageContent


def test_rep_evidence_strong_by_quote_match() -> None:
    evidence = {"representative": {"url": "https://ex.com/about", "quote": "John Smith"}}
    assert _is_rep_evidence_strong("John Smith", evidence)


def test_rep_evidence_strong_by_title_label() -> None:
    evidence = {
        "representative": {
            "url": "https://ex.com/company",
            "quote": "代表取締役 John Smith",
        }
    }
    assert _is_rep_evidence_strong("John Smith", evidence)


def test_rep_evidence_missing_quote_rejected() -> None:
    evidence = {"representative": {"url": "https://ex.com/company", "quote": None}}
    assert not _is_rep_evidence_strong("John Smith", evidence)


def test_backfill_rep_evidence_from_page_text() -> None:
    visited = {
        "https://example.com/company": PageContent(
            url="https://example.com/company",
            markdown="代表取締役 山田太郎\n所在地 東京都",
            raw_html=None,
            success=True,
        )
    }
    info = {"representative": "山田太郎", "evidence": {}}
    out = _backfill_rep_evidence(info, visited)
    rep_ev = out.get("evidence", {}).get("representative")
    assert rep_ev and rep_ev.get("url") == "https://example.com/company"
    assert "山田太郎" in rep_ev.get("quote", "")
