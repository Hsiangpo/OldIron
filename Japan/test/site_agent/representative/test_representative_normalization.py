from __future__ import annotations

from site_agent.pipeline import _clean_representative_name, _is_rep_evidence_strong


def test_clean_representative_name_normalizes_kanji_variants() -> None:
    raw = "代表取締役　外﨑　善久"
    cleaned = _clean_representative_name(raw)
    assert cleaned is not None
    assert "外" in cleaned and "善久" in cleaned


def test_rep_evidence_strong_ignores_spacing() -> None:
    evidence = {
        "representative": {
            "url": "https://ex.com/company",
            "quote": "代表取締役　山田太郎",
        }
    }
    assert _is_rep_evidence_strong("山田 太郎", evidence)


def test_clean_representative_rejects_greeting_only() -> None:
    assert _clean_representative_name("挨拶") is None
    assert _clean_representative_name("代表挨拶") is None
    assert _clean_representative_name("社長メッセージ") is None


def test_clean_representative_strips_greeting_suffix() -> None:
    raw = "代表取締役社長 田中太郎 メッセージ"
    cleaned = _clean_representative_name(raw)
    assert cleaned == "田中太郎"
