from __future__ import annotations

from web_agent.service import _status_from_fields_simple


def test_status_from_fields_simple_ignores_missing_rep_text() -> None:
    required = ["company_name", "email", "representative"]
    missing_rep = "\u672a\u627e\u5230\u4ee3\u8868\u4eba"
    assert (
        _status_from_fields_simple("Company", missing_rep, "a@b.com", required)
        == "partial"
    )
