from __future__ import annotations

from thailand_crawler.domain_quality import assess_company_domain
from thailand_crawler.domain_quality import is_excluded_company_domain


def test_excluded_company_domain_blocks_known_platforms() -> None:
    assert is_excluded_company_domain('fb.me') is True
    assert is_excluded_company_domain('kkmuni.go.th') is True
    assert is_excluded_company_domain('centarahotelsresorts.com') is True
    assert is_excluded_company_domain('example.com') is False


def test_assess_company_domain_accepts_brand_matched_shared_domain() -> None:
    result = assess_company_domain(
        company_name='RITTA COMPANY LIMITED',
        domain='ritta.co.th',
        shared_count=248,
    )

    assert result.blocked is False
    assert result.reason == ''
    assert result.match_score > 0


def test_assess_company_domain_blocks_unrelated_shared_domain() -> None:
    result = assess_company_domain(
        company_name='THUMBS UP COMPANY LIMITED',
        domain='kkmuni.go.th',
        shared_count=314,
    )

    assert result.blocked is True
    assert result.reason == 'excluded_domain'


def test_assess_company_domain_blocks_unknown_shared_unrelated_domain() -> None:
    result = assess_company_domain(
        company_name='BLUESKY ROYAL COMPANY LIMITED',
        domain='mysterybrand.co.th',
        shared_count=15,
    )

    assert result.blocked is False
    assert result.reason == 'shared_unrelated_domain'
    assert result.suspicious is True
