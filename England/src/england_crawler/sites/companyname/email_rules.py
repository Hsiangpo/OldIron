"""England 规则邮箱提取包装。"""

from __future__ import annotations

from dataclasses import dataclass

from oldiron_core.fc_email.email_service import FirecrawlEmailService


@dataclass(slots=True)
class RuleEmailDiscoveryResult:
    emails: list[str]
    evidence_url: str
    selected_urls: list[str]


class EnglandRuleEmailExtractor:
    """England P3 只用规则抽邮箱，不进入 LLM。"""

    def __init__(self, service: FirecrawlEmailService) -> None:
        self._service = service

    def discover(self, *, company_name: str, homepage: str, domain: str = "") -> RuleEmailDiscoveryResult:
        start_url = self._service._normalize_start_url(homepage, domain)
        if not start_url:
            return RuleEmailDiscoveryResult(emails=[], evidence_url="", selected_urls=[])
        all_urls = self._service._rank_all_urls(start_url, self._service._map_site(start_url))
        limit = max(int(self._service._settings.extract_max_urls), 1)
        shortlist = self._service._build_rule_shortlist(
            start_url=start_url,
            all_urls=all_urls,
            limit=max(int(self._service._settings.prefilter_limit), limit),
        )
        final_urls = self._service._build_final_urls(start_url, [], shortlist, limit=limit)
        pages = self._service._scrape_html_pages(final_urls)
        emails = self._service._extract_rule_emails(start_url, pages)
        return RuleEmailDiscoveryResult(
            emails=emails,
            evidence_url=final_urls[0] if final_urls else start_url,
            selected_urls=final_urls,
        )
