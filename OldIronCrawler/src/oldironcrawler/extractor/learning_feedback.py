from __future__ import annotations

from dataclasses import dataclass

from oldironcrawler.extractor.value_rules import extract_learning_tokens


@dataclass
class LearningFeedback:
    rep_positive_tokens: list[str]
    rep_negative_tokens: list[str]
    email_positive_tokens: list[str]
    email_negative_tokens: list[str]


def build_learning_feedback(
    *,
    representative: str,
    evidence_url: str,
    rep_urls: list[str],
    rep_fetched_urls: list[str],
    emails: str,
    email_sources: list[str],
    email_urls: list[str],
    email_fetched_urls: list[str],
) -> LearningFeedback:
    rep_positive_tokens = _collect_positive_rep_tokens(representative, evidence_url, rep_fetched_urls)
    rep_negative_tokens = _collect_failed_rep_negative_tokens(
        representative,
        evidence_url,
        rep_positive_tokens,
        rep_fetched_urls,
    )
    email_positive_tokens = _collect_positive_email_tokens(emails, email_sources, email_fetched_urls)
    email_negative_tokens = _collect_failed_email_negative_tokens(
        emails,
        email_positive_tokens,
        email_fetched_urls,
    )
    return LearningFeedback(
        rep_positive_tokens=rep_positive_tokens,
        rep_negative_tokens=rep_negative_tokens,
        email_positive_tokens=email_positive_tokens,
        email_negative_tokens=email_negative_tokens,
    )


def _merge_learning_tokens(urls: list[str]) -> list[str]:
    tokens: list[str] = []
    for url in urls:
        for token in extract_learning_tokens(url):
            if token not in tokens:
                tokens.append(token)
    return tokens


def _collect_failed_rep_negative_tokens(
    representative: str,
    evidence_url: str,
    positive_tokens: list[str],
    rep_fetched_urls: list[str],
) -> list[str]:
    return []


def _collect_failed_email_negative_tokens(
    emails: str,
    positive_tokens: list[str],
    email_fetched_urls: list[str],
) -> list[str]:
    return []


def _collect_positive_rep_tokens(representative: str, evidence_url: str, rep_fetched_urls: list[str]) -> list[str]:
    if not representative or not evidence_url:
        return []
    if evidence_url not in rep_fetched_urls:
        return []
    return extract_learning_tokens(evidence_url)


def _collect_positive_email_tokens(emails: str, email_sources: list[str], email_fetched_urls: list[str]) -> list[str]:
    if not emails:
        return []
    kept_sources = [url for url in email_sources if url in email_fetched_urls]
    return _merge_learning_tokens(kept_sources)
