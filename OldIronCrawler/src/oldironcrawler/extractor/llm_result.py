from __future__ import annotations

from oldironcrawler.extractor.llm_client import LlmExtractionResult, WebsiteLlmClient
from oldironcrawler.extractor.value_rules import canonicalize_target_url


def extract_with_llm_or_empty(
    *,
    llm_client: WebsiteLlmClient,
    homepage: str,
    rep_pages: list,
    deadline_monotonic: float | None,
) -> LlmExtractionResult:
    if not rep_pages:
        return LlmExtractionResult(company_name="", representative="", evidence_url="", evidence_quote="")
    return llm_client.extract_company_and_representative(
        homepage=homepage,
        pages=[{"url": page.url, "html": page.html} for page in rep_pages],
        deadline_monotonic=deadline_monotonic,
    )


def normalize_llm_result(llm_result: LlmExtractionResult, rep_pages: list) -> LlmExtractionResult:
    available_urls = {
        canonicalize_target_url(page.url): page.url
        for page in rep_pages
        if str(page.url or "").strip()
    }
    raw_evidence_url = str(llm_result.evidence_url or "").strip()
    evidence_url = available_urls.get(canonicalize_target_url(raw_evidence_url), "")
    representative = str(llm_result.representative or "").strip() if evidence_url else ""
    evidence_quote = str(llm_result.evidence_quote or "").strip() if representative else ""
    return LlmExtractionResult(
        company_name=str(llm_result.company_name or "").strip() if rep_pages else "",
        representative=representative,
        evidence_url=evidence_url,
        evidence_quote=evidence_quote,
    )
