from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SiteInput:
    website: str
    input_name: str | None = None
    source: str | None = None
    raw: dict[str, Any] | None = None


@dataclass
class LinkItem:
    url: str
    text: str | None = None
    is_nav: bool = False


@dataclass
class PageContent:
    url: str
    markdown: str
    raw_html: str | None = None
    fit_markdown: str | None = None
    title: str | None = None
    links: list[LinkItem] | None = None
    attachments: list[dict[str, Any]] | None = None
    success: bool = True
    error: str | None = None


@dataclass
class ExtractionResult:
    website: str
    input_name: str | None
    company_name: str | None
    representative: str | None
    capital: str | None
    employees: str | None
    email: str | None
    emails: list[str] | None
    email_count: int | None
    phone: str | None
    company_name_source_url: str | None
    representative_source_url: str | None
    capital_source_url: str | None
    employees_source_url: str | None
    email_source_url: str | None
    phone_source_url: str | None
    notes: str | None
    source_urls: list[str]
    status: str
    error: str | None
    extracted_at: str
    raw_llm: dict[str, Any] | None = None
