"""官网首页公司名抽取服务。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from thailand_crawler.streaming.firecrawl_client import FirecrawlClient
from thailand_crawler.streaming.llm_client import SiteNameExtractResult
from thailand_crawler.streaming.llm_client import SiteNameLlmClient


@dataclass(slots=True)
class SiteNameService:
    firecrawl: FirecrawlClient
    llm: SiteNameLlmClient

    @staticmethod
    def ensure_keys_file(target_path: Path, inline_keys: list[str]) -> None:
        cleaned = [str(item).strip() for item in inline_keys if str(item).strip()]
        if not cleaned:
            raise ValueError('Firecrawl keys 为空，请检查根目录 .env 中的 FIRECRAWL_KEYS。')
        unique: list[str] = []
        for item in cleaned:
            if item not in unique:
                unique.append(item)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text('\n'.join(unique) + '\n', encoding='utf-8')

    def extract_homepage_name(self, *, company_name_en: str, website: str) -> SiteNameExtractResult:
        page = self.firecrawl.scrape_page(website)
        markdown = str(page.get('markdown', '') or '').strip()
        raw_html = str(page.get('raw_html', '') or '').strip()
        if not markdown and not raw_html:
            return SiteNameExtractResult(company_name_th='', evidence_url=website, evidence_quote='', confidence=0.0)
        result = self.llm.extract_company_name(
            company_name_en=company_name_en,
            website=website,
            markdown=markdown,
            raw_html=raw_html,
        )
        if not result.evidence_url:
            result.evidence_url = str(page.get('url', website) or website)
        return result
