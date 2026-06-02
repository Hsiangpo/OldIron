from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Protocol

import httpx


@dataclass(frozen=True)
class ActiveRepresentativeSearchResult:
    representative: str = ""
    confidence: str = ""
    evidence_url: str = ""


class ActiveRepresentativeLlm(Protocol):
    def build_active_representative_queries(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> list[str]:
        ...

    def extract_active_representative_from_search_results(
        self,
        *,
        company_name: str,
        website: str,
        results: list[dict[str, str]],
        deadline_monotonic: float | None = None,
    ) -> dict[str, str]:
        ...


class TavilySearchClient:
    def __init__(
        self,
        *,
        api_key: str,
        max_results: int,
        search_depth: str,
        timeout_seconds: float,
        proxy_url: str = "",
    ) -> None:
        self._api_key = str(api_key or "").strip()
        self._max_results = max(int(max_results or 5), 1)
        self._search_depth = str(search_depth or "basic").strip() or "basic"
        kwargs: dict[str, object] = {
            "timeout": timeout_seconds,
            "follow_redirects": True,
            "trust_env": False,
        }
        if proxy_url:
            kwargs["proxy"] = proxy_url
        self._client = httpx.Client(**kwargs)

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def close(self) -> None:
        self._client.close()

    def search(self, query: str) -> list[dict[str, str]]:
        if not self._api_key:
            return []
        response = self._client.post(
            "https://api.tavily.com/search",
            json={
                "api_key": self._api_key,
                "query": query,
                "search_depth": self._search_depth,
                "max_results": self._max_results,
                "include_answer": False,
                "include_raw_content": False,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return [_normalize_tavily_result(item) for item in payload.get("results", [])]


class ActiveRepresentativeSearcher:
    def __init__(
        self,
        *,
        llm_client: ActiveRepresentativeLlm,
        tavily_client,
        enabled: bool,
        max_queries: int = 2,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._llm = llm_client
        self._tavily = tavily_client
        self._enabled = bool(enabled)
        self._max_queries = max(int(max_queries or 1), 1)
        self._executor = executor

    def close(self) -> None:
        close = getattr(self._tavily, "close", None)
        if callable(close):
            close()

    def submit(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> Future:
        if self._executor is None:
            raise RuntimeError("active_representative_searcher_missing_executor")
        return self._executor.submit(
            self.search,
            company_name=company_name,
            website=website,
            deadline_monotonic=deadline_monotonic,
        )

    def search(
        self,
        *,
        company_name: str,
        website: str,
        deadline_monotonic: float | None = None,
    ) -> ActiveRepresentativeSearchResult:
        company = str(company_name or "").strip()
        if not self._enabled or not company:
            return ActiveRepresentativeSearchResult()
        if _deadline_expired(deadline_monotonic):
            return ActiveRepresentativeSearchResult()
        queries = self._llm.build_active_representative_queries(
            company_name=company,
            website=website,
            deadline_monotonic=deadline_monotonic,
        )
        results = self._run_queries(queries[: self._max_queries])
        if not results or _deadline_expired(deadline_monotonic):
            return ActiveRepresentativeSearchResult()
        data = self._llm.extract_active_representative_from_search_results(
            company_name=company,
            website=website,
            results=results,
            deadline_monotonic=deadline_monotonic,
        )
        return ActiveRepresentativeSearchResult(
            representative=str(data.get("representative", "") or "").strip(),
            confidence=str(data.get("confidence", "") or "").strip(),
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
        )

    def _run_queries(self, queries: list[str]) -> list[dict[str, str]]:
        merged: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for query in queries:
            text = str(query or "").strip()
            if not text:
                continue
            for result in self._tavily.search(text):
                url = str(result.get("url", "") or "").strip()
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                merged.append(result)
        return merged


def _normalize_tavily_result(item: object) -> dict[str, str]:
    if not isinstance(item, dict):
        return {"title": "", "url": "", "content": ""}
    return {
        "title": str(item.get("title", "") or "").strip(),
        "url": str(item.get("url", "") or "").strip(),
        "content": str(item.get("content", "") or "").strip(),
    }


def _deadline_expired(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic
