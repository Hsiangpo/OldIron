from __future__ import annotations

from dataclasses import field
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineSettings:
    input_path: Path
    output_base_dir: Path
    run_dir: Path
    concurrency: int
    llm_concurrency: int
    max_pages: int
    max_rounds: int
    max_sites: int | None
    page_timeout: int
    max_content_chars: int
    save_pages: bool
    resume: bool
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_temperature: float
    llm_max_output_tokens: int
    llm_reasoning_effort: str | None
    use_llm: bool = False
    llm_max_pages: int = 4
    crawler_reset_every: int = 0
    site_timeout_seconds: float | None = None
    snov_extension_selector: str | None = None
    snov_extension_token: str | None = None
    snov_extension_fingerprint: str | None = None
    snov_extension_cdp_host: str | None = None
    snov_extension_cdp_port: int | None = None
    snov_extension_only: bool = False
    skip_email: bool = False
    country_code: str | None = None
    required_fields: list[str] = field(
        default_factory=lambda: ["company_name", "email", "representative"]
    )
    keyword: str | None = None
    keyword_filter_enabled: bool = False
    keyword_min_confidence: float = 0.6
    email_max_per_domain: int = 0
    email_details_limit: int = 80
    pdf_max_pages: int = 4
    resume_mode: str | None = None
    firecrawl_keys_path: Path | None = None
    firecrawl_base_url: str | None = None
    firecrawl_extract_enabled: bool = False
    firecrawl_extract_max_urls: int = 6
    firecrawl_key_per_limit: int = 2
    firecrawl_key_wait_seconds: int = 120
    simple_mode: bool = False


@dataclass
class RunStrategy:
    """运行策略配置，根据 resume_mode 区分全量跑与续跑的行为差异。"""

    mode: str  # "full" | "representative" | ...
    allow_llm_link_select: bool  # 是否允许 LLM 选择链接
    allow_llm_keyword_filter: bool  # 是否允许 LLM 关键词过滤
    allow_snov_prefetch: bool  # 是否允许 Snov 预取
    allow_pdf_extract: bool  # 是否允许 PDF 解析
    max_rounds: int  # 最大轮次
    max_pages: int  # 最大页数


def get_strategy_for_mode(
    resume_mode: str | None, settings_max_rounds: int = 3, settings_max_pages: int = 10
) -> RunStrategy:
    """根据 resume_mode 返回对应的运行策略。

    全量跑：可在上层开关控制是否启用 LLM，邮箱走规则 + Snov。
    代表人续跑：可启用 LLM 选链与关键词过滤以提高补全率。
    """
    mode = (resume_mode or "").strip().lower()
    if mode == "representative":
        return RunStrategy(
            mode="representative",
            allow_llm_link_select=True,
            allow_llm_keyword_filter=True,
            allow_snov_prefetch=True,
            allow_pdf_extract=False,  # 当前无实际使用，禁用
            max_rounds=settings_max_rounds,  # 尊重配置值
            max_pages=settings_max_pages,
        )
    # 默认为全量跑（full），或其他未知模式按全量跑处理
    return RunStrategy(
        mode="full",
        allow_llm_link_select=True,
        allow_llm_keyword_filter=False,
        allow_snov_prefetch=True,  # 全量跑启用 Snov 预取
        allow_pdf_extract=False,  # 当前无实际使用，禁用
        max_rounds=settings_max_rounds,  # 尊重配置值
        max_pages=settings_max_pages,
    )
