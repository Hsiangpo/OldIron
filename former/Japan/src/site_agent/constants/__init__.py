"""集中管理的常量配置。

将散落在代码各处的魔法数字和硬编码值集中管理。
"""
from __future__ import annotations


class Limits:
    """数量限制相关常量。"""
    # 邮箱相关
    MAX_EMAIL_CANDIDATES_FROM_PAGE = 20
    MAX_EMAIL_CANDIDATES_FROM_BING = 5
    MAX_EMAIL_SNIPPET_WINDOW = 40
    
    # 页面相关
    MAX_FALLBACK_URLS = 16
    MAX_HEURISTIC_SELECT = 10
    MAX_LLM_SELECT_PER_ROUND = 5
    
    # LLM 相关
    MAX_VISION_IMAGES = 4
    MAX_CONTENT_CHARS_DEFAULT = 20000
    MAX_LLM_ATTEMPTS = 3
    
    # 链接相关
    MAX_LINKS_FOR_PROMPT = 200
    MAX_SITEMAP_URLS = 100
    
    # PDF 相关
    MAX_PDF_PAGES_DEFAULT = 4


class Timeouts:
    """超时配置（秒）。"""
    PAGE_TIMEOUT_DEFAULT = 60000  # 毫秒
    LLM_TIMEOUT_DEFAULT = 90.0
    LLM_RETRY_MIN_SECONDS = 5.0
    LLM_RETRY_MAX_SECONDS = 20.0
    SNOV_REQUEST_TIMEOUT = 30.0


class Concurrency:
    """并发配置。"""
    SITE_CONCURRENCY_DEFAULT = 16
    LLM_CONCURRENCY_DEFAULT = 16
    GMAP_CONCURRENCY_DEFAULT = 16


class Patterns:
    """正则表达式模式。"""
    # 日本人名中的连接符
    PERSON_NAME_SEPARATORS = r"[・･·•\s]"
    
    # 公司法人类型关键词
    CORP_KEYWORDS = (
        "株式会社",
        "有限会社",
        "合同会社",
        "合名会社",
        "合資会社",
        "持株会社",
        "社団法人",
        "財団法人",
        "一般社団法人",
        "一般財団法人",
        "医療法人",
        "学校法人",
        "宗教法人",
        "特定非営利活動法人",
        "有限責任",
        "組合",
        "事業協同組合",
        "農業協同組合",
        "漁業協同組合",
        "生活協同組合",
        "共同組合",
    )
    
    # 代表人职位关键词（仅保留接近法人代表的词，避免误报）
    REPRESENTATIVE_TITLES = (
        # 日本常见职位（按长度从长到短排序，防止部分匹配）
        "代表取締役社長",
        "代表取締役会長",
        "代表取締役",
        "代表社員",
        "代表理事",
        "取締役社長",
        "取締役会長",
        "取締役",
        "社長",
        "会長",
        "院長",
        "理事長",
        "校長",
        "所長",
        "執行役員",
        # 英文职位（仅保留核心）
        "CEO",
        "President",
        "Chairman",
        "Managing Director",
        # 中文职位
        "法人代表",
        "法定代表人",
        "董事长",
        "总经理",
        "总裁",
        # 通用
        "代表者",
        "代表人",
        "代表",
    )


class FieldNames:
    """字段名称映射。"""
    ZH_NAMES = {
        "company_name": "公司名称",
        "representative": "代表人",
        "email": "邮箱",
        "phone": "座机",
        "capital": "注册资金",
        "employees": "公司人数",
    }
    
    @classmethod
    def to_zh(cls, field: str) -> str:
        """将字段名转换为中文。"""
        return cls.ZH_NAMES.get(field, field)
    
    @classmethod
    def format_fields_zh(cls, fields: list[str] | None) -> str:
        """格式化多个字段为中文。"""
        if not (isinstance(fields, list) and fields):
            return "公司名称、代表人"
        cleaned = [f for f in fields if isinstance(f, str) and f.strip()]
        if not cleaned:
            return "公司名称、代表人"
        return "、".join(cls.to_zh(f) for f in cleaned)


class CompanyOverviewKeywords:
    """公司信息页面关键词。"""
    URL_KEYWORDS = (
        "about",
        "company",
        "corporate",
        "profile",
        "gaiyou",
        "kaisya",
        "会社案内",
        "会社概要",
        "企業情報",
        "企業概要",
        "会社情報",
        "経営理念",
        "代表挨拶",
        "代表メッセージ",
        "ご挨拶",
        "沿革",
        "概要",
    )
    
    LINK_TEXT_KEYWORDS = (
        "会社概要",
        "会社案内",
        "企業情報",
        "企業概要",
        "会社情報",
        "about",
        "company",
        "corporate",
        "profile",
    )
