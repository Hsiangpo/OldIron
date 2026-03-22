# protocol_crawler — 协议爬虫共享模块

## 简介

用 `curl_cffi` 实现的轻量级站点爬虫，提供两个核心能力：

1. **链接发现**（`map_site`）—— 先解析 `sitemap.xml`，没有则从首页提取站内链接
2. **HTML 抓取**（`scrape_html` / `scrape_html_pages`）—— HTTP GET 获取完整 HTML

作为 Firecrawl API 的零成本替代方案。接口与 Firecrawl 客户端对齐，可直接注入现有管线。

## 安装

确保 `curl_cffi` 已安装（通常各国家 `requirements.txt` 已包含）：

```bash
pip install curl_cffi
```

运行时需要 `PYTHONPATH` 包含 `shared/` 目录：

```bash
export PYTHONPATH=/path/to/OldIron/shared:$PYTHONPATH
```

## 使用

```python
from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig

client = SiteCrawlClient(SiteCrawlConfig(
    timeout_seconds=20.0,
    max_retries=2,
    proxy_url="http://127.0.0.1:7897",  # 可选
    impersonate="chrome110",
))

# 发现站点所有页面链接
links = client.map_site("https://example.dk", limit=200)

# 抓取单个页面 HTML
page = client.scrape_html("https://example.dk/about")
print(page.url, len(page.html))

# 批量抓取，自动跳过空/失败页面
pages = client.scrape_html_pages(["https://a.dk/1", "https://a.dk/2"])
```

## 模块结构

```
protocol_crawler/
├── __init__.py          # 导出 SiteCrawlClient, SiteCrawlConfig, HtmlPageResult
├── client.py            # 核心客户端类
├── sitemap.py           # robots.txt → sitemap.xml 解析
├── link_extractor.py    # HTML <a href> 站内链接提取
├── tests/
│   ├── __init__.py
│   └── test_client.py   # 单元测试
└── README.md            # 本文件
```

## 测试

```bash
cd /path/to/OldIron
PYTHONPATH=shared:$PYTHONPATH python -m unittest shared.oldiron_core.protocol_crawler.tests.test_client -v
```

## 在 Denmark 管线中使用

设置环境变量 `CRAWL_BACKEND=protocol`（默认值），管线会自动用 `SiteCrawlClient` 替代 Firecrawl。

如需切回 Firecrawl，设置 `CRAWL_BACKEND=firecrawl` 并提供有效的 Firecrawl keys。
