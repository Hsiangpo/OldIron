# Protocol Crawler（协议爬虫）

## 简介

协议爬虫共享模块，用 `curl_cffi` 实现站点链接发现和 HTML 页面抓取，作为 Firecrawl 的零成本替代方案。

**代码位置**：`shared/oldiron_core/protocol_crawler/`

本目录仅作为文档指引。协议爬虫是 Python 模块（核心依赖 `curl_cffi`），按照项目架构原则放在 Python 共享目录 `shared/oldiron_core/` 下，而非 Go 后端目录。

## 功能

| 方法 | 用途 |
|------|------|
| `map_site(url, limit=200)` | 发现站点链接：先尝试 sitemap.xml，没有则从首页 HTML 提取 |
| `scrape_html(url)` | 抓取单个页面的完整 HTML |
| `scrape_html_pages(urls)` | 批量抓取多个页面，跳过失败/空页面 |

## 快速使用

```python
from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig

client = SiteCrawlClient(SiteCrawlConfig(
    timeout_seconds=20.0,
    proxy_url="http://127.0.0.1:7897",
))

links = client.map_site("https://example.dk")
page = client.scrape_html("https://example.dk/about")
```

## 在 Denmark 管线中启用

在 `Denmark/.env` 中设置（默认已启用）：

```
CRAWL_BACKEND=protocol
```

如果以后补充了 Firecrawl API key，可以切回：

```
CRAWL_BACKEND=firecrawl
```
