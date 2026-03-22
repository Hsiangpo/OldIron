"""protocol_crawler 单元测试。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

from oldiron_core.protocol_crawler.client import SiteCrawlClient, SiteCrawlConfig, HtmlPageResult
from oldiron_core.protocol_crawler.sitemap import discover_sitemap_urls
from oldiron_core.protocol_crawler.link_extractor import extract_same_site_links


# ──────────────────────────────────────────────────────
# link_extractor 测试
# ──────────────────────────────────────────────────────

class TestLinkExtractor(unittest.TestCase):
    def test_extracts_same_site_links(self) -> None:
        html = '''
        <html><body>
            <a href="/about">关于</a>
            <a href="/contact">联系</a>
            <a href="https://other.com/page">外站</a>
            <a href="https://example.com/team">团队</a>
        </body></html>
        '''
        links = extract_same_site_links(html, "https://example.com/")
        self.assertIn("https://example.com/about", links)
        self.assertIn("https://example.com/contact", links)
        self.assertIn("https://example.com/team", links)
        self.assertNotIn("https://other.com/page", links)

    def test_skips_static_resources(self) -> None:
        html = '<a href="/logo.png">图片</a><a href="/style.css">样式</a><a href="/page">页面</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertEqual(["https://example.com/page"], links)

    def test_skips_mailto_and_javascript(self) -> None:
        html = '<a href="mailto:a@b.com">邮件</a><a href="javascript:void(0)">JS</a><a href="/ok">OK</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertEqual(["https://example.com/ok"], links)

    def test_deduplication(self) -> None:
        html = '<a href="/about">A</a><a href="/about">B</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertEqual(["https://example.com/about"], links)

    def test_limit(self) -> None:
        html = "".join(f'<a href="/page{i}">P{i}</a>' for i in range(50))
        links = extract_same_site_links(html, "https://example.com/", limit=5)
        self.assertEqual(5, len(links))

    def test_www_subdomain_matching(self) -> None:
        html = '<a href="https://www.example.com/about">关于</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertIn("https://www.example.com/about", links)


# ──────────────────────────────────────────────────────
# sitemap 测试
# ──────────────────────────────────────────────────────

@dataclass
class _MockResponse:
    status_code: int
    text: str = ""
    content: bytes = b""


class TestSitemap(unittest.TestCase):
    def test_parses_simple_sitemap(self) -> None:
        robots_resp = _MockResponse(200, text="Sitemap: https://example.com/sitemap.xml")
        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/page1</loc></url>
            <url><loc>https://example.com/page2</loc></url>
        </urlset>'''
        sitemap_resp = _MockResponse(200, text=sitemap_xml, content=sitemap_xml.encode())

        session = MagicMock()
        session.get.side_effect = lambda url, **kw: (
            robots_resp if "robots.txt" in url else sitemap_resp
        )

        urls = discover_sitemap_urls(session, "https://example.com", limit=100)
        self.assertEqual(2, len(urls))
        self.assertIn("https://example.com/page1", urls)
        self.assertIn("https://example.com/page2", urls)

    def test_returns_empty_on_no_sitemap(self) -> None:
        session = MagicMock()
        session.get.return_value = _MockResponse(404)

        urls = discover_sitemap_urls(session, "https://example.com", limit=100)
        self.assertEqual([], urls)


# ──────────────────────────────────────────────────────
# SiteCrawlClient 测试
# ──────────────────────────────────────────────────────

class TestSiteCrawlClient(unittest.TestCase):
    @patch("oldiron_core.protocol_crawler.client.discover_sitemap_urls")
    @patch("oldiron_core.protocol_crawler.client.extract_same_site_links")
    def test_map_site_uses_sitemap_first(self, mock_links, mock_sitemap) -> None:
        mock_sitemap.return_value = ["https://a.com/p1", "https://a.com/p2"]
        client = SiteCrawlClient(SiteCrawlConfig())

        result = client.map_site("https://a.com", limit=10)
        self.assertEqual(["https://a.com/p1", "https://a.com/p2"], result)
        mock_links.assert_not_called()

    @patch("oldiron_core.protocol_crawler.client.discover_sitemap_urls")
    def test_map_site_falls_back_to_html(self, mock_sitemap) -> None:
        mock_sitemap.return_value = []
        client = SiteCrawlClient(SiteCrawlConfig())
        client._fetch_html = MagicMock(return_value='<a href="/about">关于</a><a href="/contact">联系</a>')

        result = client.map_site("https://example.com", limit=10)
        self.assertTrue(len(result) > 0)

    def test_scrape_html_returns_page_result(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig())
        client._fetch_html = MagicMock(return_value="<html>hello</html>")

        page = client.scrape_html("https://example.com/page")
        self.assertEqual("https://example.com/page", page.url)
        self.assertEqual("<html>hello</html>", page.html)

    def test_scrape_html_pages_skips_empty(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig())
        client._fetch_html = MagicMock(side_effect=["<html>ok</html>", "", "<html>ok2</html>"])

        pages = client.scrape_html_pages(["https://a.com/1", "https://a.com/2", "https://a.com/3"])
        self.assertEqual(2, len(pages))
        self.assertEqual("https://a.com/1", pages[0].url)
        self.assertEqual("https://a.com/3", pages[1].url)

    def test_html_page_result_fields(self) -> None:
        result = HtmlPageResult(url="https://test.com", html="<p>hi</p>")
        self.assertEqual("https://test.com", result.url)
        self.assertEqual("<p>hi</p>", result.html)


if __name__ == "__main__":
    unittest.main()
