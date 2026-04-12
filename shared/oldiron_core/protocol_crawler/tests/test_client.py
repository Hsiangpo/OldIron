"""protocol_crawler 单元测试。"""

from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SHARED_DIR = ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from oldiron_core.protocol_crawler.client import SiteCrawlClient, SiteCrawlConfig, HtmlPageResult
from oldiron_core.protocol_crawler.sitemap import decode_response_text
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
        html = '<a href="/logo.png">图片</a><a href="/style.css">样式</a><a href="/book.pdf">PDF</a><a href="/page">页面</a>'
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

    def test_subdomain_is_excluded_by_default(self) -> None:
        html = '<a href="https://blog.example.com/post">博客</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertEqual([], links)

    def test_subdomain_is_included_when_enabled(self) -> None:
        html = '<a href="https://blog.example.com/post">博客</a>'
        links = extract_same_site_links(
            html,
            "https://example.com/",
            include_subdomains=True,
        )
        self.assertEqual(["https://blog.example.com/post"], links)

    def test_skips_invalid_bracketed_host_href(self) -> None:
        html = '<a href="https://[HTTP_HOST]/broken">坏链接</a><a href="/ok">OK</a>'
        links = extract_same_site_links(html, "https://example.com/")
        self.assertEqual(["https://example.com/ok"], links)


# ──────────────────────────────────────────────────────
# sitemap 测试
# ──────────────────────────────────────────────────────

@dataclass
class _MockResponse:
    status_code: int
    text: str = ""
    content: bytes = b""
    headers: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {}


@dataclass
class _BrokenTextResponse:
    status_code: int
    content: bytes
    headers: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {}

    @property
    def text(self) -> str:
        raise UnicodeDecodeError("utf-8", self.content, 0, 1, "broken decode")


class TestSitemap(unittest.TestCase):
    def test_decode_response_text_falls_back_to_cp932_bytes(self) -> None:
        text = "<html><body>株式会社あいう</body></html>"
        resp = _BrokenTextResponse(
            200,
            content=text.encode("cp932"),
            headers={"Content-Type": "text/html"},
        )
        self.assertEqual(text, decode_response_text(resp))

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

    def test_filters_out_subdomain_urls_by_default(self) -> None:
        robots_resp = _MockResponse(200, text="Sitemap: https://example.com/sitemap.xml")
        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/page1</loc></url>
            <url><loc>https://blog.example.com/page2</loc></url>
        </urlset>'''
        sitemap_resp = _MockResponse(200, text=sitemap_xml, content=sitemap_xml.encode())

        session = MagicMock()
        session.get.side_effect = lambda url, **kw: (
            robots_resp if "robots.txt" in url else sitemap_resp
        )

        urls = discover_sitemap_urls(session, "https://example.com", limit=100)
        self.assertEqual(["https://example.com/page1"], urls)

    def test_keeps_subdomain_urls_when_enabled(self) -> None:
        robots_resp = _MockResponse(200, text="Sitemap: https://example.com/sitemap.xml")
        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/page1</loc></url>
            <url><loc>https://blog.example.com/page2</loc></url>
        </urlset>'''
        sitemap_resp = _MockResponse(200, text=sitemap_xml, content=sitemap_xml.encode())

        session = MagicMock()
        session.get.side_effect = lambda url, **kw: (
            robots_resp if "robots.txt" in url else sitemap_resp
        )

        urls = discover_sitemap_urls(
            session,
            "https://example.com",
            limit=100,
            include_subdomains=True,
        )
        self.assertEqual(
            ["https://example.com/page1", "https://blog.example.com/page2"],
            urls,
        )

    def test_sitemap_retries_insecure_https_before_giving_up(self) -> None:
        robots_resp = _MockResponse(200, text="Sitemap: https://example.com/sitemap.xml")
        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/page1</loc></url>
        </urlset>'''
        sitemap_resp = _MockResponse(200, text=sitemap_xml, content=sitemap_xml.encode())
        https_error = RuntimeError(
            "curl: (60) SSL certificate problem: unable to get local issuer certificate"
        )

        session = MagicMock()
        session.get.side_effect = [https_error, robots_resp, sitemap_resp]

        urls = discover_sitemap_urls(session, "https://example.com", limit=100)

        self.assertEqual(["https://example.com/page1"], urls)
        self.assertEqual("https://example.com/robots.txt", session.get.call_args_list[0].args[0])
        self.assertEqual("https://example.com/robots.txt", session.get.call_args_list[1].args[0])
        self.assertFalse(session.get.call_args_list[0].kwargs.get("verify", True) is False)
        self.assertFalse(session.get.call_args_list[1].kwargs.get("verify", True))

    def test_skips_document_urls_from_sitemap(self) -> None:
        robots_resp = _MockResponse(200, text="Sitemap: https://example.com/sitemap.xml")
        sitemap_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/profile.pdf</loc></url>
            <url><loc>https://example.com/contact</loc></url>
        </urlset>'''
        sitemap_resp = _MockResponse(200, text=sitemap_xml, content=sitemap_xml.encode())

        session = MagicMock()
        session.get.side_effect = lambda url, **kw: (
            robots_resp if "robots.txt" in url else sitemap_resp
        )

        urls = discover_sitemap_urls(session, "https://example.com", limit=100)
        self.assertEqual(["https://example.com/contact"], urls)


# ──────────────────────────────────────────────────────
# SiteCrawlClient 测试
# ──────────────────────────────────────────────────────

class TestSiteCrawlClient(unittest.TestCase):
    @patch("oldiron_core.protocol_crawler.client._is_unavailable_local_proxy")
    def test_init_disables_unavailable_local_proxy(self, mock_proxy_down) -> None:
        mock_proxy_down.return_value = True
        client = SiteCrawlClient(SiteCrawlConfig(proxy_url="http://127.0.0.1:7897"))

        self.assertFalse(client._using_proxy)

    @patch("oldiron_core.protocol_crawler.client._is_unavailable_local_proxy")
    def test_dead_local_proxy_error_switches_to_direct(self, mock_proxy_down) -> None:
        mock_proxy_down.return_value = False
        client = SiteCrawlClient(SiteCrawlConfig(proxy_url="http://127.0.0.1:7897"))
        reset_calls: list[bool] = []

        def _capture_reset(*, use_proxy: bool) -> None:
            client._using_proxy = use_proxy
            reset_calls.append(use_proxy)

        client._reset_session = _capture_reset  # type: ignore[method-assign]
        switched = client._disable_unavailable_local_proxy(
            RuntimeError("curl: (7) Failed to connect to 127.0.0.1 port 7897")
        )

        self.assertTrue(switched)
        self.assertEqual([False], reset_calls)

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

    def test_fetch_html_falls_back_to_http_on_tls_error(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0))
        https_error = RuntimeError("curl: (35) TLS wrong_version_number")
        http_response = MagicMock(
            status_code=200,
            text="<html>ok</html>",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        client._session.get = MagicMock(side_effect=[https_error, http_response])

        html = client._fetch_html("https://example.com")

        self.assertEqual("<html>ok</html>", html)
        self.assertEqual(2, client._session.get.call_count)
        self.assertEqual("https://example.com", client._session.get.call_args_list[0].args[0])
        self.assertEqual("http://example.com", client._session.get.call_args_list[1].args[0])

    def test_fetch_html_retries_insecure_https_before_http_fallback(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0))
        https_error = RuntimeError("curl: (60) SSL certificate problem: unable to get local issuer certificate")
        insecure_response = MagicMock(
            status_code=200,
            text="<html>secure-ok</html>",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        client._session.get = MagicMock(side_effect=[https_error, insecure_response])

        html = client._fetch_html("https://example.com")

        self.assertEqual("<html>secure-ok</html>", html)
        self.assertEqual(2, client._session.get.call_count)
        self.assertEqual("https://example.com", client._session.get.call_args_list[0].args[0])
        self.assertEqual("https://example.com", client._session.get.call_args_list[1].args[0])
        self.assertFalse(client._session.get.call_args_list[0].kwargs.get("verify", True) is False)
        self.assertFalse(client._session.get.call_args_list[1].kwargs.get("verify", True))

    def test_fetch_html_decodes_non_utf8_response_without_crashing(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0))
        response = _BrokenTextResponse(
            200,
            content="<html>株式会社あいう</html>".encode("cp932"),
            headers={"Content-Type": "text/html"},
        )
        client._session.get = MagicMock(return_value=response)

        html = client._fetch_html("https://example.com/page")

        self.assertIn("株式会社あいう", html)

    def test_fetch_html_skips_pdf_response(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0))
        pdf_response = MagicMock(
            status_code=200,
            text="%PDF-1.4 ...",
            headers={"Content-Type": "application/pdf"},
        )
        client._session.get = MagicMock(return_value=pdf_response)

        html = client._fetch_html("https://example.com/profile.pdf")

        self.assertEqual("", html)

    def test_fetch_html_truncates_large_html(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0, max_html_chars=20))
        long_html = "<html>" + ("a" * 60) + "</html>"
        response = MagicMock(
            status_code=200,
            text=long_html,
            headers={"Content-Type": "text/html"},
        )
        client._session.get = MagicMock(return_value=response)

        html = client._fetch_html("https://example.com/page")

        self.assertIn("内容过长已截断", html)
        self.assertLess(len(html), len(long_html))

    def test_fetch_html_keeps_full_large_html_when_truncate_disabled(self) -> None:
        client = SiteCrawlClient(SiteCrawlConfig(max_retries=0, max_html_chars=20))
        long_html = "<html>" + ("a" * 60) + "</html>"
        response = MagicMock(
            status_code=200,
            text=long_html,
            headers={"Content-Type": "text/html"},
        )
        client._session.get = MagicMock(return_value=response)

        html = client._fetch_html("https://example.com/page", truncate_html=False)

        self.assertEqual(long_html, html)
        self.assertNotIn("内容过长已截断", html)

    def test_html_page_result_fields(self) -> None:
        result = HtmlPageResult(url="https://test.com", html="<p>hi</p>")
        self.assertEqual("https://test.com", result.url)
        self.assertEqual("<p>hi</p>", result.html)


if __name__ == "__main__":
    unittest.main()
