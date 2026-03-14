from malaysia_crawler.businesslist.cdp_crawler import _is_blocked_page


def test_is_blocked_page_detects_cloudflare_title() -> None:
    assert _is_blocked_page(title="Just a moment...", html="<html></html>") is True


def test_is_blocked_page_detects_challenge_html() -> None:
    html = "<html><body>执行安全验证 challenge-platform</body></html>"
    assert _is_blocked_page(title="请稍候…", html=html) is True


def test_is_blocked_page_returns_false_for_normal_company_page() -> None:
    html = "<html><body><h1 id='company_name'>Securepay Sdn Bhd</h1></body></html>"
    assert _is_blocked_page(title="Securepay Sdn Bhd", html=html) is False
