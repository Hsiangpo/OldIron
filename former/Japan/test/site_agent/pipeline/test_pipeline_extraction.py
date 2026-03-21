from site_agent.pipeline import _extract_labeled_values_from_html
from site_agent.pipeline import _extract_email_candidates_from_pages
from site_agent.models import PageContent


def test_extract_labeled_values_from_html_table() -> None:
    html = """
    <table>
      <tr><th>会社名</th><td>愛電株式会社</td></tr>
      <tr><th>代表者</th><td>清田 淳一</td></tr>
      <tr><th>許可番号</th><td>厚生労働大臣許可 派13-314774</td></tr>
    </table>
    """
    values = _extract_labeled_values_from_html(html, ["代表者"])
    assert values and values[0] == "清田 淳一"


def test_extract_labeled_values_from_html_definition_list() -> None:
    html = """
    <dl>
      <dt>代表取締役</dt><dd>山田 太郎</dd>
      <dt>所在地</dt><dd>東京都港区</dd>
    </dl>
    """
    values = _extract_labeled_values_from_html(html, ["代表取締役"])
    assert values and values[0] == "山田 太郎"


def test_extract_email_from_obfuscated_text() -> None:
    html = (
        "info (at) example (dot) co.jp / info＠example.co.jp / info&#64;example.co.jp"
    )
    page = PageContent(
        url="https://example.co.jp/contact",
        markdown="",
        raw_html=html,
        fit_markdown="",
        title="contact",
        links=[],
    )
    candidates = _extract_email_candidates_from_pages(
        {page.url: page}, "https://example.co.jp", limit=10
    )
    emails = {item.get("email") for item in candidates if isinstance(item, dict)}
    assert "info@example.co.jp" in emails


def _encode_cfemail(email: str, key: int = 0x12) -> str:
    encoded = f"{key:02x}"
    for ch in email:
        encoded += f"{ord(ch) ^ key:02x}"
    return encoded


def test_extract_email_from_cfemail() -> None:
    email = "info@example.co.jp"
    encoded = _encode_cfemail(email)
    html = f'<span class="__cf_email__" data-cfemail="{encoded}">[email&#160;protected]</span>'
    page = PageContent(
        url="https://example.co.jp/contact",
        markdown="",
        raw_html=html,
        fit_markdown="",
        title="contact",
        links=[],
    )
    candidates = _extract_email_candidates_from_pages(
        {page.url: page}, "https://example.co.jp", limit=10
    )
    emails = {item.get("email") for item in candidates if isinstance(item, dict)}
    assert email in emails


def test_extract_email_from_data_attrs() -> None:
    html = '<a data-user="info" data-domain="example.co.jp">email</a>'
    page = PageContent(
        url="https://example.co.jp/contact",
        markdown="",
        raw_html=html,
        fit_markdown="",
        title="contact",
        links=[],
    )
    candidates = _extract_email_candidates_from_pages(
        {page.url: page}, "https://example.co.jp", limit=10
    )
    emails = {item.get("email") for item in candidates if isinstance(item, dict)}
    assert "info@example.co.jp" in emails


def test_extract_email_from_data_email() -> None:
    html = '<span data-email="info (at) example (dot) co.jp"></span>'
    page = PageContent(
        url="https://example.co.jp/contact",
        markdown="",
        raw_html=html,
        fit_markdown="",
        title="contact",
        links=[],
    )
    candidates = _extract_email_candidates_from_pages(
        {page.url: page}, "https://example.co.jp", limit=10
    )
    emails = {item.get("email") for item in candidates if isinstance(item, dict)}
    assert "info@example.co.jp" in emails
