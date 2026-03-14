from malaysia_crawler.snov.client import extract_domain
from malaysia_crawler.snov.client import is_valid_domain


def test_extract_domain_normalizes_common_urls() -> None:
    assert extract_domain("https://www.securepay.my/") == "securepay.my"
    assert extract_domain("http://securepay.my/path?a=1") == "securepay.my"
    assert extract_domain("www.securepay.my") == "securepay.my"
    assert extract_domain("securepay.my") == "securepay.my"


def test_is_valid_domain_accepts_fqdn_and_rejects_invalid_value() -> None:
    assert is_valid_domain("securepay.my") is True
    assert is_valid_domain("sub.securepay.my") is True
    assert is_valid_domain("clickmyproject") is False
    assert is_valid_domain("") is False
