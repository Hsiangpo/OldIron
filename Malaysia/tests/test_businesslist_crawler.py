import json

from malaysia_crawler.businesslist.crawler import BusinessListCrawler
from malaysia_crawler.businesslist.models import BusinessListCompany


def test_build_row_uses_slim_fields_only() -> None:
    crawler = BusinessListCrawler()
    company = BusinessListCompany(
        company_id=381082,
        company_url="https://www.businesslist.my/company/381082/securepay-sdn-bhd",
        company_name="Securepay Sdn Bhd",
        registration_code="1358366-A",
        address="Shah Alam",
        contact_numbers=["+60123121979"],
        website_href="/redir/381082?u=www.securepay.my",
        website_url="https://www.securepay.my",
        contact_email="hello@securepay.my",
        company_manager="AMIR HARIS AHMAD",
        employees=[{"name": "AMIR DIRECTOR", "role": "DIRECTOR", "phone": "+60136527979"}],
    )

    row = crawler._build_row(company)
    assert set(row.keys()) == {
        "company_name",
        "contact_numbers",
        "website_url",
        "contact_email",
        "company_manager",
        "employees",
    }
    assert row["contact_email"] == "hello@securepay.my"
    assert json.loads(row["employees"])[0]["role"] == "DIRECTOR"


def test_businesslist_crawler_proxy_settings() -> None:
    crawler = BusinessListCrawler(
        proxy_url="http://127.0.0.1:7890",
        use_system_proxy=True,
    )
    assert crawler.client.session.trust_env is True
    assert crawler.client.session.proxies["http"] == "http://127.0.0.1:7890"
    assert crawler.client.session.proxies["https"] == "http://127.0.0.1:7890"
