from malaysia_crawler.businesslist.parser import parse_company_page
from malaysia_crawler.businesslist.parser import parse_redir_target


COMPANY_HTML = """
<html>
  <head>
    <title>Securepay Sdn Bhd - Shah Alam, Malaysia - Contact Number, Email Address</title>
  </head>
  <body>
    <h1>Securepay Sdn Bhd - Shah Alam, Malaysia</h1>
    <div class="info">
      <div class="label">Company name</div>
      <div class="text" id="company_name">Securepay Sdn Bhd</div>
    </div>
    <div class="info">
      <div class="label">Registration code</div>
      1358366-A
    </div>
    <div class="info">
      <div class="label">Website address</div>
      <div class="text weblinks">
        <a href="/redir/381082?u=www.securepay.my">www.securepay.my</a>
      </div>
    </div>
    <div class="info">
      <div class="label">Contact Person</div>
      hello@securepay.my
    </div>
    <div class="info">
      <div class="label">Company manager</div>
      AMIR HARIS AHMAD
    </div>
    <div class="info">
      <div class="label">Contact number</div>
      <a href="tel:+60123121979">+60123121979</a>
      <a href="tel:+603 2242 4255">+603 2242 4255</a>
    </div>
    <h2>Employees <span class="r10">2</span></h2>
    <div class="cmp_list scroller">
      <li>
        <div class="product employee r10">
          <div class="product_name b">AMIR DIRECTOR</div>
          DIRECTOR<br/>
          +60136527979<br/>
        </div>
      </li>
      <li>
        <div class="product employee r10">
          <div class="product_name b">MD KHAIRI</div>
          CTO<br/>
          +60136527979<br/>
        </div>
      </li>
    </div>
  </body>
</html>
"""

REDIR_HTML = """
<html>
  <head>
    <meta http-equiv="refresh" content="1;url=https://www.securepay.my" />
  </head>
  <body></body>
</html>
"""


def test_parse_company_page_extracts_core_fields() -> None:
    company = parse_company_page(
        COMPANY_HTML,
        response_url="https://www.businesslist.my/company/381082/securepay-sdn-bhd",
    )

    assert company is not None
    assert company.company_id == 381082
    assert company.company_name == "Securepay Sdn Bhd"
    assert company.registration_code == "1358366-A"
    assert company.website_href == "/redir/381082?u=www.securepay.my"
    assert company.contact_email == "hello@securepay.my"
    assert company.company_manager == "AMIR HARIS AHMAD"
    assert company.contact_numbers == ["+60123121979", "+603 2242 4255"]
    assert len(company.employees) == 1
    assert company.employees[0]["name"] == "AMIR DIRECTOR"
    assert company.employees[0]["role"] == "DIRECTOR"


def test_parse_redir_target_extracts_meta_refresh_url() -> None:
    target = parse_redir_target(REDIR_HTML)
    assert target == "https://www.securepay.my"


def test_parse_company_page_non_email_contact_is_empty() -> None:
    html = COMPANY_HTML.replace("hello@securepay.my", "AMIR HARIS AHMAD")
    company = parse_company_page(
        html,
        response_url="https://www.businesslist.my/company/381082/securepay-sdn-bhd",
    )

    assert company is not None
    assert company.contact_email == ""


def test_parse_company_page_404_like_company_name_returns_none() -> None:
    html = """
    <html>
      <head><title>BusinessList - Something</title></head>
      <body>
        <h1>404 error: Page not found</h1>
        <div class="info">
          <div class="label">Company name</div>
          <div class="text" id="company_name">404 error: Page not found</div>
        </div>
      </body>
    </html>
    """
    company = parse_company_page(
        html,
        response_url="https://www.businesslist.my/company/17",
    )
    assert company is None


def test_parse_company_page_fallback_manager_from_director_employee() -> None:
    html = COMPANY_HTML.replace("AMIR HARIS AHMAD", "")
    company = parse_company_page(
        html,
        response_url="https://www.businesslist.my/company/381082/securepay-sdn-bhd",
    )
    assert company is not None
    assert company.company_manager == "AMIR DIRECTOR"
