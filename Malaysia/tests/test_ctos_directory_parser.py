from malaysia_crawler.ctos_directory.parser import parse_company_detail_page
from malaysia_crawler.ctos_directory.parser import parse_directory_page


LIST_HTML = """
<html>
  <body>
    <li>
      <a href="https://businessreport.ctoscredit.com.my/oneoffreport_api/single-report/malaysia-company/0234465X/0-W-TAX-CONSULTANTS-SDN-BHD-">
        <span>0 &amp; W TAX CONSULTANTS SDN. BHD.</span><br />
      </a>
    </li>
    <li>
      <a href="/oneoffreport_api/single-report/malaysia-company/0920539K/0-CALORIES-HOLDINGS-SDN-BHD">
        <span>0 CALORIES HOLDINGS SDN BHD</span>
      </a>
    </li>
  </body>
</html>
"""

DETAIL_HTML = """
<html>
  <body>
    <h1>100 BASE TECHNOLOGY SDN BHD</h1>
    <table class="table table-striped">
      <tbody>
        <tr>
          <th class="tabledetails">Company Name</th>
          <td>100 BASE TECHNOLOGY SDN BHD</td>
        </tr>
        <tr>
          <th class="tabledetails">Company Registration No.</th>
          <td><span>1180434X</span><span> / 201601009506</span></td>
        </tr>
        <tr>
          <th class="tabledetails">Nature of Business</th>
          <td>PROVISION OF SPECIALIZED TELECOMMUNICATIONS APPLICATIONS</td>
        </tr>
        <tr>
          <th class="tabledetails">Date of Registration</th>
          <td><span>2016-03-22</span></td>
        </tr>
        <tr>
          <th class="tabledetails">State</th>
          <td><span>SELANGOR</span></td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def test_parse_directory_page_extracts_company_rows() -> None:
    page = parse_directory_page(
        LIST_HTML,
        response_url="https://businessreport.ctoscredit.com.my/oneoffreport_api/malaysia-company-listing/0/1",
    )

    assert page.prefix == "0"
    assert page.current_page == 1
    assert page.next_page == 2
    assert len(page.companies) == 2
    assert page.companies[0].company_name == "0 & W TAX CONSULTANTS SDN. BHD."
    assert page.companies[0].registration_no == "0234465X"


def test_parse_company_detail_page_extracts_free_fields() -> None:
    detail = parse_company_detail_page(
        DETAIL_HTML,
        "https://businessreport.ctoscredit.com.my/oneoffreport_api/single-report/malaysia-company/1180434X/100-BASE-TECHNOLOGY-SDN-BHD",
    )

    assert detail.company_name == "100 BASE TECHNOLOGY SDN BHD"
    assert detail.company_registration_no == "1180434X"
    assert detail.new_registration_no == "201601009506"
    assert detail.nature_of_business == "PROVISION OF SPECIALIZED TELECOMMUNICATIONS APPLICATIONS"
    assert detail.date_of_registration == "2016-03-22"
    assert detail.state == "SELANGOR"

