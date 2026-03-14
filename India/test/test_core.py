from src.core.parser import extract_total_pages, parse_detail_page, parse_list_page


LIST_HTML = """
<html><body>
<table>
<tr><td>CIN</td><td>Name</td><td>Status</td><td>Paid Up Capital</td><td>Address</td></tr>
<tr>
  <td><a href="https://www.zaubacorp.com/EXAMPLE-LLP-AAA-0001">AAA-0001</a></td>
  <td><a href="https://www.zaubacorp.com/EXAMPLE-LLP-AAA-0001">EXAMPLE LLP</a></td>
  <td>Active</td>
  <td>100</td>
  <td>Test Address</td>
</tr>
</table>
<div>Page 1 of 58,113</div>
</body></html>
"""

DETAIL_HTML = """
<html><body>
<h3>Basic Information</h3>
<div class="table-responsive">
  <table>
    <tr><td>LLP Identification Number</td><td>AAA-0001</td></tr>
    <tr><td>Name</td><td>EXAMPLE LLP</td></tr>
  </table>
</div>
<h3>Contact Details of EXAMPLE</h3>
<div id="contact-details-content">
  <span>Email ID: test@example.com</span>
  <span>Website: Not Available</span>
  <span>Address:</span>
  <div>Test Address Line</div>
</div>
<h3>Directors & Key Managerial Personnel of EXAMPLE</h3>
<div>
  <h4>Current Directors & Key Managerial Personnel of EXAMPLE</h4>
  <table>
    <tr>
      <th>DIN</th>
      <th>Director Name</th>
      <th>Designation</th>
      <th>Appointment Date</th>
    </tr>
    <tr>
      <td>08056255</td>
      <td>HEMANT KUMAR SATIJA</td>
      <td>Director</td>
      <td>2018-09-30</td>
    </tr>
    <tr>
      <td>00199920</td>
      <td>RAMDHARI SHARMA</td>
      <td>Director</td>
      <td>1995-12-05</td>
    </tr>
  </table>
</div>
</body></html>
"""


def test_parse_list_page():
    companies = parse_list_page(LIST_HTML)
    assert len(companies) == 1
    assert companies[0]["cin"] == "AAA-0001"
    assert companies[0]["name"] == "EXAMPLE LLP"
    assert companies[0]["detail_url"].endswith("AAA-0001")


def test_extract_total_pages():
    total = extract_total_pages(LIST_HTML)
    assert total == 58113


def test_parse_detail_page():
    basic, contact, current_director = parse_detail_page(DETAIL_HTML)
    assert basic["LLP Identification Number"] == "AAA-0001"
    assert basic["Name"] == "EXAMPLE LLP"
    assert contact["Email ID"] == "test@example.com"
    assert contact["Website"] == "Not Available"
    assert contact["Address"] == "Test Address Line"
    assert current_director["DIN"] == "08056255"
    assert current_director["Director Name"] == "HEMANT KUMAR SATIJA"
    assert current_director["Designation"] == "Director"
    assert current_director["Appointment Date"] == "2018-09-30"


def test_parse_detail_page_without_directors():
    html = "<html><body><h3>Basic Information</h3></body></html>"
    _, _, current_director = parse_detail_page(html)
    assert current_director == {}
