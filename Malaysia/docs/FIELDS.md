# 字段盘点（两步主流程）

## CTOS Directory
### 列表字段
- company_name
- registration_no
- detail_url
- detail_path
- prefix
- page

### 免费详情字段（`--with-detail`）
- company_registration_no
- new_registration_no
- nature_of_business
- date_of_registration
- state

## BusinessList
### 可补充字段
- company_name
- contact_numbers
- website_url
- contact_email
- company_manager
- employees（仅保留 DIRECTOR）

### 字段语义边界
- `contact_email`：仅保留可识别邮箱，非邮箱文本会置空。
- `company_manager`：目录里的管理人字段，不等于官方法人登记。
- `employees`：仅保留 `role == DIRECTOR` 的目录员工信息，不是官方董事名册。

## CTOS + BusinessList + Snov（管道输出）
### 输出字段
- company_name
- domain
- contact_eamils
- company_manager

### 字段语义边界
- `contact_eamils`：JSON 数组字符串，合并 `BusinessList.contact_email` 与 `Snov` 域名邮箱并去重。
- `company_manager`：目录里的管理人字段，不等于官方法人登记。
