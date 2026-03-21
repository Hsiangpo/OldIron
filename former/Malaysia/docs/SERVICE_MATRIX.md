# 服务矩阵（两步主流程）

## CTOS Directory（主源）
- 入口：`https://businessreport.ctoscredit.com.my/oneoffreport_api/malaysia-company-listing/{prefix}/{page}`
- 访问条件：公开可访问，无登录，无 yzm。
- 全量策略：按 `0-9a-z` 前缀分页抓取，空页停止当前前缀。
- 可得字段：
  - 列表：`company_name`、`registration_no`、`detail_url`、`prefix`、`page`
  - 免费详情：`company_registration_no`、`new_registration_no`、`nature_of_business`、`date_of_registration`、`state`
- 边界：董事/股东/财务不在免费字段中。

## BusinessList（补源）
- 入口：`https://www.businesslist.my/company/{id}`
- 访问条件：公开可访问，无登录，无 yzm。
- 全量策略：按 `company_id` 区间抓取，404 跳过。
- 可得字段（覆盖率不稳定）：
  - `company_name`、`website_url`、`contact_numbers`
  - `contact_email`（仅可识别邮箱）、`company_manager`
  - `employees`（仅保留 `DIRECTOR`）
- 边界：`contact_email` / `company_manager` / `employees` 均为目录展示信息，不是官方法人字段。
