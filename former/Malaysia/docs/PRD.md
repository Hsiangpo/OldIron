# PRD - Malaysia Company Crawler（两步主流程版）

ALIGNMENT_LOCK: true

## 1. 目标
- 稳定获取马来西亚全量公司名录（公司名 + 注册号）。
- 通过 BusinessList 补联系人/管理人/官网/电话。
- 全项目仅保留两步：`CTOS -> BusinessList`。

## 2. 数据源范围
- CTOS Directory（主源）
  - 提供全量目录与免费基础详情。
- BusinessList（补源）
  - 提供联系人、管理人、官网、联系电话等补充字段。

## 3. 输出需求
- CTOS：
  - `ctos_directory_companies.jsonl/csv`
  - 可选 `ctos_directory_details.jsonl/csv`
- BusinessList：
  - `businesslist_companies.jsonl/csv`

## 4. 字段目标
- 阶段A（稳定）：`company_name`、`registration_no`
- 阶段B（补源）：`contact_email`、`company_manager`、`website_url`、`contact_numbers`、`employees(DIRECTOR)`

## 5. 风险与边界
- 目录联系人与管理人字段不等于官方法人。
- 免费链路无法稳定拿到官方董事/股东/财务全量字段。

## 6. 验收标准
- CTOS 冒烟可稳定产出 1 页公司列表。
- BusinessList 冒烟可稳定命中至少 1 家公司档案。
