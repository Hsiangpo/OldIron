# 协议爬虫：catch.co.kr 韩国企业信息采集

## 目标站点

- URL: https://www.catch.co.kr/Comp/CompMajor
- 技术栈: Nuxt.js SSR + Cloudflare CDN
- 总记录数: 168,593 家公司
- 登录要求: 无

## 采集字段

| 字段                      | 来源              |      覆盖率      |
| ------------------------- | ----------------- | :--------------: |
| comp_id                   | 列表API           |       100%       |
| comp_name                 | 列表API           |       100%       |
| comp_name_full (法人全称) | 详情页 `__NUXT__` |       100%       |
| ceo_name (代表者)         | 详情页 HTML表格   |       100%       |
| industry (行业)           | 列表API           |       100%       |
| location (地区)           | 列表API           |       100%       |
| size (企业规模)           | 列表API           |       100%       |
| homepage (公司官网)       | 详情页 JSON-LD    |       ~37%       |
| emails (邮箱)             | Snov.io API       | 取决于官网覆盖率 |

## 输出格式

JSONL 文件 (`output/companies.jsonl`)，每行一条公司记录。
没有官网和邮箱的公司照样收录，对应字段留空。

## 采集策略

### Phase 1: 列表API分页遍历

- POST `/api/v1.0/comp/compMajor/getMainCompanyListV2`
- PageSize=30, 共 ~5,620 页
- 输出: `output/company_ids.jsonl`

### Phase 2: 详情页逐一解析

- GET `/Comp/CompSummary/{CompID}`
- 解析 HTML 表格 + JSON-LD + `__NUXT__`
- 输出: 更新 `output/companies.jsonl`

### Phase 3: Snov.io 邮箱补充

- 对有官网的公司调用 Snov Domain Search
- 凭据复用 Malaysia 项目 `.env`

## 限速策略

- 列表API: 请求间隔 1-2s, 每 100 页休息 10s
- 详情页: 请求间隔 0.5-1.5s
- Snov: 按 API 限额控制
