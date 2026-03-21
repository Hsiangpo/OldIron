# PRD - ZaubaCorp Active Companies 协议爬虫

## 目标
- 从 https://www.zaubacorp.com/companies-list/status-Active-company.html 开始，全量爬取 Active 公司列表（分页）。
- 进入每家公司详情页，抓取所需字段并输出 JSONL/CSV。
- 支持断点续跑、分批运行、并发提速。

## 详情页字段
- Basic Information：全部字段
- Contact Details：全部字段
- Directors & Key Managerial Personnel：
  - 只采集 “Current Directors & Key Managerial Personnel” 表格的第一行
  - 字段：DIN / Director Name / Designation / Appointment Date
  - 若无该表或无数据，则返回空

## 输出
- JSONL + CSV
- CSV 基础字段 + JSON 字符串字段（basic_info/contact_details/current_director）

## 风控与限制
- 网站有 Cloudflare 保护，需要使用 cookies（Chrome 验证后导出）
- 429/挑战需自动回退并重试
