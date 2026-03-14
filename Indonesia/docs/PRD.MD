# Indonesia 企业信息爬取 PRD

## 目标

爬取印度尼西亚各个公司的**公司名、法人、邮箱**。

## 交付字段

| 字段         | 说明                           |
| ------------ | ------------------------------ |
| company_name | 公司名（如 PT. XXX / CV. XXX） |
| ceo          | 法人/负责人（Nama Pimpinan）   |
| emails       | 邮箱（可能多个，分号分隔）     |
| homepage     | 官网（如有）                   |

## 数据源

### 已确认

1. **GAPENSI**（印尼全国建筑施工协会）
   - 地址：https://gapensi.or.id/anggota
   - 数据量：约 10,163 家
   - 字段：公司名、法人、邮箱、地址、省份、资质等级
   - 特点：邮箱直接在页面上，无需 Snov

2. **IndonesiaYP + AHU + Snov**（第二站）
   - 黄页入口：https://www.indonesiayp.com/category/general_business
   - 法人入口：https://ahu.go.id/pencarian/profil-pemilik-manfaat
   - 字段链路：
     - IndonesiaYP：公司名、官网、页面可见邮箱、公司经理
     - AHU：按 `PERSEROAN TERBATAS` + 去前缀公司名（去掉 PT/CV）查询法人
     - Snov：按官网域名补全邮箱

### 待发现

- 后续可继续新增其他印尼企业目录网站

## 技术方案

### 协议爬虫

- 使用 HTTP 协议层爬虫（requests / curl_cffi），不走浏览器
- 分析目标网站的接口（API / 表单提交），直接请求数据
- 需逆向分析页面的请求参数、分页机制、反爬策略
- AHU 查询需要 reCAPTCHA v3 token，统一通过 2Captcha 获取

### 参考实现

- 具体架构、断点续跑、交付系统、去重逻辑等参考韩国项目（`E:\Develop\Masterpiece\Spider\Website\OldIron\SouthKorea`）
- 包括但不限于：
  - 项目结构（src/xx_crawler/）
  - checkpoint 断点机制
  - JSONL 中间存储格式
  - delivery 按天交付框架
  - 域名去重模块
  - Snov 邮箱补全（如需要）
  - Firecrawl+LLM 邮箱补全（如需要）

## 运行依赖

- `SNOV_CLIENT_ID` / `SNOV_CLIENT_SECRET`：Snov API 凭据
- `TWO_CAPTCHA_API_KEY`：AHU reCAPTCHA v3 解码凭据
- 三者写入项目根目录 `.env`，代码通过环境变量读取

## 注意事项

### 去重

- 不同网站可能爬到同一家公司，需按**域名/邮箱**去重
- 同一网站内也可能有重复记录，按公司名去重
- 去重在交付层做，各站点数据独立存储

### 多站点架构

- 每个站点一个子目录（如 `output/gapensi/`、`output/site2/`）
- 交付时合并所有站点数据，统一去重后输出

### 交付

- 按天交付（day1、day2...），每天增量
- 格式：CSV（company_name, ceo, emails, homepage）
- 每天交付包含 summary.json 汇总
- 与前一天去重，只交付新增

### 断点续跑

- 每个阶段有 checkpoint 文件
- 中断后重跑自动跳过已处理记录

### 邮箱获取策略

- 优先从网页直接提取（GAPENSI 页面自带邮箱）
- 无邮箱的公司可选用 Snov API 补充（按域名查）
- 如有必要可启用 Firecrawl+LLM 从官网爬取邮箱

ALIGNMENT_LOCK: true
ALIGNMENT_DONE_AT: 2026-03-04
NEXT_ACTION: continue_implementation
DO_NOT_REALIGN: true
ALIGNMENT_SCOPE: 第二站 indonesiayp + AHU + Snov 联动补全
ALIGNMENT_HASH: 6779aa7f
