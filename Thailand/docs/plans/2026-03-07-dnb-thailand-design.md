# DNB Thailand 设计说明

## 目标

构建 Thailand 目录下的 `pc` 项目，从 D&B 的泰国建筑行业目录稳定获取公司主体数据，并串联官网补齐、邮箱补齐与日交付。

当前交付目标字段：

- `company_name`
- `key_principal`
- `emails`
- `domain`
- `phone`

## 已确认的数据事实

- 国家级 `Thailand + Construction` 当前总量显示为 `117129`。
- 省级切片求和为 `117112`，与国家级总量差 `17`。
- 本轮实现按稳定口径处理 `117112`，忽略 `17` 条未归类差额。
- 国家级、部分高量级地理切片在 `pageNumber > 20` 后会出现结果回卷，不能直接按国家页硬翻到底。
- 区级叶子页在部分情况下仍可能超过 `1000` 条，因此需要继续按 `relatedIndustries` 做二次切分。

## 推荐架构

### 方案 A：国家页直接翻页

- 优点：实现最少。
- 缺点：`page > 20` 回卷，无法稳定全量。
- 结论：放弃。

### 方案 B：地理切片到叶子后翻页

- 优点：比国家页稳定，Bangkok 等省级能继续下钻到区级。
- 缺点：部分叶子切片仍可能超过 `1000` 条，仍受 `20` 页上限影响。
- 结论：不够完整。

### 方案 C：地理切片 + 子行业切片 + 叶子翻页

- 优点：最稳，能够绕开 `20` 页上限问题。
- 做法：
  - 先按 `country -> region -> city` 递归切片。
  - 若叶子切片 `count <= 1000`，直接分页抓取。
  - 若叶子切片 `count > 1000`，用 `relatedIndustries` 继续拆桶。
  - 对所有子桶结果按 `duns` 去重汇总。
- 结论：采用此方案。

## 数据流

### Phase 1：发现稳定切片

- 输入：国家级列表接口。
- 输出：`output/dnb/segments.jsonl`
- 内容：每个可稳定抓取的切片描述，字段包含：
  - `industry_path`
  - `country_iso_two_code`
  - `region_name`
  - `city_name`
  - `expected_count`
  - `segment_type`

### Phase 2：抓取公司主体

- 输入：`segments.jsonl`
- 输出：`output/dnb/company_ids.jsonl`
- 字段：
  - `duns`
  - `company_name`
  - `company_name_url`
  - `address`
  - `region`
  - `city`
  - `country`
  - `postal_code`
  - `sales_revenue`

### Phase 3：详情补齐

- 输入：`company_ids.jsonl`
- 输出：`output/dnb/companies.jsonl`
- 补齐字段：
  - `website`
  - `domain`
  - `key_principal`
  - `phone`
  - `trade_style_name`
  - `formatted_revenue`

### Phase 4：GMAP 补官网

- 输入：`companies.jsonl`
- 规则：仅处理 `website` 为空且 `company_name/city/region` 非空的记录。
- 查询词：`company_name + city + region + Thailand`
- 输出：`output/dnb/companies_enriched.jsonl`

### Phase 5：Snov 补邮箱

- 输入：`companies_enriched.jsonl`
- 规则：仅处理 `domain` 非空的记录。
- 输出：`output/dnb/companies_with_emails.jsonl`

### Phase 6：最终去重与交付

- 去重优先级：
  - `duns + domain`
  - `duns`
- 交付只落：
  - 公司名
  - 代表人
  - 邮箱
  - 域名
  - 电话

## 关键实现策略

### 1. D&B 采集层

- 仅使用 `curl_cffi`。
- 先访问 HTML 页获取 cookie，再调用列表/详情接口。
- 列表接口按页面 URL 设置 `referer`，避免被 D&B/Akamai 直接拦截。
- 对 `403/429/5xx` 使用指数退避与 session 重建。

### 2. 回卷保护

- 每个切片分页时记录当前页首条 `duns`。
- 若高页出现与前序页重复的首批 `duns`，立即停止该桶并标记为异常切片。
- 异常切片只允许通过进一步切分解决，不允许盲目加页数重试。

### 3. GMAP / Snov

- `GMAP` 负责补官网，`Snov` 负责补邮箱。
- 官网统一清洗为 `domain`，屏蔽 `google.*`、社媒、`mailto:` 等无效域。
- `Snov` 结果与已有邮箱合并去重。

### 4. 每日交付

- `product.py` 参考韩国项目日交付方式。
- 输出目录：`output/delivery/Thailand_dayNNN/`
- 固定产物：
  - `companies.csv`
  - `keys.txt`
  - `summary.json`

## 测试策略

- 先测纯逻辑：
  - 地理/子行业切片决策
  - 域名清洗
  - 交付去重与增量计算
- 再测解析：
  - 列表接口到公司记录
  - 详情接口到补齐字段
- 最后做小样本冒烟：
  - 人工限制切片数与条数，验证整链路可跑通。

