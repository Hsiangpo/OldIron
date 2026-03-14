# England Firecrawl 邮箱默认路线设计

## 目标

将 England 项目的默认邮箱补齐路线从 `Snov` 切换为 `Firecrawl`，保持下游 `emails` 字段和交付文件不变。

## 方案

### 1. 共享服务层

新增 `england_crawler/firecrawl` 子目录，承载以下公共能力：

- `FirecrawlKeyPool`
- `FirecrawlClient`
- `EmailLlmClient`
- `EmailDiscoveryService`

职责分层：

- `FirecrawlClient`
  - 负责 `map`、`scrape(json)` 请求
  - 处理 `401 / 402 / 429 / 5xx / request_failed`
- `EmailLlmClient`
  - 负责候选 URL 重排
  - 只做跨页决策，不解析整页 HTML
- `EmailDiscoveryService`
  - 执行 `map -> 规则预筛 -> LLM 重排 -> scrape(json) -> merge`
  - 返回 `emails` 与证据元数据

### 2. 结果边界

England 现有下游只消费 `emails`，因此 V1 继续写回 `emails_json`。

同时在公司表新增轻量证据字段：

- `email_source`
- `email_evidence_url`
- `email_evidence_quote`

不改变最终交付格式，避免连锁修改。

### 3. CH / DNB 改造

两条线都将：

- `snov_queue` 替换为 `firecrawl_queue`
- `snov_status` 替换为 `firecrawl_status`
- 默认 worker、日志、统计口径全部改为 `Firecrawl`

原有 `Snov` 模块保留为历史代码，不再作为默认运行路径。

### 4. Firecrawl 策略

默认执行顺序：

1. `map(domain)`
2. 规则筛选高价值 URL
3. 外部 LLM 从候选页中重排
4. 对首页 + 候选页执行 `scrape(json)`
5. 合并邮箱，仅保留规范化结果

抽取字段：

- `emails`
- `mailto_links`
- `has_contact_form`
- `evidence_text`
- `page_type`

### 5. 错误策略

- `401`：禁用单个 key，任务失败
- `402`：禁用单个 key，任务失败
- `429`：仅冷却单个 key，按 `Retry-After` 或配置重试
- `5xx / request_failed`：任务级退避，不做全池冷却
- 无域名：直接失败

### 6. 验证

- 单元测试覆盖：
  - key 池
  - firecrawl client 错误码
  - URL 预筛/重排
  - CH / DNB 队列流转
- 全量运行 `python -m unittest discover England/tests`
- 最小 live 验证：
  - 用一个已知官网域名跑 `map + scrape(json)`
  - 确认 `emails` 能落入 England 输出
