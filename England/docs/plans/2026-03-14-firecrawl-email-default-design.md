# England Firecrawl 邮箱默认路线设计

## 目标

将 England 项目的默认邮箱补齐阶段从 `Snov` 切换为 `Firecrawl + 外部 LLM + Firecrawl 内部 JSON/Extract`，保持下游 `emails` 输出字段不变。

## 范围

- 覆盖 `companies_house`
- 覆盖 `dnb`
- 保持 `delivery.py` 现有消费方式不变
- 保留旧 `Snov` 模块代码，但移出默认主链路

## 设计概览

### 总体流程

1. 上游阶段产出官网 `domain`
2. 邮箱阶段把 `domain` 投给 `FirecrawlEmailService`
3. `FirecrawlEmailService` 执行：
   - `map(domain)` 发现站内 URL
   - 规则预筛高价值候选页
   - 外部 LLM 对候选页重排
   - Firecrawl `scrape(json)` 抽取邮箱、电话、联系表单、证据
   - 外部 LLM 合并多页结果
4. 返回 `emails`
5. 写回原有 `emails_json`

### 默认原则

- 默认不再调用 `Snov`
- 默认不走 `/agent`
- 默认不全站 `crawl`
- 默认先站内发现，再按页抽取
- 保留证据页和置信度，便于后续扩展

## 模块设计

新增目录：`England/src/england_crawler/firecrawl`

计划文件：

- `client.py`
  - Firecrawl 同步客户端
  - 支持 `map_urls()`、`scrape_json()`
  - 统一错误码：`firecrawl_401`、`firecrawl_402`、`firecrawl_429`、`firecrawl_5xx`、`firecrawl_request_failed`、`firecrawl_key_unavailable`
- `key_pool.py`
  - 基于 sqlite 的 key 池
  - 仅对单 key 处理 `429 / Retry-After`
  - `5xx / 网络异常` 只做任务级退避，不全池冷却
- `domain_cache.py`
  - 域名级查询去重与结果缓存
- `llm_client.py`
  - 外部 LLM 客户端
  - 两个能力：候选页重排、结果合并
- `email_service.py`
  - 封装整个邮箱提取流程

## 队列与状态设计

### Companies House

将第三阶段从 `snov` 语义替换为 `firecrawl`：

- `snov_queue` -> `firecrawl_queue`
- `snov_status` -> `firecrawl_status`
- `skip_snov` -> `skip_firecrawl`
- `snov_workers` -> `firecrawl_workers`

### DNB

同样将第三阶段从 `snov` 语义替换为 `firecrawl`：

- `snov_queue` -> `firecrawl_queue`
- `snov_status` -> `firecrawl_status`
- `skip_snov` -> `skip_firecrawl`
- `snov_workers` -> `firecrawl_workers`

## 抽取策略

### 候选页规则预筛

优先路径：

- `/contact`
- `/about`
- `/team`
- `/leadership`
- `/management`
- `/support`
- `/privacy`
- `/legal`
- `/imprint`
- `/careers`
- `/terms`
- PDF 链接

### 单页抽取字段

Firecrawl 单页抽取至少输出：

- `emails`
- `phones`
- `has_contact_form`
- `contact_form_url`
- `evidence_text`
- `page_type`

### 合并策略

外部 LLM 合并多页结果时输出：

- `emails`
- `best_email`
- `confidence`
- `same_domain_flags`
- `evidence`

当前主流程先只落 `emails`，其余字段先留在 service 内部结果结构中，后续按需扩展到 store。

## 配置设计

新增环境变量：

- `FIRECRAWL_KEYS`
- `FIRECRAWL_KEYS_FILE`
- `FIRECRAWL_KEY_POOL_DB`
- `FIRECRAWL_BASE_URL`
- `FIRECRAWL_TIMEOUT_SECONDS`
- `FIRECRAWL_MAX_RETRIES`
- `FIRECRAWL_KEY_PER_LIMIT`
- `FIRECRAWL_KEY_WAIT_SECONDS`
- `FIRECRAWL_KEY_COOLDOWN_SECONDS`
- `FIRECRAWL_KEY_FAILURE_THRESHOLD`
- `LLM_API_KEY`
- `LLM_BASE_URL`
- `LLM_MODEL`
- `LLM_REASONING_EFFORT`

## 错误处理

- `401`
  - 视为 key 失效
  - 禁用单 key
- `402`
  - 视为 key 无额度
  - 禁用单 key
- `429`
  - 尊重 `Retry-After`
  - 只冷却单 key
- `5xx`
  - 不冷却全池
  - 仅任务级退避
- 请求异常
  - 不冷却全池
  - 仅任务级退避

## 测试策略

- 新增 `firecrawl` 模块单测
- 改 `companies_house` 流程测试
- 改 `dnb` 流程测试
- 保证现有 `emails` 导出测试继续通过

## 风险与取舍

- 旧 `Snov` 模块先保留，降低一次性大改风险
- 当前先把证据、置信度保留在 service 结果层，不立即扩展 England store 表结构过多字段
- 默认主路径切到 `Firecrawl` 后，再看是否需要彻底删除 `Snov` 模块
