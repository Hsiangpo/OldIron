# England Firecrawl 邮箱链路设计

## 目标

- 将 England 默认邮箱补齐阶段从 `Snov` 切换为 `Firecrawl + 外部 LLM + Firecrawl 内置 LLM`。
- 保持现有输出字段 `emails` 不变，避免下游交付与快照导出破坏。
- 保守复用隔壁项目已有 `Firecrawl key pool`、错误码、LLM 配置口径。

## 总体方案

### 公共模块

- 新增 `england_crawler.firecrawl` 子模块。
- 组件拆分：
  - `config.py`：读取 `FIRECRAWL_*` 与 `LLM_*` 环境变量。
  - `key_pool.py`：复用 Thailand 的 sqlite key pool 机制。
  - `client.py`：同步 Firecrawl 客户端，支持 `map`、`scrape(json)`，统一错误码。
  - `llm_client.py`：外部 LLM 两个职责：
    - 候选页重排
    - 邮箱结果合并与排序
  - `email_service.py`：封装 `domain -> emails` 主流程。

### 邮箱主流程

1. 输入官网域名。
2. `map(domain)` 发现站内 URL。
3. 规则粗筛 URL：
   - `contact/about/team/leadership/support/help/privacy/legal/imprint/careers/jobs/terms/pdf`
4. 外部 LLM 对粗筛结果重排，选高价值候选页。
5. 对首页 + 候选页逐页执行 `scrape(json)`：
   - 抽取 `emails`、`mailto_links`、`phones`、`has_contact_form`、`evidence_text`
6. 外部 LLM 做最终合并：
   - 去重
   - 优先级排序
   - 过滤明显非邮箱文本
7. 写回 `emails`。

## England 接入策略

### Companies House

- 保留 `ch -> gmap -> email` 三段结构。
- 新阶段默认命名改为 `Firecrawl`。
- 先沿用现有队列表结构，减少数据库迁移风险。
- 队列内部可以暂时复用 `snov_queue/snov_status` 字段承载邮箱阶段，但日志、CLI、配置默认文案改为 `Firecrawl`。

### DNB

- 保留 `detail -> gmap -> email` 主流程。
- 替换原 `Snov` worker 为 `Firecrawl` worker。
- 保留 `emails_json` 与 `final_companies` 刷新逻辑。

## 错误处理

- 统一 Firecrawl 错误码：
  - `firecrawl_key_unavailable`
  - `firecrawl_401`
  - `firecrawl_402`
  - `firecrawl_429`
  - `firecrawl_5xx`
  - `firecrawl_request_failed`
  - `firecrawl_http_*`
- key 池策略：
  - `401/402` 只禁用当前 key。
  - `429` 只冷却当前 key，并尊重 `Retry-After`。
  - `5xx`、网络错误不做全池冷却，只做任务级退避。

## 配置口径

- 新增 `.env` / `.env.example`：
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
  - `LLM_TIMEOUT_SECONDS`

## 测试与验证

- 新增公共模块单测：
  - key pool
  - firecrawl client
  - llm client 解析
  - email service URL 选择与合并
- 修改 CH / DNB 现有测试：
  - 默认配置校验改为 Firecrawl
  - 邮箱阶段 worker 改为 Firecrawl
- 最后运行：
  - `python -m unittest discover England/tests`
  - 针对受影响文件跑诊断

## 风险

- 现有 sqlite 字段名仍叫 `snov_*`，短期可接受但语义不干净。
- Firecrawl 账号状态与 credits 需做最小 live 校验。
- 外部 LLM 合并阶段若 prompt 设计差，会影响邮箱排序质量。
