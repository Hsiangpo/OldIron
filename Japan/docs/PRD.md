# Firecrawl 抓取迁移 PRD

## 背景
当前 `site_agent` 已使用 Firecrawl API 抓取官网页面。本 PRD 聚焦字段来源收敛与质量稳定：公司名来自输入名，代表人/座机仅由 Firecrawl 抽取，邮箱仅走规则 + Snov。

## 目标
- 使用 Firecrawl API 承担官网抓取/渲染/内容获取（/v2/scrape 为主，必要时浅层 /v2/crawl）。
- **代表人/座机仅由 Firecrawl（/v2/extract）抽取**；公司名固定来自输入名；邮箱仅使用规则 + Snov。
- 支持多 Firecrawl API key 负载均衡（KeyPool），并具备熔断/冷却/退避机制。

## 范围
### In Scope
- `site_agent` 抓取层使用 Firecrawl（`CrawlerClient.fetch_page`/`fetch_page_rendered`）。
- `site_agent` 抽取层使用 Firecrawl /extract（代表人/座机），公司名来自输入名。
- 邮箱策略限定为规则 + Snov（不使用 Bing/LLM 邮箱筛选）。
- KeyPool 读 key（`output/firecrawl_keys.txt`）、轮询/熔断/限流处理。
- 新增单元测试（mock HTTP）。
- 更新 README 使用说明（不含真实 key）。

### Out of Scope
- `gmap_agent` 的官网发现逻辑。
- 变更输出 CSV 字段结构。

## 用户故事
- 作为数据采集运营者，我希望无需本机浏览器依赖也能抓取官网内容。
- 作为数据质量负责人，我希望代表人抽取准确率不下降。
- 作为运维人员，我希望多 key 自动负载均衡，避免单 key 限流导致任务停滞。

## 功能需求
1. Firecrawl 抓取
   - 支持单页抓取（/v2/scrape），返回 markdown + rawHtml。
   - 对需要“渲染重试”的页面，提供更宽松的超时/等待策略。
2. KeyPool
   - 从 `output/firecrawl_keys.txt` 读取多 key。
   - 401/402/429 分类处理；429 进入冷却；5xx/timeout 限次重试。
3. Firecrawl 抽取
   - 使用 /v2/extract 输出字段：representative、phone。
   - 抽取失败时返回明确错误分类。
4. 邮箱策略
   - 规则先行，规则未命中时使用 Snov。
5. 护栏
   - 限制并发与重试次数，避免成本失控。
   - 失败原因分类清晰可见。

## 非功能需求
- 安全：不在代码/日志中泄露完整 key。
- 可靠：429 风暴时可等待/降级，避免进程崩溃。
- 可测试：新增单元测试覆盖关键分支。

## 验收标准
- `python -m pytest test -v` 通过。
- 小样本运行可成功抓取并完成抽取。
- 代表人抽样准确率不低于迁移前基线。

## 配置
- `output/firecrawl_keys.txt`：一行一个 key。
- 可选：`FIRECRAWL_BASE_URL`（用于自定义 API 地址）。

## 已确认的运行策略
- 当所有 key 冷却/不可用时：等待重试（有最大等待时长，超时后标记失败）。
- 成本护栏：启用每站点/每任务上限（页数/尝试次数）。
- 访问代理：不需要代理。
