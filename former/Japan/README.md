# Official Site Agent

日本企业官网信息采集工具（命令行版）。基于 Firecrawl + 规则/Snov + LLM 选链，完成官网发现与字段抽取（公司名/代表人/邮箱/电话）。

## 功能概览

- `gmap_agent`：通过 Google Maps 协议获取企业官网候选链接。
- `site_agent`：用 Firecrawl 抽取代表人/电话，邮箱走规则 + Snov，公司名来自输入名；LLM 仅用于选链。
- `hojin_agent`：国税厅法人番号数据源下载与解析。
- `corp_agent`：法人名录筛选 + 官网富化流程。
- `web_agent`：命令行一键运行入口（仅 CLI，无 Web UI）。

## 环境与安装

- Python 3.10+
- Firecrawl API Keys（写入 `output/firecrawl_keys.txt`，每行一个）

```powershell
pip install -r requirements.txt

# 安装 Playwright 的 Chromium（gmap 自动抓取 pb 参数需要）
python -m playwright install chromium
```

## 配置说明

### LLM 配置（必需）

优先级：环境变量 > 文件首行。

用途：仅用于选链与地名归属推断，不参与字段抽取。

- `LLM_API_KEY`：OpenAI 兼容 API Key
- `LLM_BASE_URL`：API Base URL（可选）
- `LLM_MODEL`：模型名（可选），默认 `gpt-5.1-codex-mini`
- `LLM_REASONING_EFFORT`：推理强度（可选），默认 `high`
- `docs/llm_key.txt`：备用 Key 文件（首行）
- `WEB_AGENT_LLM_KEY_FILE`：自定义 Key 文件路径（覆盖默认）

### Snov 扩展配置（可选）

- `python -m web_agent snov-login`：启动专用 Chrome 进行登录
- `python -m web_agent snov-export`：导出扩展 cookies

相关文件：

- `output/snov_extension_cookies.json`：登录导出的 cookies（主用）
- `output/snov_profile/`：Chrome 账号 Profile 目录（用于复用登录）

可用环境变量：

- `SNOV_EXTENSION_COOKIE_FILE`：自定义 cookies 文件路径
- `SNOV_PROFILE_DIR`：自定义 profile 目录

### Firecrawl 配置（必需）

- `output/firecrawl_keys.txt`：Firecrawl API key 列表（每行一个）
- `FIRECRAWL_KEYS_PATH`：自定义 key 文件路径
- `FIRECRAWL_BASE_URL`：自定义 Firecrawl API 地址（可选）
- `FIRECRAWL_EXTRACT`：是否启用 /extract（默认 true）
- `FIRECRAWL_EXTRACT_MAX_URLS`：单站点最多提交到 /extract 的 URL 数（默认 6）
- `FIRECRAWL_KEY_PER_LIMIT`：单 key 并发上限（免费版建议 2）
- `FIRECRAWL_KEY_WAIT_SECONDS`：key 冷却等待秒数（默认 900）

## 常用命令

```powershell
# 全量跑
python -m web_agent 东京都

# 续跑模式：失败 / 半成 / 代表人
python -m web_agent 东京都 失败
python -m web_agent 东京都 半成
python -m web_agent 东京都 代表人

# 单独运行模块
python -m gmap_agent --query "株式会社"
python -m site_agent --input docs\websites.csv
python -m corp_agent --prefecture "东京都"
```

## 运行模式说明

详见：`docs/RUN_MODES.md`

## 更新记录

- 2026-01-17：过滤脱敏邮箱并不落盘；Snov 仅脱敏时进入延迟重试；失败/异常结果补齐公司名与代表人占位，便于晚到邮箱升级。
- 2026-01-17：修复 `site_agent` 规则抽取缺失函数 `_apply_heuristic_extraction`，避免运行时 NameError。
- 2026-01-17：修复邮箱规则抽取函数错位、Snov 合并函数缺失及手机号/代表人噪音过滤。

## 目录结构（必须维护）

> 约定：每次改代码或调整目录结构，必须同步更新本 README。

### 根目录

- `AGENTS.md`：运行/维护约束与操作说明。
- `README.md`：项目说明（本文件）。
- `requirements.txt`：Python 依赖清单。
- `product.py`：按天交付增量 CSV 的汇总脚本（按“网站”去重；要求公司名称+代表人+邮箱）。
- `docs/`：文档与地区配置。
- `output/`：运行产出与缓存（动态生成）。
- `snov_ext/`：Snov 扩展源码目录。
- `src/`：核心源码。
- `test/`：测试用例。
- `backup/`：历史备份（人工维护）。
- `.pytest_cache/`：pytest 缓存（运行生成）。
- `.gitignore`：Git 忽略规则。

### docs/

- `docs/日本.txt`：全国行政区划与城市清单（原始总表）。
- `docs/llm_key.txt`：LLM Key 备用文件（首行，敏感信息）。
- `docs/RUN_MODES.md`：运行模式与策略说明。
- `docs/prefectures/`：各都道府县城市清单（由 `日本.txt` 拆分）。
  - `docs/prefectures/三重县.txt`
  - `docs/prefectures/东京都.txt`
  - `docs/prefectures/京都府.txt`
  - `docs/prefectures/佐贺县.txt`
  - `docs/prefectures/兵库县.txt`
  - `docs/prefectures/冈山县.txt`
  - `docs/prefectures/冲绳县.txt`
  - `docs/prefectures/北海道.txt`
  - `docs/prefectures/千叶县.txt`
  - `docs/prefectures/和歌山县.txt`
  - `docs/prefectures/埼玉县.txt`
  - `docs/prefectures/大分县.txt`
  - `docs/prefectures/大阪府.txt`
  - `docs/prefectures/奈良县.txt`
  - `docs/prefectures/宫城县.txt`
  - `docs/prefectures/宫崎县.txt`
  - `docs/prefectures/富山县.txt`
  - `docs/prefectures/山口县.txt`
  - `docs/prefectures/山形县.txt`
  - `docs/prefectures/山梨县.txt`
  - `docs/prefectures/岐阜县.txt`
  - `docs/prefectures/岛根县.txt`
  - `docs/prefectures/岩手县.txt`
  - `docs/prefectures/广岛县.txt`
  - `docs/prefectures/德岛县.txt`
  - `docs/prefectures/新潟县.txt`
  - `docs/prefectures/栃木县.txt`
  - `docs/prefectures/滋贺县.txt`
  - `docs/prefectures/熊本县.txt`
  - `docs/prefectures/爱媛县.txt`
  - `docs/prefectures/爱知县.txt`
  - `docs/prefectures/石川县.txt`
  - `docs/prefectures/神奈川县.txt`
  - `docs/prefectures/福井县.txt`
  - `docs/prefectures/福冈县.txt`
  - `docs/prefectures/福岛县.txt`
  - `docs/prefectures/秋田县.txt`
  - `docs/prefectures/群马县.txt`
  - `docs/prefectures/茨城县.txt`
  - `docs/prefectures/长崎县.txt`
  - `docs/prefectures/长野县.txt`
  - `docs/prefectures/青森县.txt`
  - `docs/prefectures/静冈县.txt`
  - `docs/prefectures/香川县.txt`
  - `docs/prefectures/高知县.txt`
  - `docs/prefectures/鸟取县.txt`
  - `docs/prefectures/鹿儿岛县.txt`

### output/（运行产物目录）

- `output/cache/`：域名邮箱/法人名称缓存。
- `output/hojin_cache/`：法人番号名录 ZIP 缓存。
- `output/web_jobs/`：每次任务运行目录（包含 job.json/job.log/registry/site 子目录）。
- `output/delivery/`：交付产物（由 `product.py` 生成）。
- `output/snov_extension_cookies.json`：Snov 扩展 cookies（登录导出）。
- `output/snov_profile/`：Snov 登录专用 Chrome Profile。

### snov_ext/

- Snov 扩展源码（已解压）。

### src/

#### src/gmap_agent/

- `cli.py`：命令行入口。
- `config.py`：配置结构。
- `crawler.py`：请求与抓取逻辑。
- `http_client.py`：HTTP 请求与重试策略。
- `models.py`：数据模型。
- `output_writer.py`：输出写入。
- `parsing.py`：解析逻辑。
- `pb_capture.py`：PB 参数捕获。
- `query.py`：查询构建。
- `utils.py`：辅助工具。
- `__init__.py` / `__main__.py`：模块入口。

#### src/site_agent/

- `cli.py`：命令行入口。
- `config.py`：Pipeline 设置与运行策略。
- `constants.py`：常量与规则表。
- `crawler.py`：Firecrawl 抓取与链接解析封装。
- `email_rules.py`：邮箱规则与过滤。
- `errors.py`：自定义异常。
- `heuristics.py`：规则/清洗逻辑。
- `input_loader.py`：输入加载。
- `llm_client.py`：LLM 调用与 Prompt 处理。
- `models.py`：数据结构。
- `output_writer.py`：输出写入。
- `pipeline.py`：主流程控制。
- `prompt_loader.py`：Prompt 加载。
- `snov_client.py`：Snov 扩展调用封装。
- `utils.py`：通用工具。
- `prompts/`：Prompt 模板。
- `__init__.py` / `__main__.py`：模块入口。

#### src/hojin_agent/

- `cli.py`：命令行入口。
- `nta_zenken.py`：国税厅名录下载与解析。
- `pipeline.py`：法人名录处理流程。
- `prefectures.py`：都道府县规范化。
- `__init__.py` / `__main__.py`：模块入口。

#### src/corp_agent/

- `cli.py`：命令行入口。
- `loader.py`：输入加载。
- `models.py`：数据结构。
- `output_writer.py`：输出写入。
- `__init__.py` / `__main__.py`：模块入口。

#### src/web_agent/

- `cli.py`：一键入口（仅 CLI）。
- `runner.py`：调度与并发执行。
- `service.py`：核心业务逻辑与流程控制。
- `store.py`：任务存储。
- `pb_capture.py`：PB 参数记录。
- `keyword_store.py`：关键词管理。
- `city_pref_llm.py`：城市/都道府县归属推断。
- `__init__.py` / `__main__.py`：模块入口。

### test/

- `test_cache.py`：缓存逻辑测试。
- `test_errors.py`：异常处理测试。
- `test_gmap_query.py`：Google Maps 查询测试。
- `test_heuristics.py`：规则/清洗测试。
- `test_strategy.py`：运行策略测试。
