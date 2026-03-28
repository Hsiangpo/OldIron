---
name: oldiron-crawler
version: "1.1.0"
description: "专用于 OldIron 多国企业数据采集项目。触发场景：用户说要爬新国家、给已有国家加新站点、导入外部数据(Excel/CSV)、或调整交付逻辑。覆盖站点探索(DevTools+protocol-crawler)、Pipeline实现(站点采集→GMap补全→Protocol+LLM邮箱/代表人提取)、交付打包(product.py)、多机部署全流程。"
---

# oldiron-crawler skill

> OldIron 项目专用 — 全世界爬公司信息的标准化工作流。

**激活公告（强制）**：激活本 skill 时，Agent 必须向用户说：

> "我正在使用 **oldiron-crawler** skill。接下来按流程执行：环境自检 → 需求捕获 → 站点探索/数据分析 → Pipeline 方案对齐 → 代码实现 → 冒烟测试 → Git + 部署 → 交付集成。"

---

# ⛔ 强制规则

## 1. 技术栈

- 爬虫端一律 **Python**，新国家/新站点**禁止引入 Go 后端依赖**
- HTTP 客户端强制 `curl_cffi`（遵循 `protocol-crawler` skill）
- GMap 查询使用 Python `GoogleMapsClient`（`shared/oldiron_core/google_maps/`）
- 邮箱/代表人抽取使用 Python `FirecrawlEmailService`（`shared/oldiron_core/fc_email/`）
- 协议爬虫使用 `shared/oldiron_core/protocol_crawler/`
- 交付引擎使用 `shared/oldiron_core/delivery/`
- 禁止在国家目录内复制或 symlink 共享模块代码

> 如果 `fc_email/` 或 `google_maps/` 尚未迁移至 `shared/oldiron_core/`，当前仍在 `Denmark/src/denmark_crawler/` 下，则先从 Denmark 目录 import。所有新代码的 import 路径应指向 `shared/oldiron_core/`，待迁移完成后无需改动。

> **shared 模块加载方式**：每个国家的 `run.py` 必须在 import 前注入 shared 路径：`sys.path.insert(0, str(ROOT.parent / "shared"))`，之后即可 `from oldiron_core.xxx import ...`。

## 2. 模型与思考深度（强制）

- 本 skill 全流程**必须使用 `claude-opus-4-6` 模型**，并使用 **max 思考深度**
- 禁止使用低能力模型（haiku、sonnet）或低思考深度来执行本 skill 的任何步骤
- 代码实现、审核、探索分析均要求最高推理能力

## 3. Agent Teams 并行工作模式（强制）

> **必须使用 Agent Teams（`TeamCreate` 创建团队 + 多 Agent 协作），而非普通 subagents。** Agent Teams 支持成员间互相通信（`SendMessage`）、共享上下文、协调工作，比普通 subagent 更适合多模块协作场景。

### 写代码阶段

主代理拆解任务后，**必须创建 Agent Team 并行编写不同模块**：

1. `TeamCreate` 创建开发团队
2. 启动多个 team agent，各自负责不同模块：

```
主代理（架构设计 + 接口定义 + 任务分配）
    ├─ Team Agent A → 写 client.py + parser.py
    ├─ Team Agent B → 写 pipeline.py（P1 站点采集）
    ├─ Team Agent C → 写 pipeline2_gmap.py + pipeline3_email.py
    ├─ Team Agent D → 写 store.py
    └─ Team Agent E → 写 cli.py + delivery.py
```

- 主代理先定义好各模块的**接口契约**（函数签名、参数类型、返回值），再分配给 team agents
- Team agents 之间可通过 `SendMessage` 协调接口细节
- 所有 team agents 必须使用 `claude-opus-4-6` 模型（通过 `model: "opus"` 参数指定）

### 代码审核阶段

写完代码后，**必须创建审核 Agent Team 并行审核不同模块**：

```
主代理（汇总审核结果）
    ├─ Review Agent 1 → 审核 client.py / parser.py / pipeline.py
    ├─ Review Agent 2 → 审核 pipeline2_gmap.py / pipeline3_email.py
    ├─ Review Agent 3 → 审核 store.py / cli.py / delivery.py
    └─ Review Agent 4 → 审核整体架构一致性 + CI 门禁
```

- 审核重点：接口对接是否正确、数据流是否通畅、有无遗漏、代码规范
- 审核 agents 必须使用 `claude-opus-4-6` 模型

## 4. 三项齐全门禁（铁律）

交付落盘的每条记录必须**同时具备**：

| 字段 | 要求 |
|------|------|
| `company_name` | 非空，2-150 字符 |
| `representative` | 非空，不含公司后缀（GmbH/Ltd/Inc/Co./Oy 等） |
| `emails` | 至少一个包含 `@` 的有效邮箱 |

缺任何一项 → **不落盘**。

## 5. CSV 字段与顺序（固定，所有国家统一）

```
company_name, representative, emails, website, phone, evidence_url
```

- `emails`：分号分隔多个邮箱，格式 `[email1; email2]`
- `evidence_url`：邮箱/代表人来源页面 URL
- **禁止**在交付 CSV 中添加其他字段（address/industry/capital 等仅保存在 DB 中，不进 CSV）

## 6. 交付模式选择（强制提问）

**不同国家/不同需求有两种交付模式**，必须在写 delivery 代码前问用户：

> "这个国家有多个站点/路线时，交付模式选哪种？
> - **合并模式**：所有站点数据合并去重，输出单个 `companies.csv` + `keys.txt`（如 Denmark/England/Finland）
> - **分站点模式**：各站点数据独立落盘（如 `bizmaps.csv`、`hellowork.csv`），但 day2+ 仍然要排除前一天已有的数据"

**合并模式**输出：
```
{Country}_day001/
├── companies.csv       # 所有站点合并去重的 delta 记录
├── keys.txt            # 截至当日的全量去重 key
└── summary.json
```

**分站点模式**输出：
```
{Country}_day001/
├── {site1}.csv         # 站点1的 delta 记录
├── {site2}.csv         # 站点2的 delta 记录
└── summary.json
```

两种模式都要做增量 delta（day2+ 排除已交付的记录）。

## 7. 邮箱处理规则

- **爬取阶段**：所有邮箱**全部保存**到 SQLite，不做任何过滤
- **交付阶段（product.py）**：根据用户指定的策略过滤
  - 默认：**不过滤**
  - 白名单模式：只保留指定域名后缀（如 gmail/icloud/outlook）
  - 黑名单模式：排除指定域名后缀（如排除 gmail/yahoo 只保留企业邮箱）
- **必须在写 delivery 代码前问用户**：

> "这个国家的邮箱交付策略是什么？默认不过滤。如果需要过滤，是白名单（只保留指定后缀）还是黑名单（排除指定后缀）？具体域名列表是什么？"

## 8. Pipeline 并行架构（强制）

```
P1 (站点采集) ──产出一条→ 立即投入 P2 队列
P2 (GMap补全) ──产出一条→ 立即投入 P3 队列
P3 (Protocol+LLM) ──产出一条→ 入库等待交付
    │                   │                   │
    └── 独立限速/重试   └── 独立限速/重试   └── 独立限速/重试
```

- 各 Pipeline 作为**守护线程**并行运行
- P2/P3 **轮询 DB** 获取新数据（默认间隔 60s）
- P1 完成后，P2/P3 连续 3 轮空闲则自动退出
- 禁止串行（等 P1 全跑完再跑 P2）

## 9. HTML → Markdown → 截断 → LLM（强制流程）

Pipeline 3 将网页投喂 LLM 前，必须严格按以下顺序处理：

### 9.1 单页处理

1. 获取 HTML 原文（`curl_cffi` / `SiteCrawlClient`）
2. `BeautifulSoup` 清除 `script` / `style` / `img` / `svg` / `video` / `audio` / `canvas` / `iframe` / `noscript` 标签（连同标签内容一起删除）
3. `markdownify` 转为 Markdown
4. 合并连续空行（3 行以上合并为 2 行）
5. **单页截断至 80,000 字符**（对称截断：保留前 40K + 后 40K，中间插入省略提示）

### 9.2 总 Prompt 截断

所有页面的 Markdown 拼接后，加上 system prompt 和公司信息，**总 Prompt 截断至 272,000 字符**。超出部分直接截断尾部，不分批发送。

## 10. 代表人提取与二次校验（强制）

### 10.1 LLM 提取规则

LLM 提取代表人时遵循严格的级别限制：

**接受的级别**：CEO / Managing Director / Director / Chairman / Founder / Owner / Partner / President / Vice President / Chief Officer

**拒绝的级别**：Manager / Coordinator / Consultant / Advisor / Employee / Assistant / Secretary / Accountant / Receptionist / Clerk / Officer（无 Chief 前缀）

**关键禁令**：
- 代表人姓名**必须在网页正文中原文出现**
- **绝对禁止从公司名中拆分或猜测人名**（如 "Smith & Johnson Ltd" 不能返回 "Smith"）
- 必须提供 `evidence_quote`（包含该人名的页面原文片段）

### 10.2 二次校验（代码层面强制）

LLM 返回后，代码必须执行硬校验：
- 代表人名字拆成单词，**至少 50% 的单词**必须出现在 `evidence_quote` 中（大小写不敏感）
- 有代表人但**没有 evidence_quote** → 直接丢弃该代表人
- 校验不通过 → 清空 representative 字段，不入库

## 11. Google Maps 协议爬虫

GMap 查询**不是调 Google API**，而是协议爬虫：

- 直接构造 HTTP GET 请求到 `https://www.google.com/search?tbm=map`
- 使用 `curl_cffi` impersonate chrome 模拟浏览器
- 代理走 **SOCKS5**：`socks5h://127.0.0.1:7897`（注意不是 HTTP 代理）
- 解析返回的 protobuf/JSON 数据

**评分机制**：每个搜索结果有评分（0-100）：
- 公司名匹配：完全匹配 100 分，包含关系 70 分，前缀匹配 45 分
- 域名匹配：主域名相同 100 分，前缀匹配 80 分
- 海外惩罚：外国 TLD 或外国电话号码减 40-60 分
- **最低 45 分**才采纳，低于 45 分的结果丢弃

**过滤域名**：社交媒体、百科、点评站等非官网域名必须过滤（根据国家追加当地域名）

**Query 拼接**：`"{company_name} {address_prefix} {Country_English}"`

## 12. Domain Cache 防重复爬取

Pipeline 3 中，同一域名下可能有多家公司（如 apple.com 下有多个子公司）。必须实现 **domain_cache** 机制：

- 第一个请求该域名的公司获得处理权（`claimed`），实际执行爬取 + LLM
- 后续请求同域名的公司等待结果（`wait`），轮询直到完成
- 完成后所有同域名公司**共享缓存结果**
- 缓存存储在独立的 SQLite 数据库中
- **无邮箱结果 → 标记 `done`，不重试**

## 13. LLM API 429 无限排队

LLM API 遇到 429 限流时的处理策略：
- **不消耗重试次数**
- 等待 30-60 秒的随机时间
- 然后重新尝试
- 无限排队直到成功

其他可重试错误（网络、超时、5xx）：
- 指数退避：2s → 4s → 8s → 16s → 32s
- 最多 5 次重试

## 14. 断点续跑（必须，但实现灵活）

断点续跑是**必须功能**，但具体实现方式根据站点特性灵活选择：

- **页码型**：适合翻页类站点（保存 segment + last_page + 翻页 token）
- **队列型**：适合多阶段任务（search_tasks/gmap_queue/email_queue 表，status + next_run_at）
- **状态型**：适合简单流程（companies 表的 gmap_status/email_status 字段）

**共通要求**：
- 程序崩溃重启后，能从断点继续，不重复已完成的工作
- SQLite 使用 WAL 模式 + `PRAGMA synchronous=NORMAL`
- 每线程独立 DB 连接（`threading.local()`）或共享连接 + `timeout=30`
- 建议实现 stale recovery：`running` 状态超时（如 300-900 秒）自动回退为 `pending`

## 15. 全量采集是首要目标

Agent 在设计 P1 时，必须**自主研究并实现**能覆盖站点全量数据的采集策略：
- 分片（按地区/行业/分类/字母等维度穷举）
- 大类包小类（递归子分类）
- 翻页（遍历所有页码）
- 多关键词搜索（覆盖搜索盲区）
- 突破单次搜索结果上限（通过细化分片条件）

**不需要和用户讨论全量策略**，Agent 自己研究分析后直接实现。但需在探索报告中说明策略。

## 16. 代码完成后的 Git + 部署规则

- 代码写完并通过冒烟测试后，**必须 git commit + push**
- 如果代码在远程机器运行：
  1. SSH 进入远程机器
  2. `git pull` 拉取最新代码
  3. 安装依赖（`pip install -r requirements.txt`）
  4. 把**爬取命令**和**交付命令**发给用户，方便用户执行

---

# 环境自检（激活 skill 后首先执行）

## 1. 机器识别

读取 hostname + OS，确认当前机器身份：

| 机器 | OS | 用户名 | 项目路径 | 角色 |
|------|------|--------|----------|------|
| Machine 2 (主) | macOS | Zhuanz1 | `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron` | 开发机 |
| Machine 1 (副) | Windows | Administrator | `E:\Develop\Masterpiece\Spider\Website\OldIron` | 运行机，IP: 192.168.0.102，密码: deadman |

识别后告知用户当前机器。

> 如果以后新增机器，用户会在 `AGENTS.md` 的 Machines 章节注册，以该文件为准。

## 2. 运行目标确认（强制提问）

> "当前在 Machine N（{OS}）。这个国家/站点的代码是在**本机**跑，还是部署到**另一台机器**跑？"

记住用户的回答，影响后续部署流程。

## 3. 依赖检查

- `protocol-crawler` skill 是否可用（站点探索依赖它）
- Chrome DevTools MCP 是否已配置 `127.0.0.1:9222`（站点探索依赖它）
- `.env` 文件是否存在，关键环境变量是否配置：
  - `LLM_API_KEY` / `LLM_BASE_URL` / `LLM_MODEL`
  - `HTTP_PROXY`（默认 `http://127.0.0.1:7897`，用于非中国外网访问）

缺失则按 `protocol-crawler` skill 的凭据闭环流程处理。

---

# 触发与流程决策

```
用户输入
    │
    ├─ "我要爬 XX 国家" / "给 XX 加个站点" + 甩一个网址
    │   └─ → Flow A: 站点探索路线
    │
    ├─ "我要爬 XX 国家" / "给 XX 加个站点" + 甩一份 Excel/CSV 文件
    │   └─ → Flow B: 数据导入路线
    │
    ├─ "我要爬 XX 国家" + 没给具体信息
    │   └─ 先问："数据来源是一个网站，还是一份现有的数据文件（Excel/CSV）？"
    │
    └─ 调整交付逻辑 / product.py 问题
        └─ 直接处理，参考「交付标准」章节
```

---

# Flow A: 站点探索路线（新国家 / 新站点）

## Step 1: 需求捕获

明确以下信息（不够就问）：

- 目标国家名称（英文，用于目录命名）
- 目标站点 URL
- 是**新国家**（需创建国家目录）还是**已有国家加新站点**
- 代码在哪台机器运行（环境自检已问过）

## Step 2: DevTools 站点探索

**激活 `protocol-crawler` skill**，使用 Chrome DevTools MCP 探索目标站点。

### 2.1 数据字段摸底

浏览站点的搜索结果页、列表页、详情页，逐一确认以下字段的可获取性：

| 字段 | 优先级 | 看什么 |
|------|--------|--------|
| company_name | ★★★ 必须 | 列表页/详情页是否显示公司名 |
| representative | ★★★ 必须 | 详情页是否有代表人/CEO/法人 |
| emails | ★★★ 必须 | 详情页是否有邮箱 |
| phone | ★★ 有就要 | 列表页或详情页是否有电话 |
| website | ★★ 有就要 | 详情页是否有官网链接 |
| address | ★ 辅助 | GMap 查询时作为定位辅助 |

### 2.2 数据覆盖率分析

当站点**直接提供**代表人或邮箱时，必须做覆盖率分析：

1. 采样至少 **3-5 页**（跨不同分类/地区）
2. 每页统计：有代表人的记录数 / 总记录数 = 代表人覆盖率
3. 每页统计：有邮箱的记录数 / 总记录数 = 邮箱覆盖率
4. 估算全站覆盖率

覆盖率影响 Pipeline 设计：
- 代表人覆盖率 > 80% → P1 直接获取，P3 只补漏
- 代表人覆盖率 < 30% → P3 为主力，P1 获取的代表人作为参考
- 邮箱同理

### 2.3 全量采集路线分析

**Agent 自主分析，不需要和用户讨论：**

1. **总数据量**：站点共有多少家公司？（搜索结果总数、分类页统计等）
2. **分片维度**：
   - 按地区？（国家行政区划：省/县/市/区）
   - 按行业/分类？（大类 → 小类 → 子类？）
   - 按公司名首字母/拼音？
   - 按注册时间/规模/状态？
3. **单次搜索上限**：一次搜索最多返回多少条？有无硬上限？
   - 如果有上限（如 10000 条），必须细化分片让每个分片 < 上限
4. **翻页机制**：
   - offset 翻页？cursor 翻页？token 翻页？
   - 最大页数限制？
   - 翻页参数有无加密/签名？
5. **反爬情况**：
   - 限流策略？429 返回？
   - 是否需要登录？
   - IP 封禁？验证码？
   - TLS 指纹检测？

### 2.4 接口抓包分析

遵循 `protocol-crawler` skill 步骤 3 的完整流程：

1. `take_snapshot` → `navigate_page` → `wait_for` → 触发操作
2. `list_network_requests`（filter: fetch/xhr）
3. `get_network_request` 逐个分析关键请求
4. 至少抓 **2-3 次翻页**，对比分页参数递进规律
5. 区分动态字段（token/签名/时间戳）与静态字段

## Step 3: 结构化探索报告（强制输出）

探索完成后，**必须**向用户输出以下格式的报告，等待用户确认：

```
【站点探索报告：{站点名} ({URL})】

1. 数据字段可获取性
   ┌─────────────────┬──────┬────────────────────────┐
   │ 字段            │ 状态 │ 说明                   │
   ├─────────────────┼──────┼────────────────────────┤
   │ company_name    │ ✅   │ 列表页直接获取         │
   │ representative  │ ❌   │ 站点不提供 → 需 P3 LLM │
   │ emails          │ ❌   │ 站点不提供 → 需 P3 LLM │
   │ phone           │ ✅   │ 详情页获取             │
   │ website         │ ✅   │ 详情页获取，覆盖率~60% │
   │ address         │ ✅   │ 列表页直接获取         │
   └─────────────────┴──────┴────────────────────────┘

2. 数据覆盖率（采样 X 页，共 Y 条记录）
   - 代表人覆盖率: 0%（站点不提供）
   - 邮箱覆盖率: 0%（站点不提供）
   - 官网覆盖率: ~60%
   - 电话覆盖率: ~85%

3. 全量采集策略
   - 站点总公司数: ~XXX,XXX 家
   - 分片维度: 按 {维度} 分，共 N 个分片
   - 单次搜索上限: X 条
   - 翻页机制: {offset/cursor/token}
   - 采集策略: {具体描述如何穷举全量}

4. 反爬情况
   - 限流: {描述}
   - 验证码: {有/无}
   - 其他: {描述}

5. 接口详情
   - Endpoint: {URL}
   - Method: {GET/POST}
   - 关键 Headers: {列举}
   - 关键参数: {列举}
   - 分页参数: {描述递进规律}
   - 响应结构: {JSON Schema 概要}

6. Pipeline 方案建议
   - P1（站点采集）: 获取 {company_name, phone, website, address, ...}
   - P2（GMap 补全）: 补 {website}（P1 已有 website 的跳过）+ 补 {phone}（P1 已有的跳过）
   - P3（Protocol+LLM）: 补 {representative, emails}
   - 预计并发数: P1={N}, P2={N}, P3={N}
```

**等待用户确认后再进入 Step 4。**

## Step 4: 代码实现

**使用 Agent Teams 并行编写（见强制规则 §3）。**

### 4.1 新国家目录结构

```
{Country}/
├── run.py                              # 入口: python run.py {site_name}
├── .env                                # 环境变量（不提交 git）
├── requirements.txt
├── src/{country}_crawler/
│   ├── __init__.py
│   ├── delivery.py                     # 调用 shared delivery engine
│   └── sites/
│       └── {site_name}/
│           ├── __init__.py
│           ├── cli.py                  # CLI 入口 + Pipeline 线程编排
│           ├── client.py               # HTTP 请求（curl_cffi）
│           ├── parser.py               # HTML/JSON 解析（如需要）
│           ├── pipeline.py             # P1: 站点采集
│           ├── pipeline2_gmap.py       # P2: GMap 补全
│           ├── pipeline3_email.py      # P3: Protocol+LLM 提取
│           └── store.py                # SQLite 存储层
├── output/
│   ├── {site_name}/                    # 站点运行产物（DB、JSONL）
│   └── delivery/                       # 交付输出
│       └── {Country}_day001/
├── tests/
│   └── test_{site_name}.py
```

已有国家加新站点：只需在 `sites/` 下新建站点目录，复用国家级的 `delivery.py` 和 `run.py`。

### 4.2 run.py 模式

```python
"""国家入口：python run.py {site_name} [options]"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# 注入 shared 模块路径
sys.path.insert(0, str(ROOT.parent / "shared"))
# 注入国家 src 路径
sys.path.insert(0, str(ROOT / "src"))

def main():
    site = sys.argv[1].strip().lower()
    rest = sys.argv[2:]
    if site == "{site_name}":
        from {country}_crawler.sites.{site_name}.cli import run_{site_name}
        run_{site_name}(rest)
    else:
        print(f"未知站点: {site}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### 4.3 cli.py — Pipeline 并行编排模式

```python
"""CLI 入口 + Pipeline 线程编排"""
import argparse
import threading
import logging

from .pipeline import run_pipeline1
from .pipeline2_gmap import run_pipeline2_gmap
from .pipeline3_email import run_pipeline3_email
from .store import Store

log = logging.getLogger(__name__)

def run_{site_name}(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", default="http://127.0.0.1:7897")
    parser.add_argument("--p1-workers", type=int, default=1)
    parser.add_argument("--p2-workers", type=int, default=16)
    parser.add_argument("--p3-workers", type=int, default=32)
    args = parser.parse_args(argv)

    store = Store(db_path="output/{site_name}/{site_name}_store.db")
    p1_done = threading.Event()

    # 三条 Pipeline 并行启动
    threads = [
        threading.Thread(
            target=run_pipeline1,
            args=(store, args, p1_done),
            daemon=True, name="P1-Site",
        ),
        threading.Thread(
            target=run_pipeline2_gmap,
            args=(store, args, p1_done),
            daemon=True, name="P2-GMap",
        ),
        threading.Thread(
            target=run_pipeline3_email,
            args=(store, args, p1_done),
            daemon=True, name="P3-Email",
        ),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("所有 Pipeline 已完成")
```

### 4.4 P3 关键实现要点

Pipeline 3 除了基本的 LLM 提取流程外，必须实现以下机制：

**代表人二次校验**（见强制规则 §10）：
- LLM 返回后，代码层面验证 representative 的 50% 单词是否出现在 evidence_quote 中
- 校验不通过则清空 representative

**Domain Cache**（见强制规则 §12）：
- 首次爬取某域名 → 标记 claimed → 执行爬取 → 标记 done + 缓存结果
- 后续同域名公司 → 直接使用缓存结果
- 无邮箱结果 → 标记 done，不重试

**LLM 429 无限排队**（见强制规则 §13）：
- 429 不消耗重试次数，30-60 秒随机等待后重试

## Step 5: 冒烟测试 + 数据校验（强制）

代码完成后，**必须实际运行**验证。禁止"代码写好了你试试"。

**使用 Agent Teams 并行审核（见强制规则 §3）。**

### 5.1 冒烟测试

1. 运行 P1：至少爬 **2-3 页**，确认数据正确入库
2. 运行 P2：确认 GMap 查询返回有效 website
3. 运行 P3：确认 LLM 提取返回有效 emails/representative
4. 全流程跑通：确认 SQLite 中有三项齐全的完整记录

### 5.2 数据校验

| 校验项 | 规则 |
|--------|------|
| company_name | 非空，2-150 字符，无 HTML 残留 |
| representative | 非空，不含公司后缀，evidence_quote 校验通过 |
| emails | 包含 `@`，域名部分 ≥ 3 字符 |
| phone | 只含数字 + 合理分隔符（空格/+/-/括号） |
| website | 以 http(s):// 开头的有效 URL |
| evidence_url | 以 http(s):// 开头的有效 URL |
| 无脏数据 | 无零宽字符、无 HTML entity 残留（`&amp;` 等） |

### 5.3 冒烟测试未通过

- 分析失败原因，修复代码，重新运行
- **冒烟测试未通过禁止进入 Git + 部署步骤**

## Step 6: CI 门禁（继承 protocol-crawler）

| # | 检查项 | 标准 |
|---|--------|------|
| 1 | 单文件行数 | ≤ 1000 行 |
| 2 | 单函数行数 | ≤ 200 行 |
| 3 | 文件命名 | 禁止 `_v2` / `_old` 等版本后缀 |
| 4 | 废弃代码 | 同功能只保留一份 |
| 5 | 注释语言 | 中文 |
| 6 | 文件编码 | UTF-8 |
| 7 | 临时文件 | 已清理或在 `tmp/` 下 |
| 8 | .env | 凭据在 `.env` 中，已加 `.gitignore` |
| 9 | 单目录文件数 | ≤ 10 个文件，超出按领域拆子目录 |

## Step 7: Git + 部署

### 7.1 Git 推送

```bash
cd /path/to/OldIron
git add {Country}/ shared/ product.py
git commit -m "feat({country}): add {site_name} crawler"
git push
```

### 7.2 本机运行

直接提供命令给用户：

```
爬取: cd {Country} && python run.py {site_name}
交付: python product.py {Country} day{N}
```

### 7.3 远程机器运行

```bash
# 1. SSH 进入远程机器
ssh Administrator@192.168.0.102    # 密码: deadman

# 2. 拉取最新代码
cd E:\Develop\Masterpiece\Spider\Website\OldIron
git pull

# 3. 安装依赖
cd {Country}
python -m pip install -r requirements.txt

# 4. 提供命令给用户
爬取: cd {Country} && python run.py {site_name}
交付: python product.py {Country} day{N}
```

## Step 8: 交付集成

确认国家的 `delivery.py` 能被 `product.py` 正确调用。

`product.py` 动态导入机制：
- 查找 `COUNTRY_BUILDERS` 字典中的国家名 → 对应模块名
- `sys.path.insert(0, "{Country}/src")` → `importlib.import_module(module_name)`
- 调用 `build_delivery_bundle(data_root, delivery_root, day_label)`
- **新国家必须在 `product.py` 的 `COUNTRY_BUILDERS` 中注册**

---

# Flow B: 数据导入路线（Excel/CSV）

当用户提供的不是站点 URL，而是数据文件时。流程更简单，但仍遵循相同架构。

## Step 1: 数据源分析

读取文件，确认有哪些列：

| 文件已有字段 | 需要的 Pipeline |
|-------------|----------------|
| 只有 company_name | P2(GMap 找 website+phone) → P3(LLM 找 email+representative) |
| company_name + website | 跳过 P2 → P3(LLM 找 email+representative) |
| company_name + emails | P2(GMap 找 website) → P3(LLM 找 representative) |
| company_name + website + emails | P3(LLM 只找 representative) |
| website + emails（无公司名） | P3(LLM 找 company_name+representative) |

## Step 2: 实现

使用 **xlsximport 模式**（参考 Japan/xlsximport）：

1. 读取 Excel/CSV → 导入 SQLite（UNIQUE 约束防重复）
2. 根据缺失字段启动对应 Pipeline
3. 遵循相同的并行架构（守护线程 + 轮询队列）

站点目录命名建议：`{source_name}import`（如 `xlsximport`、`csvimport`）

## Step 3: 后续

冒烟测试、CI 门禁、Git + 部署、交付集成 — 与 Flow A 完全相同。

---

# 交付标准速查

| 项目 | 规则 |
|------|------|
| CSV 字段顺序 | `company_name, representative, emails, website, phone, evidence_url` |
| 三项齐全门禁 | company_name + representative + emails 全非空才落盘 |
| 邮箱过滤 | 爬取阶段不过滤；交付阶段按用户配置（默认不过滤） |
| 交付模式 | 合并模式（单 CSV）或分站点模式（多 CSV）— 问用户 |
| 去重 | 按 normalized company_name 去重，同名取最佳记录 |
| 增量交付 | 两种模式都做 delta（day2+ 排除前一天已有的数据） |
| 交付目录 | `{Country}/output/delivery/{Country}_day{NNN}/` |
| 交付命令 | `python product.py {Country} day{N}`（从项目根目录执行） |
| GMap 代理 | SOCKS5: `socks5h://127.0.0.1:7897` |
| HTTP 代理 | HTTP: `http://127.0.0.1:7897` |

---

# 常见问题速查

| 问题 | 回答 |
|------|------|
| P1 已有 website → P2 怎么处理？ | 跳过 P2，直接标记 gmap_status='done'，投入 P3 队列 |
| P1 已有 emails + representative → P3 怎么处理？ | 跳过 P3，标记 email_status='done' |
| LLM 只缺 representative 不缺 emails → P3 怎么提取？ | LLM prompt 中只要求提取 representative，不重复提取 emails |
| P2 的 GMap 查不到 website → 该公司怎么办？ | P3 无法执行（无官网可爬），标记 email_status='skipped'，不进入交付 |
| P3 爬官网但没找到邮箱 → 重试吗？ | 不重试，直接标记 email_status='done'，该公司不进入交付 |
| 同域名多家公司怎么处理？ | domain_cache 机制：第一个爬取，后续共享缓存结果 |
| LLM 返回的代表人可信吗？ | 必须通过二次校验（evidence_quote 50% 单词匹配），不通过则丢弃 |
| 站点有详情页但需要额外请求 → 怎么算？ | 仍属于 P1 的范畴，P1 负责获取站点能给的所有数据 |
| 代码在哪台机器跑？ | 环境自检阶段已问过用户，按用户回答执行 |
| 新增机器怎么办？ | 用户需在 AGENTS.md Machines 章节注册，skill 读取该文件获取机器信息 |
| 用什么模型？ | 必须 claude-opus-4-6 + max 思考深度，禁止低能力模型 |
