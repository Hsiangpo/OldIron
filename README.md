# OldIron 多国公司信息采集项目

## 这是什么

`OldIron` 是一个面向海外企业信息采集与交付的多国爬虫仓库。  
目标不是做单站点脚本，而是持续扩展成一套可横向复制的采集体系：

- 一个国家可以接多个站点
- 一个站点可以拆多阶段流水线
- 不同国家可以复用同一类能力（官网补齐、联系方式提取、增量交付等）

当前仓库覆盖英国、丹麦、芬兰、德国、意大利、日本、台湾、巴西、美国、阿联酋共 10 个国家方向。

## 当前开发口径

- 多机协作模型：**不同机器跑不同站点**，按国家维度合并交付。不做同一站点多机分片。
- 双 Codex 并行开发时，统一使用 `coordination/` + GitHub issue / PR 双通道做任务登记、共享区租约锁和交接。
- 邮箱补充路线：已从 `Firecrawl` 迁移到**协议爬虫（curl_cffi）+ LLM**。协议爬虫抓取网页转 Markdown 后，邮箱由规则提取、代表人由 LLM 提取。
- 老旧实现按约定归档到 `<Country>/bak/` 或 `former/`（当前这两类目录均不存在，所有国家都已在新框架上），新开发全部接入新框架。

## 机器分工

| 机器 | 系统 | 用户 | 项目路径 | 角色 |
|------|------|------|---------|------|
| Machine 1 | Windows | Administrator | `E:\Develop\Masterpiece\Spider\Website\OldIron` | 跑 England CompanyName |
| Machine 2 | macOS | Zhuanz1 | `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron` | 主开发机；跑 Denmark Proff + Virk |

说明：

- 上表是默认运行职责，不是永久独占开发锁。
- 实时“谁正在改什么”以 `coordination/active_tasks.json` 和 `coordination/shared_locks.json` 为准。

## 当前国家与站点覆盖

仓库当前覆盖 **10 个国家**。各站点的详细策略以对应的 `<Country>/README.md` 和根 `AGENTS.md` 的 country-specific override 为准。

| 国家 | 活跃站点 | 主链路 | 邮箱/代表人路线 |
|------|---------|--------|---------|
| Denmark | `proff`、`virk` | Proff/Virk → GMap → 协议爬虫+LLM → 合并去重交付 | 站点直出优先，缺邮箱/代表人用协议爬虫+LLM 补强 |
| England | `companyname`、`kompass`、`wiza` | `companyname`：Excel 名单 → GMap → Companies House officers → 规则邮箱；`kompass`/`wiza`：仅抓官网列表 → `websites` per-site 交付 | `companyname` 代表人来自 Companies House、邮箱走规则；`kompass`/`wiza` 只交付官网列表 |
| Finland | `tmt`、`duunitori`、`jobly` | 招聘站（`tmt` 官方 API / `duunitori`、`jobly` SSR）→ 条件触发 GMap → 协议爬虫+LLM → 三站合并去重交付 | 站内联系人字段优先，缺时官网规则邮箱 + LLM 代表人兜底 |
| Germany | `kompass`、`wiza` | 登录态/cookie 抓官网列表 → `websites` per-site 交付，不进详情、不跑 GMap/P2/P3 | 仅交付官网列表 |
| Italy | `dnb`、`wiza` | `dnb`：列表 → Verif 补官网+代表人 → 官网规则邮箱；`wiza`：仅抓官网列表 → `websites` 交付 | `dnb` 代表人来自 Verif、邮箱走官网规则；`wiza` 只交付官网列表 |
| Brazil | `cnpjbiz`、`dnb` | `cnpjbiz`：cnpj.biz 浏览器+代理按州全量抓；`dnb`：DNB 列表/详情 → GMap → 协议爬虫+LLM | per-site 交付 |
| Japan | `bizmaps`、`hellowork`、`mynavi`、`onecareer`、`openwork`、`pasonacareer`、`xlsximport` | 企业库/招聘站/本地 xlsx 导入 → GMap（`hellowork`、`xlsximport` 除外）→ 协议爬虫规则邮箱 + LLM 代表人 | per-site 交付，门禁要求公司名+代表人+邮箱三项齐全 |
| Taiwan | `ieatpe` | 会员协议接口 → 详情接口 → 交付 | 站点直出 |
| UnitedStates | `dnb`、`wiza` | `dnb` 走详情补齐链路；`wiza` 复用登录态抓官网列表 → `websites` 交付，不跑详情/GMap/P2/P3 | per-site 交付 |
| UnitedArabEmirates | `dubaibusinessdirectory`、`hidubai`、`dayofdubai`、`dubaibizdirectory`、`wiza`、`wizasnov` | 目录站走目录页/接口/协议详情 → GMap → 协议爬虫+LLM；`wiza` 走普通三段式；`wizasnov` 走 Snov 域名邮箱 + Snov 人员 + LLM 选关键联系人 | per-site 交付，门禁见 AGENTS override |

## 统一技术路线

1. **主体获取** — 从工商库、黄页、协会名录等入口拿公司主体
2. **详情补齐** — 拉详情页补公司号、地址、电话、代表人、官网
3. **官网发现** — 站内直接给官网最好；缺时走 Google Maps 或目录站补
4. **联系方式提取** — 协议爬虫抓取官网页面；邮箱由规则从完整页面提取，代表人由 LLM 从 HTML→Markdown 内容提取
5. **质量过滤** — 过滤共享域名、占位邮箱、无效官网
6. **增量交付** — 按 `day1/day2/...` 输出每日增量包

## 官网联系方式提取技术细节

当前官网补充链路（协议爬虫规则抽邮箱 + LLM 抽代表人）的工作流程：

1. **站点地图获取** — 用 curl_cffi 抓取目标官网的 sitemap 或首页链接
2. **LLM 选页** — LLM 从所有链接中选出最可能包含联系信息的 8 个页面
3. **页面抓取** — 协议爬虫抓取这 8 个页面的完整 HTML
4. **HTML → Markdown** — BeautifulSoup 清洗无用标签（script/style/img 等），markdownify 转换，压缩率 88-99%
5. **提取** — 邮箱由规则从完整页面内容提取（不走 LLM）；公司名和代表人由 LLM 从 Markdown 内容提取
6. **429 处理** — LLM API 返回 429 时无限排队等待（30-60 秒随机间隔），不算失败

关键参数：
- 单页 Markdown 上限：80,000 字符（超过截断）
- 总 prompt 上限：250,000 字符（代码 `_MAX_PROMPT_CHARS`，低于模型 272k token 限制）
- LLM 并发：默认 8 个 worker，间隔 0.3 秒启动

## England 当前状态

- 站点：`companyname`、`kompass`、`wiza`
- `companyname` 主链路：`Excel → GMap（补官网）→ Companies House officers（补代表人）→ 协议爬虫规则抽邮箱 → delivery`
- `companyname` 代表人来源：Companies House `officers` 页面，只取当前在任，多个名字用分号拼接
- `companyname` 邮箱来源：官网规则提取，不让官网 LLM 抽代表人或邮箱
- `kompass`、`wiza`：只抓官网列表，不进详情、不跑 GMap/P3，走 `websites` per-site 交付
- 运行机器：Windows (Machine 1)

```bash
cd England
python run.py companyname
python run.py kompass list --max-pages 3
python run.py wiza list
```

## Denmark 当前状态

- 站点：`proff`（丹麦最大企业黄页）、`virk`（丹麦官方 CVR 工商库）
- 主链路：`Proff/Virk → GMap → 协议爬虫+LLM → delivery`
- 运行机器：macOS (Machine 2)

```bash
cd Denmark
python run.py proff
python run.py virk
```

交付：
```bash
python product.py Denmark day1
```

## 双 Codex 协作协议

当两台机器上的 Codex 可能同时开发时，仓库本身就是协作面：

- `coordination/active_tasks.json`
  - 记录谁在做什么、准备改哪些文件、GitHub 对应任务是什么
- `coordination/shared_locks.json`
  - 记录哪些高风险共享路径正在被占用
- `coordination/handoffs/`
  - 记录中途暂停、部分完成、阻塞后的交接说明
- GitHub issue / PR
  - 提供给人类看的长期留痕和审计记录

高风险共享区包括：

- `shared/`
- repo-root `product.py`
- repo-root `AGENTS.md`
- repo-root `README.md`
- `.github/`
- `coordination/`
- 任意 `<Country>/shared/`
- 任意 `<Country>/src/*/delivery.py`

任务先分两类：

- `site_local`
  - 只改某个国家/站点自己的代码，不碰共享高风险区
- `shared_zone`
  - 会改 `shared/`、根文档、根 `product.py`、`.github/`、`coordination/`、任意 `delivery.py` 这类共享区

默认流程：

1. `git pull`
2. 读取 `AGENTS.md`
3. 读取 `coordination/active_tasks.json`
4. 读取 `coordination/shared_locks.json`
5. 先判断任务属于 `site_local` 还是 `shared_zone`
6. `site_local`：
   - 先登记任务
   - 创建任务分支
   - 尽早把任务分支推到远端
7. `shared_zone`：
   - 先登记任务
   - 先写租约锁（`expires_at` + `heartbeat_at`）
   - 先把锁推到远端，再改共享区
8. 改完验证后，先同步最新代码并合并
9. 推代码
10. 如果是共享区任务，再释放锁并一起推送
11. 如果工作未完成，写 `coordination/handoffs/` 交接文档

常用命令：

```bash
python coordination/coord_cli.py begin --task-id coord-2026-04-03-example --change-class site_local --machine "Machine 1" --agent codex-windows --base-branch main --working-branch main --scope England/sites/companyname --planned-file England/src/england_crawler/sites/companyname/pipeline.py
python coordination/coord_cli.py begin --task-id coord-2026-04-03-shared --change-class shared_zone --machine "Machine 1" --agent codex-windows --base-branch main --working-branch main --scope AGENTS.md --planned-file AGENTS.md --lock-path AGENTS.md --lease-minutes 20
python coordination/coord_cli.py heartbeat --task-id coord-2026-04-03-shared --lease-minutes 20
python coordination/coord_cli.py finish --task-id coord-2026-04-03-shared --notes "done"
python coordination/coord_cli.py render-issue --task-id coord-2026-04-03-shared
python coordination/coord_cli.py render-pr --task-id coord-2026-04-03-shared
python coordination/preflight.py --change-class shared_zone --scope AGENTS.md --lock-path AGENTS.md
python coordination/lease_doctor.py
```

## 目录约定

```
OldIron/
├── AGENTS.md                    # 全局协作规则
├── README.md                    # 本文件
├── CLAUDE.md                    # 给 Claude Code 的仓库指南
├── coordination/                # 双 Codex 协作状态与交接
├── product.py                   # 统一交付入口
├── shared/oldiron_core/         # 共享 Python 业务核心
│   ├── delivery/                # 共享交付辅助
│   ├── fc_email/                # 共享邮箱/代表人提取
│   ├── google_maps/             # 共享 Google Maps 补齐
│   ├── snov/                    # 共享 Snov 邮箱/人员
│   └── protocol_crawler/        # 协议爬虫模块（curl_cffi）
├── VersatileBackend/            # Go 通用后端（Gmap/Snov/MyIP/Firecrawl 等高并发服务）
├── OldIronCrawler/              # 独立通用官网采集工具（网站名单 → CSV，可打包 exe）
├── Denmark/                     # 丹麦：proff、virk
│   ├── run.py
│   ├── src/denmark_crawler/sites/{proff,virk}/
│   └── output/
├── England/                     # 英国：companyname、kompass、wiza
├── Finland/                     # 芬兰：tmt(目录 tyomarkkinatori)、duunitori、jobly
├── Germany/                     # 德国：kompass、wiza（+ sites/common 国内共享）
├── Italy/                       # 意大利：dnb、wiza
├── Brazil/                      # 巴西：cnpjbiz、dnb
├── Japan/                       # 日本：bizmaps、hellowork、mynavi、onecareer、openwork、pasonacareer、xlsximport
├── Taiwan/                      # 台湾：ieatpe
├── UnitedStates/                # 美国：dnb、wiza
└── UnitedArabEmirates/          # 阿联酋：dubaibusinessdirectory、hidubai、dayofdubai、dubaibizdirectory、wiza、wizasnov（+ sites/common）

每个国家目录结构一致：`run.py` 入口 + `src/<country>_crawler/`（内含 `sites/<site>/` 与 `delivery.py`）+ `output/` + `tests/`。
```

注意：
- 共享能力统一收敛到 `shared/oldiron_core/`，不再使用跨国家符号链接做长期共享。
- `coordination/` 和 `.github/` 里的协作文件属于 Git 管理范围，不走 SSH/scp 代码覆盖同步。
- `.env`、`output/`、API keys 不进 git。

## 常见依赖与凭据

各国家的 `.env` 通常包含：

| 变量 | 说明 |
|------|------|
| `LLM_API_KEY` | LLM 服务的 API 密钥 |
| `LLM_BASE_URL` | LLM 服务的接口地址 |
| `LLM_MODEL` | 使用的模型名称 |
| `CRAWL_BACKEND` | 爬虫后端类型（`protocol` = 协议爬虫） |
| `FIRECRAWL_API_KEYS` | 遗留的 Firecrawl keys（部分国家仍在用） |
| `DNB_CDP_URL` | DNB 美国线读取 9222 浏览器 cookie 的入口 |

原则：凭据按国家隔离；长期续跑的流程必须支持断点恢复。

## 交付原则

- 统一走根目录脚本：`python product.py <Country> dayN`
- 国家内多站点按**公司名去重**后输出
- 交付目录：`<Country>/output/delivery/<Country>_dayNNN/`
- 每天只交付新增，不重复全量
- 同一天重跑交付时，旧的当日交付目录先进入系统回收站/废纸篓，再重建新的当日交付目录
