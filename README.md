# OldIron 多国公司信息采集项目

## 这是什么

`OldIron` 是一个面向海外企业信息采集与交付的多国爬虫仓库。  
目标不是做单站点脚本，而是持续扩展成一套可横向复制的采集体系：

- 一个国家可以接多个站点
- 一个站点可以拆多阶段流水线
- 不同国家可以复用同一类能力（官网补齐、联系方式提取、增量交付等）

当前仓库已经覆盖英国、丹麦、德国、韩国、日本、印尼、马来西亚、泰国、印度，以及新接入的美国、台湾、阿联酋方向。

## 当前开发口径

- 多机协作模型：**不同机器跑不同站点**，按国家维度合并交付。不做同一站点多机分片。
- 双 Codex 并行开发时，统一使用 `coordination/` + GitHub issue / PR 双通道做任务登记、共享区租约锁和交接。
- 邮箱补充路线：已从 `Firecrawl` 迁移到**协议爬虫（curl_cffi）+ LLM**。协议爬虫抓取网页，HTML 转 Markdown 后由 LLM 提取邮箱和代表人。
- 老旧实现统一归档到 `<Country>/bak/` 或 `former/`，新开发全部接入新框架。

## 机器分工

| 机器 | 系统 | 用户 | 项目路径 | 角色 |
|------|------|------|---------|------|
| Machine 1 | Windows | Administrator | `E:\Develop\Masterpiece\Spider\Website\OldIron` | 跑 England CompanyName |
| Machine 2 | macOS | Zhuanz1 | `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron` | 主开发机；跑 Denmark Proff + Virk |

说明：

- 上表是默认运行职责，不是永久独占开发锁。
- 实时“谁正在改什么”以 `coordination/active_tasks.json` 和 `coordination/shared_locks.json` 为准。

## 当前国家与站点覆盖

| 国家 | 活跃站点 | 主链路 | 邮箱路线 |
|------|---------|--------|---------|
| Denmark | `proff`、`virk` | Proff/Virk → GMap → 协议爬虫+LLM → delivery | 站点直出优先，缺邮箱用协议爬虫+LLM 补强 |
| Brazil | `dnb` | DNB 列表 API → 隐藏详情 API → GMap → 协议爬虫+LLM → delivery | DNB 官网层 + 协议爬虫+LLM |
| England | `companyname` | Excel 名单 → GMap → Companies House officers → 规则邮箱提取 → delivery | 代表人来自 Companies House，邮箱走规则提取 |
| Germany | `wiza` | Wiza 登录态协议列表 → 官网协议爬虫+LLM → per-site delivery | 官网邮箱走规则，代表人只来自官网 LLM |
| UnitedStates | `dnb` | DNB API → DNB 详情 → GMap → 协议爬虫+LLM → delivery | DNB 官网层 + 协议爬虫+LLM |
| UnitedArabEmirates | `dubaibusinessdirectory`、`hidubai`、`dayofdubai`、`dubaibizdirectory`、`wiza` | UAE 目录站点仍走目录页/接口/协议详情 → GMap → 协议爬虫+LLM；`wiza` 复用登录态抓列表后，直接走 Snov 域名邮箱 + Snov 人员列表 + LLM 选关键联系人 → per-site delivery | `wiza` 不走官网规则邮箱/官网代表人链路 |
| Taiwan | `ieatpe` | 会员协议接口 → 详情接口 → delivery | 站点直出 |
| SouthKorea | `catch`、`incheon`、`dart` 等 | 列表/详情 → 官网 → 邮箱 → 交付 | Snov 为主 |
| Japan | `bizmaps`、`hellowork`、`xlsximport` | 站点列表/导入 → 官网/邮箱补齐 → delivery | 协议爬虫+LLM + 站点字段 |
| Indonesia | `gapensi`、`indonesiayp` | 列表 → 详情 → 法人 → 邮箱 | Snov 为主 |
| Malaysia | `CTOS`、`BusinessList` | 公司名池 → 官网/管理人 → 邮箱 → 交付 | Snov 为主 |
| Thailand | `dnb` | DNB → 官网解析 → 站点抽取 → 邮箱 | Snov 为主 |
| India | `ZaubaCorp` | 列表 → 详情 → 联系方式/董事 | 站点内字段为主 |

## 统一技术路线

1. **主体获取** — 从工商库、黄页、协会名录等入口拿公司主体
2. **详情补齐** — 拉详情页补公司号、地址、电话、代表人、官网
3. **官网发现** — 站内直接给官网最好；缺时走 Google Maps 或目录站补
4. **联系方式提取** — 协议爬虫抓取官网页面，HTML→Markdown 后由 LLM 提取邮箱和代表人
5. **质量过滤** — 过滤共享域名、占位邮箱、无效官网
6. **增量交付** — 按 `day1/day2/...` 输出每日增量包

## 邮箱提取技术细节

当前邮箱补充链路（协议爬虫 + LLM）的工作流程：

1. **站点地图获取** — 用 curl_cffi 抓取目标官网的 sitemap 或首页链接
2. **LLM 选页** — LLM 从所有链接中选出最可能包含联系信息的 8 个页面
3. **页面抓取** — 协议爬虫抓取这 8 个页面的完整 HTML
4. **HTML → Markdown** — BeautifulSoup 清洗无用标签（script/style/img 等），markdownify 转换，压缩率 88-99%
5. **LLM 提取** — Markdown 内容发给 LLM，提取公司名、代表人、邮箱
6. **429 处理** — LLM API 返回 429 时无限排队等待（30-60 秒随机间隔），不算失败

关键参数：
- 单页 Markdown 上限：80,000 字符（超过截断）
- 总 prompt 上限：750,000 字符（≈250k token，低于模型 272k 限制）
- LLM 并发：默认 8 个 worker，间隔 0.3 秒启动

## England 当前状态

- 站点：`companyname` — 从 Excel 公司名单出发
- 主链路：`Excel → GMap（补官网）→ Companies House officers（补代表人）→ 协议爬虫规则抽邮箱 → delivery`
- 运行机器：Windows (Machine 1)
- 代表人来源：Companies House `officers` 页面，只取当前在任，多个名字用分号拼接
- 邮箱来源：官网规则提取，不再让官网 LLM 抽代表人或邮箱

```bash
cd England
python run.py companyname
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
python coordination/coord_cli.py begin --task-id coord-2026-04-03-example --change-class site_local --machine "Machine 1" --agent codex-windows --base-branch main --working-branch machine1/england/example --scope England/sites/companyname --planned-file England/src/england_crawler/sites/companyname/pipeline.py
python coordination/coord_cli.py begin --task-id coord-2026-04-03-shared --change-class shared_zone --machine "Machine 1" --agent codex-windows --base-branch main --working-branch machine1/shared/example --scope AGENTS.md --planned-file AGENTS.md --lock-path AGENTS.md --lease-minutes 20
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
├── coordination/                # 双 Codex 协作状态与交接
├── product.py                   # 统一交付入口
├── shared/oldiron_core/         # 共享 Python 业务核心
│   ├── delivery/                # 共享交付辅助
│   ├── fc_email/                # 共享邮箱/代表人提取
│   ├── google_maps/             # 共享 Google Maps 补齐
│   └── protocol_crawler/        # 协议爬虫模块（curl_cffi）
├── VersatileBackend/            # Go 通用后端（Gmap 等高并发服务）
├── Denmark/                     # 丹麦项目
│   ├── run.py
│   ├── src/denmark_crawler/
│   │   └── sites/{proff,virk}/
│   └── output/
├── Brazil/                      # 巴西项目
│   ├── run.py
│   ├── src/brazil_crawler/
│   │   └── sites/dnb/
│   └── output/
├── England/                     # 英国项目
│   ├── run.py
│   ├── src/england_crawler/
│   │   └── sites/companyname/
│   └── output/
├── Germany/                     # 德国项目
│   ├── run.py
│   ├── src/germany_crawler/
│   │   └── sites/wiza/
│   └── output/
├── UnitedStates/                # 美国项目
│   ├── run.py
│   ├── src/unitedstates_crawler/
│   │   └── sites/dnb/
│   └── output/
├── UnitedArabEmirates/          # 阿联酋项目
│   ├── run.py
│   ├── src/unitedarabemirates_crawler/
│   │   └── sites/{dubaibusinessdirectory,hidubai,dayofdubai,dubaibizdirectory,wiza}/
│   └── output/
├── Taiwan/                      # 台湾项目
│   ├── run.py
│   ├── src/taiwan_crawler/
│   │   └── sites/ieatpe/
│   └── output/
├── SouthKorea/
├── Japan/
├── Indonesia/
├── Malaysia/
├── Thailand/
├── India/
└── former/                      # 未迁移到新框架的旧模块
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
