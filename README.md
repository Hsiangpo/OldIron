# OldIron 多国公司信息采集项目

## 这是什么

`OldIron` 是一个面向海外企业信息采集与交付的多国爬虫仓库。  
目标不是做单站点脚本，而是持续扩展成一套可横向复制的采集体系：

- 一个国家可以接多个站点
- 一个站点可以拆多阶段流水线
- 不同国家可以复用同一类能力
  - 企业名录抓取
  - 官网补齐
  - 联系方式补齐
  - 代表人 / 法人 / 董事抽取
  - 增量交付

当前仓库已经覆盖英国、丹麦、韩国、日本、印尼、马来西亚、泰国、印度等方向，后续还会继续扩国家和扩站点。

## 当前开发口径

从现在开始，这个仓库按下面这套新口径推进：

- 多机协作不再做“同一站点多机分片”。
- 多机协作改为“不同机器跑不同站点”或者“不同机器跑同一国家的不同完整流水线”。
- 老旧实现统一归档到各国家自己的 `bak/`，或者放进根目录 `former/`。
- 新功能、新站点、新重写，全部接入新框架，不再往旧实现上打补丁。

通俗点说：

- `Mac` 跑英国 `dnb`
- 本机跑英国 `companies-house`

这是允许的。

但不再推荐：

- `Mac` 跑英国 `dnb shard-002`
- 本机跑英国 `dnb shard-001`

这种“同站点多机分片”以后不作为主模型。

## 老板视角的交付目标

这个项目最终交付的是“可用的公司线索”，不是原始网页。

常见交付字段包括：

- 公司名
- 注册号 / 公司号
- 国家 / 地区
- 官网域名
- 邮箱
- 电话
- 代表人 / 法人 / 董事
- 地址
- 来源站点

不同国家的数据源能力不一样，所以不是每条记录都能同时补齐所有字段。项目采用“先拿主体，再做富化”的路线，优先保证规模，再逐步提高质量。

## 当前国家与站点覆盖

| 国家       | 当前主站点 / 数据源                                                                 | 主要链路                                      | 当前邮箱路线                                  |
| ---------- | ----------------------------------------------------------------------------------- | --------------------------------------------- | --------------------------------------------- |
| Denmark    | `dnb.com`、`datacvr.virk.dk`、`proff.dk`、`Google Maps`                             | `DNB / Virk / Proff -> GMap / Firecrawl -> delivery` | `Virk / Proff` 直出邮箱优先，缺邮箱时再补强 |
| England    | `dnb.com`、`Companies House`、`Google Maps`                                         | 名录 / 详情 -> 官网 -> 邮箱 -> 交付           | `Firecrawl` 默认主链路                        |
| SouthKorea | `catch`、`incheon`、`dart`、`saramin`、`khia`、`kssba`、`dsnuri`、`gpsc`、`dnb.com` | 列表 / 详情 -> 官网 -> 邮箱 -> 交付           | 以 `Snov` 为主，部分站点配 `Firecrawl` 辅助   |
| Japan      | `Google Maps`、官网抓取、法人数据                                                   | 官网发现 -> 官网抽取 -> 邮箱 / 电话 / 代表人  | `Firecrawl + 规则 + Snov`                     |
| Indonesia  | `gapensi.or.id`、`indonesiayp.com`、`AHU`                                           | 列表 -> 详情 -> 法人 -> 邮箱                  | `Snov` 主链路                                 |
| Malaysia   | `CTOS`、`BusinessList`                                                              | 公司名池 -> 官网 / 管理人 -> 邮箱 -> 交付     | `Snov` 主链路，管理人补齐用 `Firecrawl + LLM` |
| Thailand   | `dnb.com`                                                                           | DNB -> 官网解析 -> 站点抽取 -> 邮箱           | `Snov` 主链路                                 |
| India      | `ZaubaCorp`                                                                         | 列表 -> 详情 -> 联系方式 / 董事               | 站点内字段为主                                |

## 统一技术路线

虽然各国站点不同，但整体套路基本一致：

1. **主体获取**
   - 从工商库、黄页、协会名录、企业目录、招聘站、地图等入口拿公司主体。
2. **详情补齐**
   - 拉详情页或接口，补公司号、地址、电话、代表人、官网等字段。
3. **官网发现**
   - 站内直接给官网最好。
   - 站内没官网时，走 `Google Maps`、目录站、规则匹配或其他辅助路线补官网。
4. **联系方式提取**
   - 老路线以 `Snov` 为主。
   - 新路线逐步切到 `Firecrawl`，结合候选页发现、页面抽取、邮箱去重清洗。
5. **质量过滤**
   - 过滤共享域名、占位邮箱、无效官网、明显错配主体。
6. **增量交付**
   - 按 `day1/day2/...` 输出每日增量包，避免重复交付。

## 新框架运行规则

以后新架构站点统一遵守这几条：

1. **单入口运行**
   - 统一使用 `python run.py <site>`。
   - 不要求用户先手动起共享后端。
2. **站点内部自动管后端**
   - 如果某个站点需要 `Gmap`、`Firecrawl`、`MyIP` 这类共享能力，就由站点自己的 CLI 在内部自动拉起和回收。
   - 手动 `backend start/stop/status` 只保留给调试。
3. **MyIP 不是全站默认**
   - 不是所有站点都适合走轮询住宅 IP。
   - 是否使用 `MyIP`，要根据站点自己的风控情况决定。
4. **旧实现只归档，不继续长代码**
   - 被替换的旧实现放到 `bak/` 或 `former/`。
   - 主路径只保留新实现。

## England 当前状态

England 是目前迭代最频繁、也是最接近“后续模板国”的项目。

- 站点：
  - `dnb`
  - `companies-house`
- 当前主链路：
  - `Companies House / DNB -> Google Maps -> Firecrawl -> delivery`
- 邮箱策略：
  - 默认已经从 `Snov` 切到 `Firecrawl`
  - 保留了部分旧兼容壳，但默认不再走 `Snov`
- 运行入口：
  - [England/run.py](E:/Develop/Masterpiece/Spider/Website/OldIron/England/run.py)
- 主要输出目录：
  - [England/output](E:/Develop/Masterpiece/Spider/Website/OldIron/England/output)
- 历史实现归档：
  - [England/bak](E:/Develop/Masterpiece/Spider/Website/OldIron/England/bak)

常用命令：

```powershell
cd England
python run.py dnb
python run.py companies-house
cd ..
python product.py England day1
python product.py England day2
```

## Denmark 当前状态

Denmark 现在有三条站点主线，并且交付时会按国家维度统一合并。

- 站点：
  - `dnb`
  - `virk`
  - `proff`
- 当前主链路：
  - `DNB -> Google Maps -> Firecrawl -> delivery`
  - `Virk -> 站内直取邮箱 / 代表人 -> 缺邮箱再走 Google Maps -> Firecrawl -> delivery`
  - `Proff -> 站内直取邮箱 / 代表人 / 官网 -> 缺官网走 Google Maps -> 缺邮箱走 Firecrawl -> delivery`
- 交付规则：
  - `product.py dayN` 会先把丹麦所有站点结果合并，再统一去重出包
- 运行入口：
  - [Denmark/run.py](E:/Develop/Masterpiece/Spider/Website/OldIron/Denmark/run.py)
- 主要输出目录：
  - [Denmark/output](E:/Develop/Masterpiece/Spider/Website/OldIron/Denmark/output)
- 历史实现归档：
  - [Denmark/bak](E:/Develop/Masterpiece/Spider/Website/OldIron/Denmark/bak)

常用命令：

```powershell
cd Denmark
python run.py dnb
python run.py virk
python run.py proff
cd ..
python product.py Denmark day1
```

## 其他国家入口一览

### SouthKorea

- 入口：
  - [SouthKorea/run.py](E:/Develop/Masterpiece/Spider/Website/OldIron/SouthKorea/run.py)
- 可跑站点：
  - `catch`
  - `incheon`
  - `dart`
  - `saramin`
  - `khia`
  - `kssba`
  - `dsnuri`
  - `gpsc`
  - `dnb`

### Indonesia

- 入口：
  - [Indonesia/run.py](E:/Develop/Masterpiece/Spider/Website/OldIron/Indonesia/run.py)
- 可跑站点：
  - `gapensi`
  - `indonesiayp`
- 交付入口：
  - `python run.py deliver day1`

### Malaysia

- 入口：
  - [Malaysia/run.py](E:/Develop/Masterpiece/Spider/Website/OldIron/Malaysia/run.py)
- 主流程：
  - `CTOS + BusinessList + ManagerAgent + Snov`

### Thailand

- 入口：
  - [Thailand/src/thailand_crawler/cli.py](E:/Develop/Masterpiece/Spider/Website/OldIron/Thailand/src/thailand_crawler/cli.py)
- 当前主站点：
  - `dnb`

### Japan

- 项目说明：
  - [Japan/README.md](E:/Develop/Masterpiece/Spider/Website/OldIron/Japan/README.md)
- 当前主模块：
  - `gmap_agent`
  - `site_agent`
  - `hojin_agent`
  - `corp_agent`
  - `web_agent`

### India

- 项目说明：
  - [India/README.md](E:/Develop/Masterpiece/Spider/Website/OldIron/India/README.md)
- 当前方向：
  - `ZaubaCorp` Active 公司抓取

## 目录约定

仓库按“国家隔离”与“全局通用”结合的方式组织。

核心目录结构如下：

- `VersatileBackend/`
  - 全局通用后端（使用 Go 语言编写），统一处理高并发的跨国通用能力（如 Firecrawl, Gmap, Snov, MyIP）。
- `<Country>/run.py`
  - 国家级统一启动入口
- `product.py` (根目录)
  - 统一的每日交付脚本
- `shared/oldiron_core/`
  - 新的共享 Python 业务核心，当前已承接 England / Denmark 的统一交付逻辑
- `former/`
  - 还没有迁移到新框架的旧国家或旧模块归档区
- `<Country>/bak/`
  - 该国家自己已经下线的旧源码、旧输出、旧调试残留归档区
- `<Country>/src/`
  - 国家级源码
- `<Country>/docs/`
  - 设计文档、字段说明、计划文档
- `<Country>/output/`
  - 该国家自己的运行产物、缓存、交付目录
- `<Country>/.env`
  - 该国家自己的密钥与配置

注意：

- 当前主流程的产物，原则上都应写到“各国家目录自己的 `output/`”下面。
- 根目录 [output](E:/Develop/Masterpiece/Spider/Website/OldIron/output) 不是 England 等当前主流程的主产物目录，更多是历史残留或临时缓存。
- `bak/` 和 `former/` 里的内容不再作为新开发主路径。

## 常见依赖与凭据

不同国家依赖不完全一样，但常见的配置项有这些：

- `Firecrawl API Keys`
  - 通常放在各国自己的 `output/firecrawl_keys.txt`
- `LLM_API_KEY`
- `LLM_BASE_URL`
- `LLM_MODEL`
- `SNOV_CLIENT_ID`
- `SNOV_CLIENT_SECRET`
- `DNB_COOKIE_HEADER`
- 代理配置
- 浏览器导出的 cookies / profile

原则：

- 凭据按国家隔离，不共用一份 `.env`
- 火力越大的站点，越要保留 `run.log`、checkpoint、SQLite 状态库
- 任何会长期续跑的流程，都必须支持断点恢复

## 交付原则

这个仓库不是“跑完一次就结束”的项目，而是连续生产型项目，所以交付规则很重要。

统一原则：

- 每日的交付文件，统一走根目录脚本，运行命令示例：`python product.py England dayN`
- 每日交付还是国家内多站点按 **公司名去重**。必须先合并该国家所有站点结果，再统一去重和输出。
- 每个国家单独维护自己的 `output/`
- 每天只交付新增，不重复把历史全量再打给业务
- 交付目录优先放在 `<Country>/output/delivery/<Country>_dayNNN/`
- 交付文件尽量同时保留：
  - 明细 CSV
  - 汇总 JSON
  - 运行日志

临时文件规则：

- 根目录和各国家 `output/` 下的冒烟测试、临时 JSON、一次性调试目录，用完就删
- 正在续跑的目录、状态库、日志不要删

## 后续扩展方式

未来新增国家或新增站点，按这套规则走：

1. **先建国家目录**
   - 例如 `Vietnam/`
2. **再定站点矩阵**
   - 工商库
   - 黄页
   - 协会
   - 招聘站
   - 地图
   - 行业目录
3. **优先复用通用能力**
   - 国家级站点爬虫仍然使用 Python
   - 并发型通用后端统一沉到 `VersatileBackend`（Go）
   - 共享但不高并发的业务核心，例如国家级交付、统一去重、历史基线恢复，放在 `shared/oldiron_core/`
   - 不要再在国家目录里复制一整份交付逻辑
   - 新站点统一做成 `python run.py <site>` 单入口，站点内部自动管理自己依赖的共享后端
4. **保持输出口径稳定**
   - 让下游业务看到的字段尽量一致
5. **每加一个站点，都要补文档**
   - 来源站
   - 抓取方式
   - 风控点
   - 断点规则
   - 交付字段

## 当前建议的演进方向

从整个仓库的演进看，后续主路线会越来越清晰：

- 官网发现：继续保留 `Google Maps` 和目录站作为入口
- 邮箱补齐：逐步从 `Snov` 迁移到 `Firecrawl`
- 页面理解：用外部 LLM 做候选页筛选，用 `Firecrawl` 做页面抓取和结构化抽取
- 项目组织：继续按国家隔离、按站点扩展、按交付统一口径；新站点优先进入 `sites/` 这类更清晰的新结构

一句话概括：

`OldIron` 不是单个爬虫，而是一套多国企业信息生产线。
