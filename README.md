# OldIron 多国公司信息采集项目

## 这是什么

`OldIron` 是一个面向海外企业信息采集与交付的多国爬虫仓库。  
目标不是做单站点脚本，而是持续扩展成一套可横向复制的采集体系：

- 一个国家可以接多个站点
- 一个站点可以拆多阶段流水线
- 不同国家可以复用同一类能力（官网补齐、联系方式提取、增量交付等）

当前仓库已经覆盖英国、丹麦、韩国、日本、印尼、马来西亚、泰国、印度等方向。

## 当前开发口径

- 多机协作模型：**不同机器跑不同站点**，按国家维度合并交付。不做同一站点多机分片。
- 邮箱补充路线：已从 `Firecrawl` 迁移到**协议爬虫（curl_cffi）+ LLM**。协议爬虫抓取网页，HTML 转 Markdown 后由 LLM 提取邮箱和代表人。
- 老旧实现统一归档到 `<Country>/bak/` 或 `former/`，新开发全部接入新框架。

## 机器分工

| 机器 | 系统 | 用户 | 项目路径 | 角色 |
|------|------|------|---------|------|
| Machine 1 | Windows | Administrator | `E:\Develop\Masterpiece\Spider\Website\OldIron` | 跑 England CompanyName |
| Machine 2 | macOS | Zhuanz1 | `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron` | 主开发机；跑 Denmark Proff + Virk |

## 当前国家与站点覆盖

| 国家 | 活跃站点 | 主链路 | 邮箱路线 |
|------|---------|--------|---------|
| Denmark | `proff`、`virk` | Proff/Virk → GMap → 协议爬虫+LLM → delivery | 站点直出优先，缺邮箱用协议爬虫+LLM 补强 |
| England | `companyname` | Excel 名单 → GMap → 协议爬虫+LLM → delivery | 协议爬虫+LLM |
| SouthKorea | `catch`、`incheon`、`dart` 等 | 列表/详情 → 官网 → 邮箱 → 交付 | Snov 为主 |
| Japan | `gmap_agent`、`site_agent` 等 | 官网发现 → 抽取 → 邮箱/电话/代表人 | Firecrawl + 规则 + Snov |
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
- 主链路：`Excel → GMap（补官网）→ 协议爬虫+LLM（补邮箱）→ delivery`
- 运行机器：Windows (Machine 1)

```bash
cd England
.venv\Scripts\python run.py companyname
```

## Denmark 当前状态

- 站点：`proff`（丹麦最大企业黄页）、`virk`（丹麦官方 CVR 工商库）
- 主链路：`Proff/Virk → GMap → 协议爬虫+LLM → delivery`
- 运行机器：macOS (Machine 2)

```bash
cd Denmark
.venv/bin/python run.py proff
.venv/bin/python run.py virk
```

交付：
```bash
Denmark/.venv/bin/python product.py Denmark day1
```

## 目录约定

```
OldIron/
├── AGENTS.md                    # 全局协作规则
├── README.md                    # 本文件
├── product.py                   # 统一交付入口
├── shared/oldiron_core/         # 共享 Python 业务核心
│   └── protocol_crawler/        # 协议爬虫模块（curl_cffi）
├── VersatileBackend/            # Go 通用后端（Gmap 等高并发服务）
├── Denmark/                     # 丹麦项目
│   ├── run.py
│   ├── src/denmark_crawler/
│   │   ├── fc_email/            # 协议爬虫+LLM 邮箱提取
│   │   └── sites/{proff,virk}/
│   └── output/
├── England/                     # 英国项目
│   ├── run.py
│   ├── src/england_crawler/
│   │   ├── fc_email/            # → 符号链接到 Denmark 的 fc_email
│   │   └── sites/companyname/
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
- England 的 `fc_email/` 是指向 Denmark 的符号链接。Windows 上需要手动复制。
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

原则：凭据按国家隔离；长期续跑的流程必须支持断点恢复。

## 交付原则

- 统一走根目录脚本：`python product.py <Country> dayN`
- 国家内多站点按**公司名去重**后输出
- 交付目录：`<Country>/output/delivery/<Country>_dayNNN/`
- 每天只交付新增，不重复全量
