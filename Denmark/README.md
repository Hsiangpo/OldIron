# Denmark

丹麦采集项目，当前有两条活跃站点主线。

## 当前站点

| 站点   | 入口命令                | 采集来源        | 说明                         |
|--------|------------------------|----------------|------------------------------|
| `proff` | `python run.py proff`  | proff.dk       | 丹麦最大的企业黄页            |
| `virk`  | `python run.py virk`   | datacvr.virk.dk | 丹麦官方工商注册库（CVR）      |

历史站点（`dnb` 等）已归档到 `bak/`。

## 当前主链路

```text
Proff / Virk → GMap（补官网）→ 协议爬虫 + LLM（补邮箱/代表人）→ product.py（去重交付）
```

详细流程：

1. **Proff / Virk** — 直接从站点获取公司名、地址、电话、代表人、邮箱等基础字段
2. **GMap** — 用 `公司名 + 地址 + Denmark` 查询，补充官网域名
3. **协议爬虫 + LLM** — 对有官网但缺邮箱/代表人的公司：
   - 协议爬虫（curl_cffi）抓取站点地图
   - LLM 选出最可能含联系信息的页面
   - 协议爬虫抓取这些页面，HTML 转 Markdown 后发给 LLM 提取邮箱和代表人
4. **product.py** — 合并所有站点结果，按公司名去重后输出交付文件

## 安装

```bash
cd Denmark
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 运行

```bash
# 跑 Proff
python run.py proff

# 跑 Virk
python run.py virk

# 可选参数示例
python run.py proff --email-workers 8 --gmap-workers 64
```

## 交付

```bash
cd ..
Denmark/.venv/bin/python product.py Denmark day1
```

输出目录：`Denmark/output/delivery/Denmark_dayNNN/`

## 关键配置 (.env)

```env
LLM_API_KEY=你的LLM密钥
LLM_BASE_URL=https://api.gpteamservices.com/v1
LLM_MODEL=gpt-5.4-mini
CRAWL_BACKEND=protocol
```

## 并发参数

| 参数              | 默认值 | 说明                    |
|-------------------|--------|------------------------|
| `--search-workers` | 16(Proff)/4(Virk) | 搜索页并发 |
| `--gmap-workers`   | 64     | Google Maps 并发        |
| `--email-workers`  | 8      | 协议爬虫+LLM 邮箱并发   |

## 目录结构

```
Denmark/
├── run.py              # 统一启动入口
├── requirements.txt
├── .env                # 本地配置（不进 git）
├── src/
│   └── denmark_crawler/
│       ├── fc_email/       # 协议爬虫+LLM 邮箱提取（共享模块）
│       └── sites/
│           ├── proff/      # Proff 站点
│           └── virk/       # Virk 站点
├── output/
│   ├── proff/              # Proff 运行产物
│   ├── virk/               # Virk 运行产物
│   └── delivery/           # 交付文件
└── bak/                    # 归档的历史实现
```
