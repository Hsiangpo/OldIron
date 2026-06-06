# Japan

日本当前接入 7 个站点（per-site 交付）：

| 站点 | 来源 | 类型 | GMap | 备注 |
|------|------|------|------|------|
| `bizmaps` | biz-maps.com | 企业数据库 | 是 | 按 47 都道府县目录翻页 |
| `hellowork` | hellowork.mhlw.go.jp | 政府求人库 | 否 | website 直接来自详情页 |
| `mynavi` | tenshoku.mynavi.jp | 招聘平台 | 是 | 五十音分组列表 |
| `onecareer` | onecareer.jp | 企业目录 | 是 | 行业分类翻页 |
| `openwork` | openwork.jp | 企业点评库 | 是 | 有图片验证码门禁，先跑 `auth` 过码 |
| `pasonacareer` | pasonacareer.jp | 招聘平台 | 是 | 代表人纯靠官网 LLM |
| `xlsximport` | 本地 xlsx 导入 | 导入 | 否 | 邮箱来自源文件，代表人走官网 LLM |

## 主链路

标准三段式：P1 站点采集 → P2 GMap 补官网（`hellowork`、`xlsximport` 不跑）→ P3 协议爬虫规则邮箱 + LLM 代表人。P2/P3 边跑边轮询入库新行。邮箱走协议爬虫规则提取 + LLM 兜底，不使用 Snov。

## Runtime

```bash
cd Japan
python -m pip install -r requirements.txt
python run.py bizmaps        # mode: all/list/gmap/email
python run.py hellowork
python run.py mynavi
python run.py onecareer
python run.py openwork auth   # 首次需先过验证码，再 python run.py openwork
python run.py pasonacareer
python run.py xlsximport --xlsx docs/日本.xlsx
```

## Delivery

```bash
cd ..
python product.py Japan day1
```

输出目录：`Japan/output/delivery/Japan_dayNNN/`（per-site，每站点一份 `<site>.csv` + `<site>.keys.txt`，外加汇总 `summary.json`）。

注意：Japan 只支持 `companies` 交付（`python product.py Japan dayN`），**不支持 `websites` 模式**。

## Country Rules

- per-site 交付，不做跨站合并；站点目录动态发现，新增站点 DB 会被自动纳入。
- 交付门禁：`company_name + representative + emails` 三项齐全，缺一不交付。
- 交付 CSV 列：`company_name, representative, website, emails, phone, address, industry, founded_year, capital, detail_url, source_job_url`（比通用 6 列多出站点字段，属 Japan 特例）。
- 多机同日：按站点分工，每台机器用环境变量 `JAPAN_DELIVERY_SITES` 只交付自己负责的站点；选一台汇总机，用 `JAPAN_DELIVERY_SUMMARY_ONLY=1` 收集各机 `<site>.csv` / `<site>.keys.txt` 后重建最终 `summary.json`。
