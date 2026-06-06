# Finland

芬兰当前接入 3 个站点（三站合并去重交付）：

- `tmt` — tyomarkkinatori.fi（芬兰国家劳动力市场招聘平台），走公开 REST API
- `duunitori` — duunitori.fi（芬兰求职网），SSR HTML 解析
- `jobly` — jobly.fi（芬兰职位网），SSR HTML 解析

注意：`tmt` 的 CLI 子命令名与它的站点目录名 `tyomarkkinatori` 不同；另两站命令名与目录名一致。

## 主链路

```text
招聘站列表/详情 → 站内联系人字段优先（齐全则直接交付）→ 缺数据时条件触发 GMap 补官网 → 协议爬虫+LLM 补邮箱/代表人 → 三站合并去重交付
```

- 邮箱：站内联系人邮箱（`tmt` 的 `recruiting.email` / 详情页正则）优先；缺时官网规则提取，不使用 Snov
- 代表人：站内联系人字段优先；缺时官网 LLM 提取
- GMap 为**条件触发**：站内数据三项齐全的记录直接交付、跳过 GMap

## Runtime

```bash
cd Finland
python -m pip install -r requirements.txt
python run.py tmt
python run.py duunitori
python run.py jobly
```

常用参数（三站通用）：

```bash
python run.py tmt --gmap-workers 64 --email-workers 128 --skip-gmap --skip-email
```

非 `--skip-gmap` 时会自动拉起 Go GMap 后端（`VersatileBackend/cmd/gmap-service`，端口 8082）。

## Delivery

```bash
cd ..
python product.py Finland day1
```

输出目录：`Finland/output/delivery/Finland_dayNNN/`

## Country Rules

- 三站（TMT + Duunitori + Jobly）合并，按公司名（小写）跨站去重后交付。
- 交付门禁：`company_name + representative + emails` 三项齐全，缺一不交付；代表人含公司后缀（Oy/AB/Ltd 等）视为无效并清空。
- 交付 CSV 列：`company_name, representative, emails, website, phone, evidence_url`，多邮箱用 `; ` 分隔。
- 交付时邮箱过滤：丢弃通用免费邮箱；有官网域名时优先保留与官网域名相关的邮箱。
- CSV 只写每日增量，`keys.txt` 存全量去重键作为次日基线。
