# Brazil

巴西当前接入 2 个站点（per-site 交付）：

- `cnpjbiz` — cnpj.biz 巴西企业库，按州（estado）全量抓取，走浏览器 profile + 代理
- `dnb` — DNB 列表/详情 → GMap → 协议爬虫+LLM

`cnpjbiz-supervisor` 是 `cnpjbiz` 的常驻监督运行模式。

## Run

```bash
cd Brazil
python run.py cnpjbiz
```

```bash
cd Brazil
python run.py cnpjbiz-supervisor
```

```bash
cd Brazil
python run.py dnb
```

## Delivery

```bash
cd ..
python product.py Brazil day1
```
