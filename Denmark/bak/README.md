# Denmark

丹麦项目现在有两条主线：

- `dnb`: DNB 获取公司名、代表人、地址、电话、官网，再走 GMap / Firecrawl
- `virk`: Virk 公开接口优先直拿公司名、代表人、邮箱、电话，只有缺邮箱才走 GMap / Firecrawl
- `proff`: Proff 搜索页直取公司名、代表人、邮箱、官网、电话；缺官网时走 GMap，缺邮箱时再走 Firecrawl
- `python ..\\product.py Denmark dayN`: 先合并丹麦所有站点结果，再按国家口径统一去重打交付包

## 安装

```bash
cd Denmark
python -m pip install -r requirements.txt
```

## 单机运行

```bash
cd Denmark
python run.py dnb
python run.py virk
python run.py proff
python run.py proff --query ApS --max-pages-per-query 10 --search-workers 16 --gmap-workers 16 --firecrawl-workers 64
```

说明：

- `python run.py proff` 会自动尝试拉起它需要的 Go 后端（`gmap-service`、`firecrawl-service`）
- 正常使用时，不需要额外再敲一套 `backend start|stop` 命令
- 站点命令结束后，会把它本次自己拉起的 Go 后端自动关掉
- 不传 `--query` 时，`proff` 默认走“极限覆盖”模式：
  - 先抓 `brancher` 行业目录
  - 再按 `industry + municipality + postplace` 做 API 深分段
- 传了 `--query ApS` 这类参数时，`proff` 会走定向模式，适合小范围验证

## Virk 小规模冒烟

```bash
cd Denmark
python run.py virk --max-companies 20 --output-dir output/virk
```

## 静态分片

```bash
cd Denmark
python run.py dist plan-dnb --shards 2
```

## 分片执行

```bash
cd Denmark
python run.py dnb --seed-file output/distributed/dnb/shard-001.segments.jsonl --output-dir output/runs/dnb-shard-001
```

## 合并与交付

```bash
cd Denmark
python run.py dist merge-site dnb --run-dir output/runs/dnb-shard-001 --run-dir output/runs/dnb-shard-002
cd ..
python product.py Denmark day1
```

说明：

- 根目录 `product.py Denmark dayN` 会一起读取丹麦所有站点输出，所以最终交付是 `dnb + virk + proff` 合并后的国家级结果
- `output/virk_smoke*` 这类冒烟目录只用于测试，用完就删除，不要长期堆在 `output/`
- `proff` 现在已经是三段流水线：`搜索页直出 -> GMap 补官网 -> Firecrawl 补邮箱`
- 如果 Proff 搜索结果里已经有 `公司名 + 代表人 + 邮箱`，就不会再去跑 GMap
- 如果已有官网但没邮箱，会直接进 Firecrawl，不会先绕去 GMap
- `proff` 支持断点续跑、API 深分段任务、GMap/Firecrawl 队列和 JSONL 快照导出
