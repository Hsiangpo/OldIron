# England

England 目录是英国公司信息采集项目，当前主路径是“单机执行 + 静态切片 + 主机集中合并”。

## 日常命令

单机全量：

```bash
python run.py dnb
python run.py companies-house
python product.py day2
```

主机分片：

```bash
python run.py dist plan-ch --shards 2
python run.py dist plan-dnb --shards 2
```

子机执行：

```bash
python run.py companies-house --input-file output/distributed/ch/shard-001.txt --output-dir output/runs/ch-shard-001
python run.py dnb --seed-file output/distributed/dnb/shard-001.segments.jsonl --output-dir output/runs/dnb-shard-001
```

主机合并与交付：

```bash
python run.py dist merge-site companies-house --run-dir output/runs/ch-shard-001 --run-dir output/runs/ch-shard-002
python run.py dist merge-site dnb --run-dir output/runs/dnb-shard-001 --run-dir output/runs/dnb-shard-002
python product.py dayN
```

## 环境配置

默认配置参考 `England/.env.example`。

至少需要确认这些值：

- `DNB_CHROME_DEBUG_URL`
- `GOOGLE_MAPS_PROXY_URL`
- `LLM_API_KEY`
- `FIRECRAWL_KEYS` 或 `FIRECRAWL_KEYS_FILE`
