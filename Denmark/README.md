# Denmark

丹麦项目采用与 England 相同的执行思路，但仅保留 DNB 主链路：

- `pipeline 1`: DNB 获取公司名、代表人、地址、电话、官网
- `pipeline 2`: Google Maps 给缺官网公司补官网和电话
- `pipeline 3`: Firecrawl + LLM 从官网抽邮箱
- `product.py dayN`: 按 `公司名 + 代表人 + 邮箱` 打交付包

## 安装

```bash
cd Denmark
python -m pip install -r requirements.txt
```

## 单机运行

```bash
cd Denmark
python run.py dnb
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
python product.py day1
```
