# England 静态切片执行设计

**目标**

把 England 的主执行模式从“实时协调器 + 多机抢任务”切回“静态切片 + 各机独立执行 + 集中式合并”，保留现有 DNB / Companies House 单机 pipeline 与本地 sqlite 断点能力，不再依赖跨机实时回写。

**现状结论**

- 单机入口一直都在：
  - `python run.py dnb`
  - `python run.py companies-house`
- 两条单机流水线都自带本地 sqlite/WAL 队列和断点恢复：
  - DNB: `output/dnb/store.db`
  - CH: `output/companies_house/store.db`
- 交付打包本质上只依赖站点目录下的 `final_companies.jsonl` / `companies_with_emails.jsonl`，不依赖实时协调器。
- 当前 cluster 模式的问题不是单个 bug，而是：
  - 多机网络、IP、代理、数据库连接、协调器健康状态全部耦合
  - worker 成功与回写成功分离，天然有“白跑”风险
  - 每扩一个站点，都要再接一层集群编排

## 方案对比

### 方案 A：保留 cluster，只继续补稳定性

**优点**

- 现有命令不变。
- 实时状态可集中查看。

**缺点**

- England 这轮已经证明，复杂度在架构层，不在几个补丁。
- DNB cookie、代理、主机 IP、协调器、Postgres、跨机网络全部是故障点。
- 新国家、新站点还得继续接 cluster 任务模型，维护成本会持续上升。

**结论**

不再推荐。

### 方案 B：彻底回到单机执行，人工切片 + 人工合并

**优点**

- 最简单。
- 不需要再写协调器逻辑。

**缺点**

- 切片和合并全靠人工，容易出错。
- 无法稳定复用到后续国家和站点。
- 数据去重、目录约定、交付前校验都会分散。

**结论**

能用，但太原始，不适合作为长期标准方案。

### 方案 C：静态切片 + 各机独立执行 + 集中式合并

**优点**

- 每台机器只依赖本地代码、本地 sqlite、本机代理、本机 9222 浏览器。
- 断点恢复天然本地化，不存在“抓到了但没回写失败”的跨机状态撕裂。
- 分片和合并是通用能力，后续国家和站点可以复用。
- 交付仍然走统一 `delivery.py`，数据口径不需要重写。

**缺点**

- 需要新增切片与合并命令。
- 实时全局状态不再自动集中展示。

**结论**

这是推荐方案，也是本次改造目标。

## 推荐设计

### 1. 保留单机 pipeline，不再把 cluster 当主路径

- `run.py dnb` 和 `run.py companies-house` 继续作为真实执行器。
- cluster 命令先保留，但降级为兼容入口，不再继续加新能力。
- 后续默认执行路径改为：
  1. 主机生成切片
  2. 各机同步代码和分片
  3. 各机独立执行
  4. 主机回收各机产物
  5. 主机合并后再交付

### 2. 新增分布式命令面

新增独立命令面，暂定挂在 `python run.py dist ...`。

#### CH 切片

- 输入：`docs/英国.xlsx`
- 输出：`output/distributed/ch/<batch>/shard-001.txt` 这类稳定公司名清单
- 切片规则：对标准化公司名做稳定哈希分桶，避免按顺序切片导致重跑漂移

#### DNB 切片

- 输入：DNB 本地行业目录快照
- 输出：`output/distributed/dnb/<batch>/shard-001.segments.jsonl`
- 切片规则：按**叶子行业路径**稳定分桶
- 叶子定义：
  - 有子类的顶级行业不直接参与执行
  - 仅子类参与
  - 没有子类的顶级行业保留
- 原因：
  - 当前 `build_industry_seed_segments()` 会同时产出顶级类和子类，天然有重叠风险
  - 多机独立执行时，应尽量减少跨机重复抓取

### 3. 单机执行命令补齐“外部输入 + 独立输出目录”

#### Companies House

新增能力：

- 支持 `--input-file`，读取切片文本而不是固定 xlsx
- 支持 `--output-dir`，让每个分片写入自己的独立目录

执行示例：

```bash
python run.py companies-house --input-file output/distributed/ch/batch-001/shard-001.txt --output-dir output/runs/ch-shard-001
```

#### DNB

新增能力：

- 支持 `--seed-file`，从外部 JSONL 读取行业切片
- 支持 `--output-dir`

执行示例：

```bash
python run.py dnb --seed-file output/distributed/dnb/batch-001/shard-001.segments.jsonl --output-dir output/runs/dnb-shard-001
```

### 4. 本地断点模型保持不变

- 每个分片目录自带自己的：
  - `store.db`
  - `run.log`
  - `companies.jsonl`
  - `companies_enriched.jsonl`
  - `companies_with_emails.jsonl`
  - `final_companies.jsonl`
- 断点恢复只看本地目录，不再依赖主机协调器。
- 多台机器互不共享 sqlite，也就不会互相打架。

### 5. 集中式合并只认标准产物，不认运行时状态

主机新增合并命令，把多机 run 目录合并到标准站点目录：

- CH 合并目标：`output/companies_house`
- DNB 合并目标：`output/dnb`

合并只读取：

- `final_companies.jsonl`
- 不存在时回退 `companies_with_emails.jsonl`

合并时统一做：

- 逐记录清洗
- 按域名 / 标准化公司名去重
- 选择更优记录
- 原子写回标准站点目录

这样 `delivery.py` 无需感知“它来自几台机器”。

### 6. 交付口径不变

交付仍然使用：

```bash
python product.py dayN
```

因为 `delivery.py` 本来就是读 `output/*/final_companies.jsonl`。

这意味着本次改造只动：

- 切片
- 本地执行参数
- 合并

不重写交付口径。

## 数据流

### CH

1. 主机从 `docs/英国.xlsx` 生成 shard 文本清单
2. 每台机器拿一个或多个 shard 文本
3. 本机 `companies-house` pipeline 产出本地 `final_companies.jsonl`
4. 主机把多个 run 目录合并进 `output/companies_house`
5. `product.py dayN` 读取合并后的 CH 站点目录

### DNB

1. 主机从 NAICS 目录生成叶子行业切片
2. 每台机器拿一个或多个 segment JSONL
3. 本机 `dnb` pipeline 完成 discovery/detail/gmap/firecrawl
4. 主机把多个 run 目录合并进 `output/dnb`
5. `product.py dayN` 读取合并后的 DNB 站点目录

## 错误处理

- 单机执行失败：只影响当前机器当前分片，不影响其他机器
- 机器断电：本地 sqlite 保留断点，重启后继续同一输出目录
- 主机关机：不会导致其他机器“抓到了但没回写”
- 合并中断：目标站点目录使用原子替换，避免半成品

## 验证标准

- 同一 shard 重跑不应重复导入已完成记录
- 不同 shard 合并后不应出现明显重复公司
- `product.py dayN` 的总量与增量应只依赖合并后的标准站点目录
- 断网、代理失效、浏览器 cookie 失效都只应影响本机 shard，不应污染其他机器

## 不做的事

- 不再继续强化 England 实时 coordinator
- 不做跨机实时状态同步
- 不把本地 sqlite 再包装成新的伪集群
