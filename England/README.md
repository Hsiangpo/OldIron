# England

England 目录是英国公司信息采集项目，当前集群模式统一走 Postgres + coordinator + 多机 worker。

## 日常命令

主控机：

```bash
python run.py cluster coordinator
python run.py cluster submit England
python run.py cluster produce day2
```

任意 worker 机：

```bash
python run.py cluster start-pools
```

## 命令说明

- `python run.py cluster coordinator`
  - 自动读取 `England/.env`
  - 自动初始化 England 集群 schema
  - 不需要额外手工设置 `ENGLAND_CLUSTER_POSTGRES_DSN`
- `python run.py cluster submit England`
  - 自动处理 England 下全部已注册来源
  - 当前包含 `DNB` 和 `Companies House`
  - 已完成来源自动跳过
  - 仅剩失败任务的来源会自动重挂失败任务
- `python run.py cluster start-pools`
  - 按 `.env` 中的并发配置启动本机全部 worker
  - 前台直接打印各 worker 日志，`Ctrl+C` 可整体停止

## 环境配置

默认配置参考 `England/.env.example`。

至少需要确认这些值：

- `ENGLAND_CLUSTER_POSTGRES_DSN`
- `ENGLAND_CLUSTER_BASE_URL`
- `LLM_API_KEY`
- `FIRECRAWL_KEYS` 或 `FIRECRAWL_KEYS_FILE`
