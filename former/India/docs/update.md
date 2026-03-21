## v1.0.6 (2026-01-18)

### 新增
- 详情页提取 Current Directors & Key Managerial Personnel 第一行（DIN/Director Name/Designation/Appointment Date）
- CSV/JSONL 输出增加 current_director 字段

## v1.0.5 (2026-01-14)

### 修复
- 自动检测总页数时遇到 Cloudflare 也可回队列并重试

## v1.0.4 (2026-01-14)

### 修改
- 一键脚本默认自动检测总页数（不再固定 58000）

## v1.0.3 (2026-01-14)

### 修复
- 一键脚本兼容 browser_cookie3 旧版本（不再传 profile 参数）

## v1.0.2 (2026-01-14)

### 新增
- 一键脚本 `scripts/run_pipeline.py`（打开 Chrome -> 导出 cookies -> 续跑）

### 修改
- Cloudflare 自动回退：任务重排队并休息 60-120 秒，自动刷新 cookies
- 429 自动回退：任务重排队并休息 20-30 秒

## v1.0.1 (2026-01-13)

### 新增
- 增加测试 conftest.py 以支持直接运行 pytest

### 修改
- 默认并发提升至 24
- 列表页详情链接规范化为绝对 URL
- 更新 README / INTERFACE / TECH 文档

### 修复
- Cloudflare 页面检测误判导致列表页空解析
- 日志中文乱码（统一 UTF-8 输出）

## v1.0.0 (2026-01-13)

### 新增
- 实现 Active 公司全量爬虫（列表页 + 详情页）
- 支持 Cloudflare cookies 导出与复用
- JSONL/CSV 输出与断点续跑
- 解析核心逻辑与基础测试
