# ZaubaCorp Active Companies Crawler

## 功能
- 全量抓取 Active 公司列表（分页）
- 进入详情页抓取 Basic Information + Contact Details + Current Director（仅取 Current Directors & Key Managerial Personnel 第一行）
- 输出 JSONL + CSV，支持断点续跑

## 安装依赖
```bash
pip install -r requirements.txt
```

## Cloudflare 处理
首次运行遇到 Cloudflare 验证失败时，需要先在 Chrome 通过验证并导出 cookies。  
遇到 429 会自动将任务放回队列并休息 20-30 秒。  
遇到 Cloudflare 验证会自动回队列、休息 60-120 秒并尝试重新加载 cookies.json。

方式一：从本机 Chrome 导出
```bash
python scripts/export_cookies.py
```
示例：
```bash
python scripts/export_cookies.py --profile "Profile 1"
```

方式二：使用 9222 CDP 导出
```bash
python scripts/export_cookies_cdp.py --domain zaubacorp.com --output cookies.json
```

运行爬虫时指定 cookies：
```bash
python -m src.main --cookies cookies.json
```

如果当前目录存在 `cookies.json`，可不传 `--cookies`。

## 一键运行
```bash
python scripts/run_pipeline.py --output-dir output/zauba_active
```
流程：自动打开 Chrome → 手动通过验证 → 关闭浏览器 → 导出 cookies → 继续跑。

## 运行示例
```bash
python -m src.main --start-page 1 --end-page 100 --concurrency 12 --cookies cookies.json
```
不传 `--end-page` 会自动检测总页数（当前约 58113）。

## 断点续跑
- 默认开启断点续跑（SQLite 记录已完成页与公司）。
- 需要续跑时，指定上次输出目录：
```bash
python -m src.main --output-dir output/zauba_active --cookies cookies.json
```

## 输出说明
- `output/{timestamp}/companies.jsonl`：每行一条公司记录（含 Basic/Contact/Current Director）
- `output/{timestamp}/companies.csv`：基础字段 + Basic/Contact/Current Director 的 JSON 字符串

如需展开字段为宽表 CSV：
```bash
python scripts/expand_csv.py --input output/20250113_120000/companies.jsonl --output output/20250113_120000/companies_wide.csv
```

## 参数说明
- `--start-page` / `--end-page`：分页范围
- `--concurrency`：详情页并发数（更快可提高）
- `--min-delay` / `--max-delay`：请求间随机延迟
- `--timeout`：请求超时（秒）
- `--max-retries`：请求重试次数
- `--user-agent`：自定义 UA
- `--output-dir`：输出目录
- `--no-resume`：禁用断点续跑
