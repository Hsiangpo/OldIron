# 马来西亚公司抓取主流程（并发流式版）

## 主流程
1. `CTOS` 线程：持续抓公司名与注册号，实时写入名池。
2. `BusinessList` 线程：按 `company_id` 持续抓域名与 `company_manager`，命中 CTOS 名池后入队。
3. `ManagerAgent` 线程：当 `company_manager` 缺失时，走 Firecrawl + LLM 轮询补齐管理人（最多 3 轮）。
4. `Snov` 线程：消费队列查邮箱，合并为 `contact_eamils`，三要素齐全后才落盘成品。

## 安装
```bash
python -m pip install -r requirements.txt
```

## 命令
```bash
# 启动主流程（断点续跑）
python run.py

# 按天交付
python product.py day1
python product.py day2
```

## 交付规则
- 只能执行“最新交付天”或“最新天 + 1”。
- 已有 `day6`、尚无 `day7` 时：
  - 允许重复执行 `python product.py day6`（会按 day5 基线重算覆盖）。
  - 不允许执行 `python product.py day5`。

## 凭据说明
- 在项目根目录创建 `.env`，写入：
  - `SNOV_CLIENT_ID=...`
  - `SNOV_CLIENT_SECRET=...`
  - `LLM_API_KEY=...`
  - `LLM_BASE_URL=...`
  - `LLM_MODEL=...`
  - `LLM_REASONING_EFFORT=medium`
- 程序启动时自动读取 `.env`，直接 `python run.py` 即可。

## 输出文件
- 主流程状态库：`output/runtime/malaysia_pipeline.db`
- 交付目录：`output/delivery/Malaysia_dayNNN/`
  - `companies.csv`
  - `summary.json`

## 字段边界
- `CTOS`：稳定提供公司名、注册号；董事/股东/财务不在免费链路。
- `BusinessList`：补域名与 `company_manager`，覆盖率不稳定。
- 成品只保留：`company_name`、`domain`、`contact_eamils`、`company_manager`。
