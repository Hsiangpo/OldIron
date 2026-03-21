# Denmark Proff Mac 交接文档

## 当前代码状态

- 丹麦主路径现在只保留新框架的 `proff` 主线。
- 历史代码和历史输出已经移到 `Denmark/bak/`，不再作为主路径运行。
- 当前最新已推送提交：
  - `0372a80` `align firecrawl go backend with proff runtime`
- 当前主入口：
  - `python run.py proff`

## 当前链路

```text
Proff -> GMap -> Firecrawl + LLM -> product.py
```

具体规则：

1. `Proff`
   - 直取公司名、代表人、电话、地址、邮箱。
   - 如果 `公司名 + 代表人 + 邮箱` 齐全，直接完成。

2. `GMap`
   - 查询词固定为：`公司名 + 地址 + Denmark`
   - 目标是拿官网和电话。

3. `Firecrawl + LLM`
   - Firecrawl 先拿站点地图。
   - 外部 LLM 选 8 个最可能出现代表人/邮箱的链接。
   - Firecrawl 抓这 8 页整页 HTML。
   - 外部 LLM 从 HTML 里抽公司名、最大代表人、邮箱。

4. `product.py`
   - 最终交付只输出三列：
     - `company_name`
     - `representative`
     - `email`

## 共享后端运行方式

`proff` 站点内部会自动管理它依赖的共享后端。

正常运行时，不需要手动先开：

- `gmap-service`
- `firecrawl-service`
- `myip-service`

也就是说，正常情况下只需要：

```powershell
cd Denmark
python run.py proff
```

## 当前默认配置口径

- `GMap` 默认优先走共享 Go 后端。
- `Firecrawl` 现在也已经支持新的 Go 传输后端接回主链。
- `MyIP` 默认关闭，不是所有站点都强制走轮询住宅 IP。
- 当前默认模型：
  - `LLM_MODEL=gpt-5.4-mini`
  - `LLM_REASONING_EFFORT=medium`

## 没进 git 的东西

下面这些不会跟随 `git pull` 到 Mac：

1. `Denmark/.env`
   - 这是本地私有配置文件，不进 git。

2. `Denmark/output/firecrawl_keys.txt`
   - Firecrawl keys 文件不进 git。

3. `Denmark/bak/`
   - 这是本地历史归档目录，目前没有提交进 git。

4. `VersatileBackend/proxy-pool-service.exe`
   - 这是本地临时产物，不进 git。

## Mac 侧必须自己补的内容

### 1. 创建 `Denmark/.env`

至少要有这些：

```env
FIRECRAWL_KEYS_FILE=output/firecrawl_keys.txt
LLM_API_KEY=你的值
LLM_BASE_URL=https://api.gpteamservices.com/v1
LLM_MODEL=gpt-5.4-mini
LLM_REASONING_EFFORT=medium
PROFF_USE_GO_GMAP_BACKEND=1
PROFF_USE_GO_FIRECRAWL_BACKEND=1
MYIP_ENABLED=0
```

如果后面要让 `proff` 用 `MyIP`，再把：

```env
MYIP_ENABLED=1
```

打开。

### 2. 创建 `Denmark/output/firecrawl_keys.txt`

- 这里要放新的 Firecrawl keys。
- 一行一个 key。
- 旧 keys 不建议继续沿用。

## Firecrawl key 检查口径

以后判断 Firecrawl key 是否真可用，统一按这套口径：

- 使用 `curl_cffi`
- 浏览器指纹：`chrome131`
- 走代理：`http://127.0.0.1:7897`
- 接口：`https://api.firecrawl.dev/v1/scrape`
- 最小请求体：
  - `{"url": "https://example.com", "formats": ["markdown"]}`
- 如果先遇到 `429`，要先重试，再看最终状态

原因：

- 这样更容易真正打到 Firecrawl 的额度判断层。
- 用普通 `requests` 或错误接口去测，容易先撞到 WAF / API 网关限流，只看到 `429`，拿不到真实额度状态。

## Mac 上怎么继续跑

### 安装依赖

```powershell
cd /Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/Denmark
python -m pip install -r requirements.txt
```

### 查看入口是否正常

```powershell
python run.py proff --help
```

### 小范围试跑

```powershell
python run.py proff --query ApS --max-pages-per-query 5 --search-workers 16 --gmap-workers 16 --firecrawl-workers 64
```

### 正常全流程跑

```powershell
python run.py proff
```

### 交付

```powershell
cd ..
python product.py Denmark day1
```

## 当前我确认过的内容

- `python -m unittest tests.test_run_dispatch tests.test_proff_client tests.test_proff_store tests.test_proff_pipeline tests.test_delivery -v`
  - 通过
- `go test ./...` under `VersatileBackend`
  - 通过
- `python run.py proff --help`
  - 正常
- `python product.py Denmark day1`
  - 入口正常

## 继续开发时的注意点

- 不要再往 `bak/` 里改代码。
- 新改动都只进主路径：
  - `Denmark/run.py`
  - `Denmark/src/...`
  - `Denmark/tests/...`
  - `VersatileBackend/...`
- 写完一批可运行代码后，要继续：
  - `git add`
  - `git commit`
  - `git push`

这样 Mac 上才能直接 `git pull` 接着干。
