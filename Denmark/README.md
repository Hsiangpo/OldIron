# Denmark

丹麦主路径现在只保留新框架的 `proff` 主线。

历史实现已经归档到：

- [bak](E:/Develop/Masterpiece/Spider/Website/OldIron/Denmark/bak)

## 当前主链路

```text
Proff -> GMap -> Firecrawl + LLM -> product.py
```

规则：

- `Proff` 先直取公司名、代表人、电话、地址、邮箱
- 如果 `公司名 + 代表人 + 邮箱` 齐全，就直接入最终结果
- 否则进入 `GMap`
- `GMap` 查询词固定为：`公司名 + 地址 + Denmark`
- `GMap` 拿到官网后，进入 `Firecrawl + LLM`
- `Firecrawl` 先拿站点地图，再由外部 LLM 选 8 页，再抓这 8 页整页 HTML，再由外部 LLM 抽代表人和邮箱
- 最终交付只输出：`company_name`, `representative`, `email`

## 安装

```powershell
cd Denmark
python -m pip install -r requirements.txt
```

## 运行

```powershell
cd Denmark
python run.py proff
python run.py proff --query ApS --max-pages-per-query 5 --search-workers 16 --gmap-workers 16 --firecrawl-workers 64
```

说明：

- `python run.py proff` 是唯一主入口
- `proff` 会在内部自动拉起它需要的共享后端
- `MyIP` 不是默认启用；只有显式开了 `MYIP_ENABLED=1` 才会走住宅轮询出口
- `GMap` 默认优先走共享 Go 后端
- `Firecrawl` 默认走 Python 新链路；如果显式开了 `PROFF_USE_GO_FIRECRAWL_BACKEND=1`，则会自动拉起新的 Go Firecrawl 传输后端

## 交付

```powershell
cd ..
python product.py Denmark day1
```

输出目录：

- `Denmark/output/proff/`
- `Denmark/output/delivery/Denmark_dayNNN/`
