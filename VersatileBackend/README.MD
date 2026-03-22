# VersatileBackend

## 简介

`VersatileBackend` 是本项目（OldIron）的全局通用后端组件。
这里专门承接**并发型通用后端**，统一使用 **Go** 编写；各国家、各站点的具体网页爬虫继续放在各自国家目录中，由 **Python** 编写。

当前这个目录已经从“只有 README 的想法目录”升级为“带 Go 工程骨架的真实后端入口”：

- `go.mod`
- `cmd/firecrawl-service/`
- `cmd/gmap-service/`
- `cmd/myip-service/`
- `cmd/snov-service/`
- `internal/app/`

第一阶段先提供统一健康检查服务骨架，后面再逐步把 Firecrawl / GMap / Snov 的高并发通用能力迁进来。

## 目录与子模块

目前的子模块主要聚焦在各个国家通用的几类共享能力：

- **`Firecrawl/`**：统一处理与 Firecrawl 相关的页面遍历、数据抽取交互逻辑。
- **`Gmap/`**：统一处理 Google Maps (gmap) 相关的门店、主体查找与官网归集。
- **`Snov/`**：统一处理 Snov 相关的邮箱匹配与找回逻辑。
- **`MyIP/`**：统一处理我们自有住宅代理出口，把多个住宅出口收口成一个本地 HTTP 代理入口。
- **`Protocol/`**：协议爬虫（Python 共享模块），用 `curl_cffi` 替代 Firecrawl 进行站点链接发现和 HTML 抓取。实际代码位于 `shared/oldiron_core/protocol_crawler/`。

## 设计目标与约定

1. **拥抱高并发**：爬虫属于高网络 IO 的业务结构。抽离出 Go 语言模块，充分利用其原生并发的优势（goroutine）来极大地提升整体并发采集效率。
2. **能力复用与解耦**：将这部分跟具体国家不相关的重活（搜地图、查邮箱、通读网页）抽离出来集中维护。以后各国家脚本负责业务层拼接，所有的标准数据富化能力全部委托给 `VersatileBackend` 处理。

## 当前启动方式

```bash
cd VersatileBackend
go run ./cmd/firecrawl-service
go run ./cmd/gmap-service
go run ./cmd/myip-service
go run ./cmd/snov-service
```

健康检查示例：

```bash
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8082/healthz
curl http://127.0.0.1:17897/healthz
curl http://127.0.0.1:8083/healthz
```

## MyIP Service

`myip-service` is a local HTTP proxy bridge for our own residential IP pool.
It accepts normal proxy traffic from crawlers, then forwards requests to one of several upstream residential proxy endpoints.

Current goals:

- hide the upstream list behind one stable local proxy address
- keep the same target host on the same upstream when using `host-hash`
- avoid global punishment; only cool down the single failed upstream
- do not force every site to use MyIP; enable it only when that site really needs it

Key environment variables:

- `MYIP_ENABLED`
- `MYIP_SERVICE_ADDR`
- `MYIP_UPSTREAMS`
- `MYIP_UPSTREAMS_FILE`
- `MYIP_STRATEGY`
- `MYIP_PROXY_URL`

Recommended usage:

1. Start `myip-service`.
2. Only for sites that should use MyIP, set `MYIP_ENABLED=true`.
3. Those sites then use `MYIP_PROXY_URL=http://127.0.0.1:17897`.
4. Sites that do not enable MyIP keep using their normal proxy path, such as `7897`.
