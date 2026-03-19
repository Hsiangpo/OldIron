# Snov 模块 (VersatileBackend)

## 简介

`Snov` 模块是 `VersatileBackend` 中专门用于处理 **Snov.io** 接口请求的子服务（使用 Go 语言编写）。该模块的核心功能是通过公司的官网域名，批量并且高效地查找和挖掘潜在的企业联系人邮箱。

## 多账套 Key 轮询机制

因为 Snov 存在较强的 API 调用限额限制（单号额度有限），我们在业务场景下经常会遇到跑满配额报错。为了保证整个国家数万级别公司的持续采出，本模块支持**多 Snov Key 轮询（Round-Robin）调度**。

也就是可以将多个 Snov 账号池配制进环境变量中。在实际使用的时候，当请求失败或者报额度用尽时，Go 后端内部会自动平滑切换到下一个有效秘钥上，实现了不停机的高可用。

### 配置方式与示例

你可以通过 `.env` 或者环境变量的形式为后端服务挂载多个凭证。约定格式按照序号后缀累加（如无后缀表示第一个主账号，`_2` 表示第二个账号，以此类推）：

```env
# Snov 账号一
SNOV_CLIENT_ID=df74de644c86324077b2249e733fe9d4
SNOV_CLIENT_SECRET=b81a3e91c3fe488267c23eebd8d2d9ef

# Snov 账号二
SNOV_CLIENT_ID_2=742b7ac74e5b68444637e70c599932c9
SNOV_CLIENT_SECRET_2=37f316d5fdf7c95a94beeffc3f5213f8

# 可无限叠加配置更多账号
# SNOV_CLIENT_ID_3=...
# SNOV_CLIENT_SECRET_3=...
```

## 使用说明

各国家的主干爬虫（Python）遇到需要请求邮箱的入口时，不需要再去各自的 `.env` 中载入单机凭证。
直接向 `VersatileBackend` 发起带有目标域名和请求体的 API 调用，该后台服务就会结合当前负载及剩余额度情况，调度可用的 SNOV_CLIENT 帮你在后台跑完并将结果稳定地返回。

注意不要把这些 plaintext passwords 和 secrets 传进 Git 记录中，仅供本次本地测试或部署时放置于受保护的配置文件下。
