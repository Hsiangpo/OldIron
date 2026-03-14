# 运行模式与配置说明

本项目支持四种运行模式：全量跑、代表人跑、失败跑、半成跑。以下为命令与核心行为差异（命令行版本）。

## 1. 命令与用途

| 模式     | 命令示例                            | 主要用途                                                   |
| -------- | ----------------------------------- | ---------------------------------------------------------- |
| 全量跑   | `python -m web_agent 东京都`        | 全量抓取官网数据（公司名/邮箱/代表人/电话）                |
| 代表人跑 | `python -m web_agent 东京都 代表人` | 仅针对 success 中“代表人缺失/未找到代表人”的记录深挖代表人 |
| 失败跑   | `python -m web_agent 东京都 失败`   | 仅重跑 failed 记录                                         |
| 半成跑   | `python -m web_agent 东京都 半成`   | 仅重跑 partial 记录                                        |

## 2. 核心行为差异

| 配置/行为           | 全量跑                                | 代表人跑                              | 失败跑                                | 半成跑                                |
| ------------------- | ------------------------------------- | ------------------------------------- | ------------------------------------- | ------------------------------------- |
| 必填字段            | company_name + email + representative + phone | company_name + email + representative + phone | company_name + email + representative + phone | company_name + email + representative + phone |
| 代表人缺失处理      | 写入“未找到代表人”，仍算 success      | 写入“未找到代表人”，仍算 success      | 写入“未找到代表人”，仍算 success      | 写入“未找到代表人”，仍算 success      |
| 邮箱策略            | 规则 + Snov                           | **跳过邮箱补全**（仅沿用已有邮箱）    | 规则 + Snov                           | 规则 + Snov                           |
| LLM 选链            | 允许                                  | 允许                                  | 允许                                  | 允许                                  |
| 代表人/电话来源     | Firecrawl /extract                    | Firecrawl /extract                    | Firecrawl /extract                    | Firecrawl /extract                    |
| 本地渲染            | 不使用                                | 不使用                                | 不使用                                | 不使用                                |
| max_rounds          | 默认 3                                | 默认 >= 10                            | 默认 3                                | 默认 3                                |
| max_pages           | 默认 10                               | 默认 10                               | 默认 10                               | 默认 10                               |

## 3. 邮箱规则（全量/失败/半成）

1. 规则先行：规则已拿到邮箱则立即落库，不等待 Snov。
2. 规则未命中时等待 30s：并发等待 Snov，任一返回（含空）即可提前结束。
3. 30s 内两方邮箱统一去重合并；规则页优先 > Snov；公司域名邮箱优先。
4. 30s 超时且规则无邮箱，记为 partial。
5. 晚到补写允许合并升级（partial -> success）。

## 4. 代表人规则（全量/失败/半成）

1. 代表人/电话仅来自 Firecrawl /extract，不使用本地规则或 LLM 校验。
2. 若深挖后仍无代表人：写入“未找到代表人”，仍算 success。
3. 深挖仅从页面实际链接中发现“会社概要/会社详情/会社情報”等入口，不使用固定后缀猜测。

## 5. 公司名规则

1. company_name 固定来自输入名（registry/gmap/manual），不从 Firecrawl/LLM 抽取。

## 6. 状态判定

- success：company_name + email 已有；representative 缺失则填“未找到代表人”。
- partial：规则 + Snov 在 30s 内均无邮箱。
- failed：官网主页无法打开/抽取失败（Firecrawl 请求失败或站点不可达）。

> 说明：如有流程或规则变更，请同步更新本文件与 `README.md`。

## 7. 脱敏邮箱处理

- 含 `*`、`?`、`•`、`…` 等脱敏字符的邮箱一律视为无效，不落盘。
- 若 Snov 仅返回脱敏邮箱，会进入延迟重试队列；规则同样会过滤脱敏结果。
