# DSNURI Crawler Design

**Goal:** 从 `entryList.do` 与 `openBizInfo.do` 抓取 `公司名 + CEO + 地址/电话`，再用 `Google Maps` 补官网，最后通过 `Snov` 获取邮箱并接入现有交付。

**Architecture:** 采用四阶段 pc：Phase 1 列表分页抓基础字段和 `corpId`；Phase 2 详情页补充 `대표번호/주소/업태/종목`；Phase 3 复用现有 `GoogleMapsClient` 以 `公司名 + 地址 + 电话` 搜索官网；Phase 4 复用 `run_snov_pipeline` 写入邮箱并做域名去重。

**Key Constraints:** 公开页与 Excel 都不直接给官网；详情页公开但无官网；必须把 `corpId` 作为断点主键；官网补齐依赖搜索质量，需保留断点和稳态重试。
