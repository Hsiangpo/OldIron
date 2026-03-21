# Maintainability Refactor Plan (src/ + test/ 每目录最多 5 个文件)

## Summary
目标：把当前仓库重构到“可维护”的形态，并强制满足以下硬性规则：

- `src/` 与 `test/` 下任意目录（不含子目录递归）最多 **5 个文件**（文件类型不限，`__init__.py` 也计数）。
- 同时优化可维护性：
  - 删除/收敛历史残留与无用依赖。
  - 拆分超大文件与超长函数（目标阈值：文件 >1000 行、函数 >200 行都要消除）。
  - 保证命令入口稳定：`python -m web_agent|site_agent|gmap_agent|corp_agent|hojin_agent` 可用。
- Playwright 改为强依赖（写入依赖并明确安装 chromium 步骤）。

> 说明：该规则不适用于运行时生成的目录（例如 `__pycache__/`、`.pytest_cache/` 等），也不适用于 `output/` 等运行产物目录。

---

## Ground Truth (当前已发现的问题)
1. **目录文件数超标**
   - `src/site_agent` 20 个文件
   - `src/web_agent` 9 个文件
   - `src/gmap_agent` 12 个文件
   - `src/corp_agent` 6 个文件
   - `src/hojin_agent` 6 个文件
   - `test/` 20+ 个文件
2. **大文件超标**
   - `src/web_agent/service.py` 6349 行
   - `src/site_agent/pipeline.py` 5173 行
   - `src/site_agent/llm_client.py` 1520 行
3. **超长函数/方法超标（>200 行）**
   - 主要集中在 `src/web_agent/service.py` 与 `src/site_agent/pipeline.py`
4. **依赖问题**
   - `requirements.txt` 有未使用依赖：`cloudscraper`（代码中无引用）
   - Playwright 目前被用到（gmap pb 抓取、google 地理兜底），但 `requirements.txt` 未声明
5. **运行环境风险**
   - Python `sys.path` 被 `__editable__.official_site_agent-0.1.0.pth` 注入了 `D:\\Develop\\...\\src`，导致在 `E:\\...` 改代码可能跑的是 `D:\\...` 版本。
6. **日志乱码**
   - `src/web_agent/service.py` 有 3 条 `"[??] ?????"` 之类的乱码日志，需要修复为可读文本。

---

## Implementation Strategy (执行顺序)
采用“先立规则 + 再修环境/依赖 + 再搬迁分层 + 再拆大文件/长函数 + 最后清理与验收”的顺序。

### Phase 0 — Guardrails & Tooling
新增维护检查脚本（建议 `tools/maintain/check_repo.py`）：

- 检查 `src/` 与 `test/`：每个目录文件数 <=5（排除 `__pycache__/` 等生成目录）
- 检查 `.py`：单文件 <=1000 行
- 检查函数/方法：<=200 行（用 `ast` 解析；用 `utf-8-sig` 读取以兼容 BOM）

验收点：重构过程中可以持续跑该脚本，防止回归。

### Phase 1 — Dependency & Environment Fix
1. Playwright 变为强依赖
   - `requirements.txt` 增加 `playwright>=...`
   - README 增加安装步骤：`python -m playwright install chromium`
   - 对需要 Playwright 的路径不再静默降级，改为明确报错并提示安装方式
2. 移除未使用依赖
   - `requirements.txt` 移除 `cloudscraper`
3. 解决 D:\\ 与 E:\\ 源码混用
   - 增加 `pyproject.toml`（采用 `src/` layout）
   - `pip uninstall official-site-agent`
   - 在 `E:\\...\\wikipedia` 执行 `pip install -e .`
   - 验证：`python -c "import web_agent; print(web_agent.__file__)"` 指向 `E:\\...`

### Phase 2 — Target Package Layout (每目录 <=5 文件)
目标树（允许在实施中继续下钻拆分，只要满足文件数上限）：

- `src/site_agent/`：顶层仅保留 `__init__.py`、`__main__.py`；其余拆到子包（`app/`、`pipeline/`、`firecrawl/`、`snov/`、`llm/`、`content/`、`extract/`、`io/`、`prompts/`）
- `src/web_agent/`：顶层仅保留 `__init__.py`、`__main__.py`；其余拆到子包（`app/`、`job/`、`stage_registry/`、`stage_gmap/`、`stage_site/`、`support/`）
- `src/gmap_agent/`：顶层仅保留 `__init__.py`、`__main__.py`；其余拆到子包（`app/`、`core/`、`net/`、`pb/`、`io/`、`utils/`）
- `src/corp_agent/`：拆到 `app/` + `core/`
- `src/hojin_agent/`：拆到 `app/` + `core/`

### Phase 3 — Refactor Execution Order
按依赖链从小到大实施：

1. `corp_agent`、`hojin_agent`
2. `gmap_agent`
3. `site_agent`
4. `web_agent`

每完成一个 agent：
- 运行维护脚本（目录文件数/行数/函数长度）
- 运行 `python -m pytest test -v`
- 冒烟：`python -m <agent> --help`

### Phase 4 — Test Suite Restructure (test/ 每目录 <=5 文件)
把 `test/` 从平铺拆成按模块/主题分组的子目录结构；避免在顶层堆大量 `test_*.py`。

---

## Acceptance Criteria (最终验收)
1. `src/` 与 `test/` 下任意目录（直接子文件）<=5 文件
2. `.py` 文件均 <=1000 行
3. 函数/方法均 <=200 行
4. `requirements.txt`：移除 `cloudscraper`；加入 Playwright 并说明 chromium 安装步骤
5. `python -m pytest test -v` 全绿
6. CLI 入口可用：`python -m web_agent|site_agent|gmap_agent|corp_agent|hojin_agent`
7. Python 导入路径不再指向 D 盘旧源码
