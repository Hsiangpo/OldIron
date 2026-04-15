# Germany

德国当前接入 1 个站点：

- `wiza`

## Runtime

```bash
cd Germany
python -m pip install -r requirements.txt
python run.py wiza
```

首次启动前，还需要准备两样本地运行态：

```bash
cp .env.example .env
mkdir -p output/wiza/session
```

- `.env` 里至少要填可用的 `LLM_API_KEY`
- `output/wiza/session/login_state.json` 里要放可用的 Wiza 登录态
- 没有这两个运行态时，`list` 可能还能跑一部分，但 `email/all` 会在官网补充阶段直接失败

统一模式：

```bash
python run.py wiza all
python run.py wiza list
python run.py wiza email
```

常用参数：

```bash
python run.py wiza all --max-pages 5 --list-workers 8 --email-workers 64
```

## Delivery

德国按站点交付：

```bash
python product.py Germany day1
```

输出目录：

```text
Germany/output/delivery/Germany_day001/
```

其中每个站点会产出一份：

- `<site>.csv`
- `<site>.keys.txt`
- `summary.json`

## Country Rules

- 同站点按 `company_name` 去重。
- 跨站点允许重复出现在不同站点交付文件里。
- `wiza` 的 `P1` 不取站内联系人，也不跑 GMap，代表人只在 `P3` 官网里提取。
- 邮箱遵循当前共享规则：只从官网规则提取，不走 LLM。
- `wiza` 固定只抓 `HQ Location = Germany`。
- `wiza` 的登录态文件路径是 `output/wiza/session/login_state.json`。
- `wiza` 的 `P3` 依赖 `.env` 里的 LLM 配置；如果 key 失效，会在官网补充日志里直接报 `401/无效的令牌`。
