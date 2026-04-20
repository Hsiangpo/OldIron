# Germany

德国当前接入 2 个站点：

- `kompass`
- `wiza`

## Runtime

```bash
cd Germany
python -m pip install -r requirements.txt
python run.py kompass
python run.py wiza
```

首次启动前，还需要准备两样本地运行态：

```bash
cp .env.example .env
mkdir -p output/wiza/session
```

- `Germany/.env` 按本机现有运行方式准备即可
- `output/wiza/session/login_state.json` 里要放可用的 Wiza 登录态
- `wiza` 只跑列表采集，不跑详情、GMap、P3
- 运行后会生成 `output/wiza/websites.txt`

运行命令：

```bash
python run.py kompass list --max-pages 3
python run.py wiza list
python run.py wiza
```

常用参数：

```bash
python run.py wiza list --max-pages 5 --list-workers 8
```

`kompass` 运行说明：

- `kompass` 只跑列表页，不进入公司详情页，也不跑 GMap/P3
- `kompass` 默认读取 `Germany/.env` 里的 `KOMPASS_COOKIE_HEADER` 与 `KOMPASS_USER_AGENT`
- 也支持把同样信息写入 `output/kompass/session/login_state.json`
- 若命中 DataDome challenge，会直接报错停止，避免静默跑空
- 运行后会生成 `output/kompass/websites.txt`

## Delivery

德国按站点交付：

```bash
python product.py Germany day1
python product.py Germany websites day1
```

输出目录：

```text
Germany/output/delivery/Germany_day001/
Germany/output/delivery/Germany_websites_day001/
```

公司交付中每个站点会产出一份：

- `<site>.csv`
- `<site>.keys.txt`
- `summary.json`

网站交付会产出：

- `websites.txt`
- `keys.txt`
- `summary.json`

## Country Rules

- 同站点按 `company_name` 去重。
- 跨站点允许重复出现在不同站点交付文件里。
- `wiza` 现在只保留网站列表，不再进入后续补充链路。
- `wiza` 固定只抓 `HQ Location = Germany`。
- `wiza` 的登录态文件路径是 `output/wiza/session/login_state.json`。
