# England

英国当前接入 2 个站点：

- `companyname`
- `wiza`

## Runtime

```bash
cd England
python -m pip install -r requirements.txt
python run.py companyname
python run.py wiza
python run.py wiza list
```

`wiza` 首次启动前需要准备：

```bash
mkdir -p output/wiza/session
```

- `England/.env` 按本机现有运行方式准备即可；`wiza` 本身不依赖 Excel
- `output/wiza/session/login_state.json` 里要放可用的 Wiza 登录态
- `wiza` 只跑列表采集，不跑详情、GMap、P3
- 运行后会生成 `output/wiza/websites.txt`

## Delivery

公司交付：

```bash
python product.py England day1
```

网站交付：

```bash
python product.py England websites day1
```

网站交付目录：

```text
England/output/delivery/England_websites_day001/
```

其中包含：

- `websites.txt`
- `keys.txt`
- `summary.json`
