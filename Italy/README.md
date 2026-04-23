# Italy

意大利当前接入 1 个站点：

- `wiza`

`wiza` 当前只抓官网列表，不进入详情页，也不跑 GMap / P2 / P3。

## Runtime

```bash
cd Italy
python -m pip install -r requirements.txt
python run.py wiza
```

首次启动前，还需要准备两样本地运行态：

```bash
cp .env.example .env
mkdir -p output/wiza/session
```

- `Italy/.env` 按本机现有运行方式准备即可
- `output/wiza/session/login_state.json` 里要放可用的 Wiza 登录态
- 运行后会生成 `output/wiza/websites.txt`

## Delivery

```bash
cd ..
python product.py Italy websites day1
```

输出目录：

```text
Italy/output/delivery/Italy_websites_day001/
```
