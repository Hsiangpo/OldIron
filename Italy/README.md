# Italy

意大利当前接入 2 个站点：

- `dnb`
- `wiza`

`dnb` 当前流程：

- P1 只抓 DNB 列表公司名与地区信息，不抓 DNB 详情页
- P2 用 Verif 通过公司名补 `website + representative`
- P3 进入官网，按规则抓邮箱

`wiza` 当前只抓官网列表，不进入详情页，也不跑 GMap / P2 / P3。

## Runtime

```bash
cd Italy
python -m pip install -r requirements.txt
python run.py dnb
python run.py wiza
```

`dnb` 也支持分阶段模式：

```bash
python run.py dnb list
python run.py dnb verif
python run.py dnb email
python run.py dnb all
```

首次启动前，还需要准备两样本地运行态：

```bash
cp .env.example .env
mkdir -p output/wiza/session
mkdir -p output/dnb/session/verif_profile
```

- `Italy/.env` 按本机现有运行方式准备即可
- `output/wiza/session/login_state.json` 里要放可用的 Wiza 登录态
- 运行后会生成 `output/wiza/websites.txt`
- `dnb` 默认需要本机可启动 Chrome / Chromium
- Verif 命中 Cloudflare 时，默认走持久浏览器 profile；首次运行可能需要等待 challenge 放行

## Delivery

```bash
cd ..
python product.py Italy websites day1
```

输出目录：

```text
Italy/output/delivery/Italy_websites_day001/
```
