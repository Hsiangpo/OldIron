# UnitedStates

美国当前接入 2 个站点：

- `dnb`
- `wiza`

`wiza` 当前只抓官网列表，不进入详情页，也不跑 GMap / P2 / P3。

## Run

```bash
cd UnitedStates
.venv/bin/python run.py dnb
.venv/bin/python run.py wiza
```

## Delivery

```bash
cd ..
python product.py UnitedStates day1
python product.py UnitedStates websites day1
```
