# Taiwan

## Sites

- `ieatpe` — 台北市进出口商业同业公会会员资料查询

## Commands

```bash
cd Taiwan
python -m pip install -r requirements.txt
python run.py ieatpe
```

## Delivery

```bash
cd ..
python product.py Taiwan day1
```

台湾通过 `src/taiwan_crawler/delivery.py` 暴露共享风格的 `build_delivery_bundle`，已接入根 `product.py`。
