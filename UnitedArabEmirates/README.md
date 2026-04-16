# UnitedArabEmirates

阿联酋当前接入 6 个站点：

- `dubaibusinessdirectory`
- `hidubai`
- `dayofdubai`
- `dubaibizdirectory`
- `wiza`
- `wizasnov`

## Runtime

```bash
cd UnitedArabEmirates
python -m pip install -r requirements.txt
python run.py dubaibusinessdirectory
python run.py hidubai
python run.py dayofdubai
python run.py dubaibizdirectory
python run.py wiza
python run.py wizasnov
```

统一模式：

```bash
python run.py <site> all
python run.py <site> list
python run.py <site> gmap
python run.py <site> email
```

`wizasnov` 当前只支持：

```bash
python run.py wizasnov all
python run.py wizasnov list
python run.py wizasnov email
```

常用参数：

```bash
python run.py hidubai all --max-pages 5 --list-workers 8 --gmap-workers 64 --email-workers 64
```

## Delivery

阿联酋按站点交付：

```bash
python product.py UnitedArabEmirates day1
```

输出目录：

```text
UnitedArabEmirates/output/delivery/UnitedArabEmirates_day001/
```

其中每个站点会产出一份：

- `<site>.csv`
- `<site>.keys.txt`
- `summary.json`

## Country Rules

- 同站点按 `company_name` 去重。
- 跨站点允许重复出现在不同站点交付文件里。
- 代表人合并顺序固定为 `P1;P3`。
- `P1` 没代表人时，只保留 `P3`。
- 除 `wizasnov` 外，交付门禁按阿联酋单独规则执行：只要 `company_name` 和 `website` 存在，且后置补充链路已经完成（`gmap_status='done'` 且 `email_status='done'`），就允许交付；代表人和邮箱可以为空。
- 邮箱遵循全局默认规则：保留官网真实邮箱，不做域名过滤。
- `dubaibizdirectory` 的 P1 已改为纯协议链路：`curl_cffi + 本地 cookie jar`。
- `dubaibizdirectory` 至少需要一次有效 `cf_clearance`；程序会自动续写 `CAKEPHP` 到 `output/dubaibizdirectory/session/cookie_state.json`。
- `wiza` 的 P1 也是纯协议链路：`curl_cffi + 本地登录态文件`。
- `wiza` 固定只抓 `HQ Location = United Arab Emirates`，不再用 `EMEA/MENA` 泛筛选。
- `wiza` 走普通三段式：`P1` 列表、`P2` GMap 补官网、`P3` 官网规则邮箱 + 官网代表人抽取。
- `wizasnov` 是独立的 `Snov` 版站点，专门承接 `Wiza + Snov` 链路。
- `wizasnov` 不跑 GMap，`P3` 改为 `Snov` 专线。
- `wizasnov` 的 `P3` 先拉域名邮箱（`domain-emails + generic-contacts`），再拉该域名下的全部人员与职位。
- `wizasnov` 只保留 LLM 从 Snov 人员列表里挑出的关键联系人：最大的 4 个角色 + 财务 + 会计；姓名保留原文，职位翻成中文，邮箱单独挂在 `people_json`。
- `wizasnov` 的交付门禁是：`company_name + people_json + emails` 非空，且 `email_status='done'`；不再要求 `gmap_status='done'`。
- `wizasnov` 的交付 CSV 列固定为：`company_name, website, people_json, emails, phone`。
- `wizasnov` 的登录态文件路径是 `output/wizasnov/session/login_state.json`。
- `wizasnov` 的 `Snov` 凭据从 `UnitedArabEmirates/.env` 读取：`SNOV_CLIENT_ID / SNOV_CLIENT_SECRET`。
