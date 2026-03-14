"""清理 incheon 脏数据 + 重置 detail checkpoint，准备重跑 Phase 2。"""
import json
from pathlib import Path

out = Path("output/incheon")

# 1. 备份旧的 companies.jsonl
old = out / "companies.jsonl"
bak = out / "companies_BAD_mois.jsonl.bak"
if old.exists():
    old.rename(bak)
    print(f"备份: {old} -> {bak}")

# 2. 清除 detail checkpoint
ckpt = out / "checkpoint_detail.json"
if ckpt.exists():
    ckpt.unlink()
    print(f"已删除: {ckpt}")

# 3. 清除旧的 snov 输出（因为 homepage 是错的，email 也是错的）
for f in ["companies_with_emails.jsonl", "checkpoint_snov.json"]:
    p = out / f
    if p.exists():
        p.rename(out / (f + ".bak"))
        print(f"备份: {p}")

print("\n准备就绪！运行 python run.py incheon 重新爬取详情+snov")
