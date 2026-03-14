import json
from pathlib import Path
from collections import Counter
from urllib.parse import urlparse

# Check both data sources
for source in ["output/incheon/companies.jsonl", "output/catch/companies_with_emails.jsonl"]:
    f = Path(source)
    if not f.exists():
        print(f"SKIP {source} (not found)")
        continue
    
    total = 0
    mois = 0
    domains = Counter()
    sample = None
    
    for line in f.open("r", encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        total += 1
        hp = d.get("homepage", "")
        if hp:
            try:
                host = urlparse(hp).netloc.lower()
                if host.startswith("www."):
                    host = host[4:]
                domains[host] += 1
                if "mois.go.kr" in host:
                    mois += 1
                    if sample is None:
                        sample = d
            except Exception:
                pass
        else:
            domains["(none)"] += 1
    
    print(f"\n{'='*60}")
    print(f"Source: {source}")
    print(f"Total: {total}")
    print(f"mois.go.kr: {mois} ({mois/max(total,1)*100:.1f}%)")
    print(f"\nTop 15 domains:")
    for dom, cnt in domains.most_common(15):
        print(f"  {dom}: {cnt}")
    
    if sample:
        print(f"\nSample mois record:")
        print(json.dumps(sample, ensure_ascii=False, indent=2)[:500])
