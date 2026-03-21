from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

SUSPICIOUS_REP_RE = re.compile(
    r"[@{}<>=;]|https?://|www\\.|"
    r"お問い合わせ|連絡先|紹介|者名|趣味|特技|メッセージ|プロフィール|"
    r"\\b(?:note|blog|company|info|display|flex|official|profile)\\b",
    re.I,
)


def _resolve_input(path: Path) -> Path:
    if path.is_dir():
        candidate = path / "output.jsonl"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"missing output.jsonl under directory: {path}")
    if not path.exists():
        raise FileNotFoundError(f"file not found: {path}")
    return path


def _load_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _is_suspicious_rep(rep: str) -> bool:
    compact = re.sub(r"[\s\u3000]+", "", rep)
    if len(compact) < 3 or len(compact) > 20:
        return True
    return bool(SUSPICIOUS_REP_RE.search(rep))


def evaluate(path: Path, show_bad: int) -> None:
    rows = _load_rows(path)
    ok = [r for r in rows if r.get("status") == "ok"]
    partial = [r for r in rows if r.get("status") == "partial"]
    failed = [r for r in rows if r.get("status") == "failed"]

    ok_with_email_rep = []
    suspicious: list[dict] = []
    for row in ok:
        email = row.get("email")
        rep = row.get("representative")
        email_text = email.strip() if isinstance(email, str) else ""
        rep_text = rep.strip() if isinstance(rep, str) else ""
        if email_text and rep_text:
            ok_with_email_rep.append(row)
        if not rep_text or _is_suspicious_rep(rep_text):
            suspicious.append(row)

    total = len(rows)
    ok_count = len(ok)
    rep_email_ratio = (len(ok_with_email_rep) / ok_count) if ok_count else 0.0
    rep_quality = ((ok_count - len(suspicious)) / ok_count) if ok_count else 0.0

    print(json.dumps(
        {
            "input": str(path),
            "processed": total,
            "ok": ok_count,
            "partial": len(partial),
            "failed": len(failed),
            "ok_with_email_rep": len(ok_with_email_rep),
            "ok_with_email_rep_ratio": round(rep_email_ratio, 4),
            "rep_suspicious": len(suspicious),
            "rep_quality_estimate": round(rep_quality, 4),
        },
        ensure_ascii=False,
    ))

    if show_bad > 0 and suspicious:
        print("--- suspicious representatives ---")
        for row in suspicious[:show_bad]:
            print(
                f"rep={row.get('representative')} | website={row.get('website')} | "
                f"source={row.get('representative_source_url')}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate site_agent output quality")
    parser.add_argument("path", help="Path to output.jsonl/output.success.jsonl or run directory")
    parser.add_argument("--show-bad", type=int, default=20, help="Print top N suspicious reps")
    args = parser.parse_args()

    target = _resolve_input(Path(args.path))
    evaluate(target, max(0, int(args.show_bad)))


if __name__ == "__main__":
    main()
