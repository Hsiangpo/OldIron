from __future__ import annotations

def decode_cfemail(encoded: str) -> str:
    if not encoded:
        return ""
    try:
        key = int(encoded[:2], 16)
        chars = [chr(int(encoded[i : i + 2], 16) ^ key) for i in range(2, len(encoded), 2)]
        return "".join(chars)
    except Exception:
        return ""
