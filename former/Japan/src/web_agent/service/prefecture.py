from __future__ import annotations

import contextlib
import os
import re
import time
from pathlib import Path

from hojin_agent.prefectures import normalize_prefecture

_CN_TO_JP_CHAR_MAP = str.maketrans(
    {
        "县": "県",
        "广": "広",
        "陆": "陸",
        "惠": "恵",
        "岛": "島",
        "轻": "軽",
        "见": "見",
        "钏": "釧",
        "呗": "唄",
        "张": "張",
        "泻": "潟",
        "冈": "岡",
        "宫": "宮",
        "库": "庫",
        "马": "馬",
        "兰": "蘭",
        "贺": "賀",
        "泽": "沢",
        "爱": "愛",
        "长": "長",
        "东": "東",
        "德": "徳",
        "关": "関",
        "荣": "栄",
        "冲": "沖",
        "儿": "児",
        "滨": "浜",
        "鸟": "鳥",
        "叶": "葉",
        "绳": "縄",
        "龙": "竜",
        "网": "網",
        "带": "帯",
        "别": "別",
        "泷": "滝",
        "濑": "瀬",
        "馆": "館",
        "边": "辺",
    }
)
_JP_TO_CN_CHAR_MAP = str.maketrans({v: k for k, v in _CN_TO_JP_CHAR_MAP.items()})
_CITY_SUFFIXES = ("市", "区", "町", "村")
_PREFECTURE_SUFFIXES = ("都", "道", "府", "県", "县")
_CITY_HEADER_LINE = "城市有限会社名"


def _cn_to_jp(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return text.translate(_CN_TO_JP_CHAR_MAP)


def _jp_to_cn(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return text.translate(_JP_TO_CN_CHAR_MAP)


def _normalize_prefecture_cn(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    if value.lower() == "japan" or value == "日本":
        return "全国"
    converted = _cn_to_jp(value)
    if converted.lower() == "japan" or converted == "日本":
        return "全国"
    if converted:
        result = normalize_prefecture(converted)
        if result:
            return result
    direct = normalize_prefecture(value)
    if direct:
        return direct
    return None


def update_city_progress(
    city: str, status: int, *, doc_path: Path | None = None
) -> bool:
    if not isinstance(city, str):
        return False
    city_clean = city.strip()
    if not city_clean:
        return False
    if status not in (0, 1):
        return False
    if doc_path is None:
        doc_path = Path(__file__).resolve().parents[3] / "docs" / "日本.txt"
    if not doc_path.exists():
        return False
    lock_path = doc_path.with_name(doc_path.name + ".lock")
    acquired = False
    for _ in range(50):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            time.sleep(0.1)
            continue
        except Exception:
            break
        else:
            os.close(fd)
            acquired = True
            break
    if not acquired:
        return False

    try:
        text = doc_path.read_text(encoding="utf-8")
    except Exception:
        if lock_path.exists():
            with contextlib.suppress(Exception):
                lock_path.unlink()
        return False
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()

    def _split_line(raw: str) -> tuple[str, str | None]:
        value = raw.strip()
        if not value:
            return "", None
        parts = value.rsplit(" ", 1)
        if len(parts) == 2 and parts[1] in ("0", "1"):
            return parts[0].strip(), parts[1]
        return value, None

    def _matches(line_name: str) -> bool:
        if not line_name:
            return False
        candidates = {
            city_clean,
            _cn_to_jp(city_clean),
            _jp_to_cn(city_clean),
        }
        for cand in candidates:
            cand = (cand or "").strip()
            if not cand:
                continue
            if line_name == f"{cand}有限会社":
                return True
            if line_name.startswith(cand) and line_name.endswith("有限会社"):
                return True
        return False

    updated = False
    for idx, raw in enumerate(lines):
        name, _ = _split_line(raw)
        if not name:
            continue
        if _matches(name):
            lines[idx] = f"{name} {status}"
            updated = True
    if not updated:
        return False
    try:
        doc_path.write_text(newline.join(lines) + newline, encoding="utf-8")
    except Exception:
        return False
    finally:
        if lock_path.exists():
            with contextlib.suppress(Exception):
                lock_path.unlink()
    return True


def _collect_prefecture_sections(doc_path: Path) -> dict[str, list[str]]:
    if not doc_path.exists():
        return {}
    try:
        lines = doc_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return {}
    sections: dict[str, list[str]] = {}
    current_pref: str | None = None
    default_pref = "北海道"
    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue
        name = re.sub(r"\s+[01]$", "", line).strip()
        if not name:
            continue

        pref_name = _extract_prefecture_header(name)
        if pref_name:
            current_pref = pref_name
            sections.setdefault(current_pref, [])
            continue

        if name.endswith("城市名") or name == _CITY_HEADER_LINE:
            if not current_pref:
                current_pref = default_pref
                sections.setdefault(current_pref, [])
            sections.setdefault(current_pref, []).append(_CITY_HEADER_LINE)
            continue

        if not current_pref:
            current_pref = default_pref
            sections.setdefault(current_pref, [])
        sections.setdefault(current_pref, []).append(line)
    return sections


def _extract_prefecture_header(line_name: str) -> str | None:
    text = line_name.strip()
    if not text:
        return None
    if text.endswith("有限会社"):
        text = text[: -len("有限会社")].strip()
    if not text:
        return None
    if text.endswith(_CITY_SUFFIXES):
        return None
    if not text.endswith(_PREFECTURE_SUFFIXES):
        return None
    return text if _normalize_prefecture_cn(text) else None


def normalize_prefecture_display(value: str | None) -> str | None:
    return _normalize_prefecture_cn(value)


def ensure_prefecture_docs(
    *,
    doc_path: Path | None = None,
    output_dir: Path | None = None,
    overwrite: bool = False,
) -> dict[str, Path]:
    if doc_path is None:
        doc_path = Path(__file__).resolve().parents[3] / "docs" / "日本.txt"
    if output_dir is None:
        output_dir = doc_path.parent / "prefectures"
    output_dir.mkdir(parents=True, exist_ok=True)
    sections = _collect_prefecture_sections(doc_path)
    out: dict[str, Path] = {}
    for pref_name, lines in sections.items():
        pref_file = output_dir / f"{pref_name}.txt"
        content_lines = [pref_name]
        content_lines.extend(lines)
        content = "\n".join(content_lines).rstrip() + "\n"
        if overwrite or not pref_file.exists():
            pref_file.write_text(content, encoding="utf-8")
        out[pref_name] = pref_file
    return out


def match_prefecture_display(name: str, *, doc_path: Path | None = None) -> str | None:
    if not isinstance(name, str):
        return None
    value = name.strip()
    if not value:
        return None
    if doc_path is None:
        doc_path = Path(__file__).resolve().parents[3] / "docs" / "日本.txt"
    sections = _collect_prefecture_sections(doc_path)
    if not sections:
        pref_dir = doc_path.parent / "prefectures"
        if pref_dir.exists():
            for p in sorted(pref_dir.glob("*.txt")):
                sections[p.stem] = []
    target = _normalize_prefecture_cn(value)
    for pref_name in sections.keys():
        if pref_name == value:
            return pref_name
        pref_norm = _normalize_prefecture_cn(pref_name)
        if target and pref_norm == target:
            return pref_name
    return None


def update_pref_progress(
    pref_name: str, status: int, *, doc_path: Path | None = None
) -> bool:
    if not isinstance(pref_name, str) or status not in (0, 1):
        return False
    pref_clean = pref_name.strip()
    if not pref_clean:
        return False
    if doc_path is None:
        doc_path = (
            Path(__file__).resolve().parents[3]
            / "docs"
            / "prefectures"
            / f"{pref_clean}.txt"
        )
    if not doc_path.exists():
        return False
    try:
        text = doc_path.read_text(encoding="utf-8")
    except Exception:
        return False
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()

    def _split_line(raw: str) -> tuple[str, str | None]:
        value = raw.strip()
        if not value:
            return "", None
        parts = value.rsplit(" ", 1)
        if len(parts) == 2 and parts[1] in ("0", "1"):
            return parts[0].strip(), parts[1]
        return value, None

    updated = False
    for idx, raw in enumerate(lines):
        name, _ = _split_line(raw)
        if not name or name == pref_clean or name.endswith("城市有限会社名"):
            continue
        if not name.endswith("有限会社"):
            continue
        city_name = name.replace("有限会社", "").strip()
        if not city_name.endswith(("市", "区", "町", "村")):
            continue
        lines[idx] = f"{name} {status}"
        updated = True
    if not updated:
        return False
    try:
        doc_path.write_text(newline.join(lines) + newline, encoding="utf-8")
    except Exception:
        return False
    return True
