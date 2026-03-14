from __future__ import annotations

from pathlib import Path

from web_agent.service import ensure_prefecture_docs, match_prefecture_display


def test_match_prefecture_display_for_known_prefectures() -> None:
    assert match_prefecture_display("大阪府") == "大阪府"
    assert match_prefecture_display("東京都") in {"東京都", "东京都"}
    assert match_prefecture_display("北海道") == "北海道"
    assert match_prefecture_display("日本") is None


def test_ensure_prefecture_docs_uses_strict_prefecture_headers(tmp_path: Path) -> None:
    doc = tmp_path / "日本.txt"
    out = tmp_path / "prefectures"
    doc.write_text(
        "\n".join(
            [
                "札幌市有限会社 1",
                "北广岛市有限会社 0",
                "青森县有限会社 0",
                "城市有限会社名",
                "青森市有限会社",
                "东京都",
                "城市有限会社名",
                "新宿区有限会社",
                "大阪府",
                "城市有限会社名",
                "大阪市有限会社",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    generated = ensure_prefecture_docs(doc_path=doc, output_dir=out, overwrite=True)

    assert set(generated.keys()) == {"北海道", "青森县", "东京都", "大阪府"}
    hokkaido = (out / "北海道.txt").read_text(encoding="utf-8")
    assert "札幌市有限会社 1" in hokkaido
    assert "北广岛市有限会社 0" in hokkaido
    assert "青森县有限会社 0" not in hokkaido
