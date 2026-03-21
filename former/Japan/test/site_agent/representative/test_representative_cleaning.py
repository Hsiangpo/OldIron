from __future__ import annotations

from site_agent.pipeline import _clean_representative_name


def test_clean_representative_name_strips_titles() -> None:
    assert (
        _clean_representative_name(
            "代表取締役社長　柳下　正之"
        )
        == "柳下 正之"
    )
    assert (
        _clean_representative_name(
            "代表取締役 大林 司"
        )
        == "大林 司"
    )
    assert (
        _clean_representative_name(
            "取締役社長: 山田 太郎"
        )
        == "山田 太郎"
    )
    assert _clean_representative_name("CEO John Smith") == "John Smith"
    assert _clean_representative_name("兼CEO 毛籠勝弘") == "毛籠勝弘"
    assert _clean_representative_name("執行役員社長 菊池 廉也") == "菊池 廉也"
    assert _clean_representative_name("上席執行理事 山田 太郎") == "山田 太郎"


def test_clean_representative_name_strips_trailing_site_noise() -> None:
    assert _clean_representative_name("山田太郎 詳しくはこちら") == "山田太郎"
    assert _clean_representative_name("髙畠裕介HP") == "髙畠裕介"
    assert _clean_representative_name("山﨑 幹夫からの") == "山﨑 幹夫"


def test_clean_representative_name_rejects_invalid() -> None:
    assert _clean_representative_name("株式会社サンプル") is None
    assert _clean_representative_name("お問い合わせ") is None
    assert (
        _clean_representative_name(
            "代表取締役社長 佐藤 本店"
        )
        is None
    )
    assert _clean_representative_name("http://example.com") is None
    assert _clean_representative_name("info@example.com") is None
    assert _clean_representative_name("Toshio") is None
    assert _clean_representative_name("未找到代表人") is None
    assert _clean_representative_name("Company info") is None
    assert _clean_representative_name("note サ") is None
    assert _clean_representative_name("John 山田") is None
    assert _clean_representative_name("趣味") is None
    assert _clean_representative_name("紹介") is None
    assert _clean_representative_name("者名") is None
    assert _clean_representative_name("u le { display: flex") is None
    assert _clean_representative_name("f Elemen &&n.namespaceURI==\"h p") is None
    assert _clean_representative_name("トップ") is None
    assert _clean_representative_name("に就任") is None
    assert _clean_representative_name("的なケーススタディ") is None
    assert _clean_representative_name("**会社案内**") is None
    assert _clean_representative_name("ブログ") is None
    assert _clean_representative_name("取 ...") is None
    assert _clean_representative_name("会開催実績と役員報酬") is None
    assert _clean_representative_name("(参考)上席執行理事") is None


def test_clean_representative_name_accepts_latin_full_name() -> None:
    assert _clean_representative_name("Shinya Yoshikawa") == "Shinya Yoshikawa"


def test_clean_representative_name_rejects_bad_punct() -> None:
    assert _clean_representative_name("格。\\") is None
    assert _clean_representative_name("山田、太郎") is None


def test_clean_representative_name_keeps_first_name_when_role_tail_exists() -> None:
    assert (
        _clean_representative_name("山崎 勉 取締役 鈴木 茂洋")
        == "山崎 勉"
    )
