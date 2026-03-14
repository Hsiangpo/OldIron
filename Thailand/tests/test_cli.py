from __future__ import annotations

import json
import os

import pytest

from thailand_crawler import cli



def test_run_cli_invokes_stream_pipeline_with_stream_output(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cli, "ROOT", tmp_path)

    captured: dict[str, object] = {}

    class DummyClient:
        pass

    def fake_run_stream_pipeline(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "DnbClient", lambda cookie_header="": DummyClient())
    monkeypatch.setattr(cli, "run_stream_pipeline", fake_run_stream_pipeline)

    code = cli.run_cli([
        "dnb",
        "--log-level",
        "INFO",
        "--max-companies",
        "15",
        "--website-workers",
        "2",
        "--site-workers",
        "3",
    ])

    assert code == 0
    assert captured["project_root"] == tmp_path
    assert captured["output_dir"] == tmp_path / "output" / "dnb_stream"
    assert captured["max_companies"] == 15
    assert captured["website_workers"] == 2
    assert captured["site_workers"] == 3
    assert captured["skip_dnb"] is False
    assert captured["skip_website"] is False
    assert captured["skip_site"] is False
    assert captured["skip_snov"] is False



def test_run_cli_maps_legacy_flags_to_stream_pipeline(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cli, "ROOT", tmp_path)

    captured: dict[str, object] = {}

    class DummyClient:
        pass

    def fake_run_stream_pipeline(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "DnbClient", lambda cookie_header="": DummyClient())
    monkeypatch.setattr(cli, "run_stream_pipeline", fake_run_stream_pipeline)

    code = cli.run_cli([
        "dnb",
        "--log-level",
        "INFO",
        "--max-items",
        "20",
        "--detail-concurrency",
        "5",
        "--gmap-concurrency",
        "6",
        "--snov-concurrency",
        "7",
        "--skip-gmap",
    ])

    assert code == 0
    assert captured["max_companies"] == 20
    assert captured["dnb_workers"] == 5
    assert captured["website_workers"] == 6
    assert captured["snov_workers"] == 7
    assert captured["skip_website"] is True



def test_run_cli_rejects_second_running_instance(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    output_dir = tmp_path / "output" / "dnb_stream"
    output_dir.mkdir(parents=True)
    lock_path = output_dir / "run.lock"
    lock_path.write_text(json.dumps({"pid": 99999}, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(cli, "_pid_exists", lambda pid: True)

    with pytest.raises(RuntimeError, match="已有运行中的 DNB 进程"):
        cli.run_cli(["dnb", "--log-level", "INFO"])


def test_release_run_lock_keeps_other_process_lock(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cli, 'ROOT', tmp_path)
    output_dir = tmp_path / 'output' / 'dnb_stream'
    output_dir.mkdir(parents=True)
    lock_path = output_dir / 'run.lock'
    lock_path.write_text(json.dumps({'pid': os.getpid() + 1000}), encoding='utf-8')

    cli._release_run_lock(lock_path)

    assert lock_path.exists() is True


def test_acquire_run_lock_replaces_stale_lock(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cli, 'ROOT', tmp_path)
    output_dir = tmp_path / 'output' / 'dnb_stream'
    output_dir.mkdir(parents=True)
    lock_path = output_dir / 'run.lock'
    lock_path.write_text(json.dumps({'pid': 999999}), encoding='utf-8')
    monkeypatch.setattr(cli, '_pid_exists', lambda pid: False)

    acquired = cli._acquire_run_lock(output_dir)

    payload = json.loads(acquired.read_text(encoding='utf-8'))
    assert payload['pid'] == os.getpid()
