import os
from pathlib import Path

from malaysia_crawler.common.env_loader import load_dotenv


def test_load_dotenv_reads_values(tmp_path: Path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "SNOV_CLIENT_ID=abc123\nSNOV_CLIENT_SECRET=\"secret456\"\n#comment\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("SNOV_CLIENT_ID", raising=False)
    monkeypatch.delenv("SNOV_CLIENT_SECRET", raising=False)
    load_dotenv(env_path)
    assert os.getenv("SNOV_CLIENT_ID") == "abc123"
    assert os.getenv("SNOV_CLIENT_SECRET") == "secret456"
