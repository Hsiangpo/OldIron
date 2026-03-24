"""Finland crawler package."""

from pathlib import Path

from dotenv import load_dotenv


_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")
