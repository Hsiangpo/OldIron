"""Finland crawler package."""

from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

