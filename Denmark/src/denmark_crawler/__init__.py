"""Denmark crawler package."""

from pathlib import Path

try:
    from dotenv import load_dotenv
    _ROOT = Path(__file__).resolve().parents[2]
    load_dotenv(_ROOT / ".env")
except ModuleNotFoundError:
    pass  # 交付脚本等场景下不需要 dotenv

