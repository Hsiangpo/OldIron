#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/England"
PYTHON_BIN="/opt/homebrew/bin/python3.11"

if ! command -v brew >/dev/null 2>&1; then
  echo "缺少 Homebrew，请先安装 Homebrew 后重试。"
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  brew install python@3.11
fi

cd "$PROJECT_ROOT"
"$PYTHON_BIN" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

mkdir -p "$HOME/Library/LaunchAgents"
echo "mac worker 环境准备完成。"
