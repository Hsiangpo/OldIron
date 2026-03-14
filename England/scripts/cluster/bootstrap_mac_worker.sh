#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/England"
PYTHON_BIN=""
MINICONDA_PY="/Users/Zhuanz1/miniconda3/bin/python"
MINICONDA_ENV_PY="/Users/Zhuanz1/miniconda3/envs/oldiron311/bin/python"
MINICONDA_SH="/Users/Zhuanz1/miniconda.sh"

if command -v /opt/homebrew/bin/python3.11 >/dev/null 2>&1; then
  PYTHON_BIN="/opt/homebrew/bin/python3.11"
elif command -v brew >/dev/null 2>&1; then
  brew install python@3.11
  PYTHON_BIN="/opt/homebrew/bin/python3.11"
else
  if [ ! -x "$MINICONDA_PY" ]; then
    curl -L -o "$MINICONDA_SH" https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh
    bash "$MINICONDA_SH" -b -p /Users/Zhuanz1/miniconda3
  fi
  if [ ! -x "$MINICONDA_ENV_PY" ]; then
    /Users/Zhuanz1/miniconda3/bin/conda create -y -p /Users/Zhuanz1/miniconda3/envs/oldiron311 python=3.11
  fi
  PYTHON_BIN="$MINICONDA_ENV_PY"
fi

cd "$PROJECT_ROOT"
"$PYTHON_BIN" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

mkdir -p "$HOME/Library/LaunchAgents"
echo "mac worker 环境准备完成。"
