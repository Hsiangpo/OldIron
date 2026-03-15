#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron/England"

cd "$PROJECT_ROOT"
source .venv/bin/activate
python run.py cluster start-pools
