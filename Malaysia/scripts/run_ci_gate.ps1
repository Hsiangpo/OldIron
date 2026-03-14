param(
  [int]$MaxFileLines = 1000,
  [int]$MaxFuncLines = 200,
  [int]$MaxFilesPerDir = 10
)

$ErrorActionPreference = 'Stop'

python scripts/quality_gate.py --max-file-lines $MaxFileLines --max-func-lines $MaxFuncLines --max-files-per-dir $MaxFilesPerDir
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

pytest -q
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host 'CI门禁通过'
