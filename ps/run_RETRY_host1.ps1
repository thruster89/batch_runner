Set-Location "C:\work\batch_runner"

if (Test-Path ".venv\Scripts\Activate.ps1") {
    . .venv\Scripts\Activate.ps1
} else {
    Write-Error "venv not found"
    exit 1
}

python batch_runner.py `
  --mode RETRY `
  --hosts host1

if ($LASTEXITCODE -ne 0) {
    Write-Error "Batch failed"
    exit $LASTEXITCODE
}

Write-Host "Batch finished successfully"
