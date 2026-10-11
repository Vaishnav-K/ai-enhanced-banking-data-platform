param(
    [switch]$UploadS3
)

$ErrorActionPreference = "Stop"
$Python = Join-Path $PSScriptRoot "..\.venv312\Scripts\python.exe"
$Python = [System.IO.Path]::GetFullPath($Python)

if (-not (Test-Path $Python)) {
    throw "Python environment not found at $Python. Run the setup script first."
}

Write-Host "`n=== 1. RUNNING LOCAL PIPELINE AND TESTS ==="
& $Python src\demo.py
if ($LASTEXITCODE -ne 0) {
    throw "Local demo or automated tests failed."
}

if ($UploadS3) {
    Write-Host "`n=== 2. VERIFYING S3 INGESTION ==="

    if (-not $env:BANKING_S3_BUCKET) {
        throw "Set BANKING_S3_BUCKET before enabling S3 uploads."
    }

    & $Python src\s3_ingestion.py --upload
    if ($LASTEXITCODE -ne 0) {
        throw "S3 ingestion failed."
    }
}
else {
    Write-Host "`n=== 2. S3 UPLOAD SKIPPED ==="
    Write-Host "Use -UploadS3 to explicitly enable S3 ingestion."
}

Write-Host "`n=== 3. GENERATING INGESTION HEALTH REPORT ==="
& $Python src\manifest_report.py
if ($LASTEXITCODE -ne 0) {
    throw "Ingestion report failed."
}

Write-Host "`nWorkflow completed."
