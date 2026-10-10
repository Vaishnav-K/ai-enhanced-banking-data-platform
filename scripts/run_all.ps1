$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

Write-Host "`n=== Main ETL Pipeline ===" -ForegroundColor Cyan
& $Python ".\src\etl_pipeline.py"
if ($LASTEXITCODE -ne 0) { throw "Main ETL pipeline failed." }

Write-Host "`n=== Incremental Pipeline ===" -ForegroundColor Cyan
& $Python ".\src\incremental_pipeline.py"
if ($LASTEXITCODE -ne 0) { throw "Incremental pipeline failed." }

Write-Host "`n=== Data Quality Report ===" -ForegroundColor Cyan
& $Python ".\src\quality_report.py"
if ($LASTEXITCODE -ne 0) { throw "Quality report generation failed." }

Write-Host "`n=== Transaction Anomaly Detection ===" -ForegroundColor Cyan
& $Python ".\src\anomaly_detection.py"
if ($LASTEXITCODE -ne 0) { throw "Anomaly detection failed." }

Write-Host "`n=== Analytics CSV Exports ===" -ForegroundColor Cyan
& $Python ".\src\export_analytics.py"
if ($LASTEXITCODE -ne 0) { throw "Analytics export failed." }
Write-Host "`n=== Automated Tests ===" -ForegroundColor Cyan
& $Python -m unittest discover -s tests -p "test_*.py" -v
if ($LASTEXITCODE -ne 0) { throw "Automated tests failed." }

Write-Host "`nAll pipeline steps and tests completed successfully." -ForegroundColor Green
