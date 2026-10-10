$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$PythonLauncher = Get-Command py -ErrorAction SilentlyContinue
if (-not $PythonLauncher) {
    throw "Python launcher 'py' was not found. Install Python 3.12 first."
}

$Python = Join-Path $ProjectRoot ".venv312\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    py -3.12 -m venv .venv312
}

& $Python -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed." }

& $Python -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

& $Python --version
Write-Host "Project setup completed using Python 3.12." -ForegroundColor Green
