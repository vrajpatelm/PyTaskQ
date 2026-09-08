# ============================================================
#  dev-start.ps1  -  Start PyTaskQ full stack LOCALLY
#  Runs: Redis (WSL) + FastAPI backend + Worker + React frontend
#  Each service opens in its own PowerShell window.
#  Usage: Right-click -> "Run with PowerShell"  OR  .\dev-start.ps1
# ============================================================

$ProjectRoot = $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "   PyTaskQ  -  Local Dev Launcher" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# -- 0. Check .env exists ------------------------------------
if (-not (Test-Path "$ProjectRoot\.env")) {
    Write-Host "[WARN] .env file not found. Copying from .env.example..." -ForegroundColor Yellow
    Copy-Item "$ProjectRoot\.env.example" "$ProjectRoot\.env"
    Write-Host "[WARN] Please fill in your credentials in .env before running tasks." -ForegroundColor Yellow
    Write-Host ""
}

# -- 1. Check .venv exists -----------------------------------
$PythonExe = "$ProjectRoot\.venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Host "[ERROR] Virtual environment not found at .venv\" -ForegroundColor Red
    Write-Host "        Run: python -m venv .venv && .\.venv\Scripts\python.exe -m pip install -r requirements.txt" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# -- 2. Start Redis in WSL -----------------------------------
Write-Host "[1/4] Starting Redis in WSL..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "wsl -u root redis-server"
Start-Sleep -Seconds 2

# -- 3. Start FastAPI Backend --------------------------------
Write-Host "[2/4] Starting FastAPI backend on http://localhost:8000 ..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe -m uvicorn src.app:app --reload"
Start-Sleep -Seconds 1

# -- 4. Start Worker -----------------------------------------
Write-Host "[3/4] Starting background worker..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe -m src.worker"
Start-Sleep -Seconds 1

# -- 5. Start React Frontend ---------------------------------
Write-Host "[4/4] Starting React frontend (Vite)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot\frontend'; npm run dev"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " All services launched!" -ForegroundColor Green
Write-Host ""
Write-Host "  Frontend  ->  http://localhost:5173" -ForegroundColor White
Write-Host "  Backend   ->  http://localhost:8000" -ForegroundColor White
Write-Host "  API Docs  ->  http://localhost:8000/docs" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "To stop everything, run: .\dev-stop.ps1" -ForegroundColor Yellow
Write-Host ""
