# ============================================================
#  dev-start-viztracer.ps1  -  Start PyTaskQ with VizTracer
#  Same as dev-start.ps1 but the Worker runs under VizTracer.
#  Press Ctrl+C in the Worker window when done.
#  result.json will be saved to the project root.
# ============================================================

$ProjectRoot = $PSScriptRoot
$PythonExe   = "$ProjectRoot\.venv\Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Magenta
Write-Host "   PyTaskQ  -  VizTracer Profile Mode" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor Magenta
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    Write-Host "[ERROR] .venv not found." -ForegroundColor Red
    Read-Host "Press Enter to exit"; exit 1
}

# 1. Redis
Write-Host "[1/4] Starting Redis..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "wsl -u root redis-server"
Start-Sleep -Seconds 2

# 2. Backend
Write-Host "[2/4] Starting FastAPI backend..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe -m uvicorn src.app:app --reload"
Start-Sleep -Seconds 1

# 3. Worker under VizTracer
Write-Host "[3/4] Starting Worker under VizTracer..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe -m viztracer --ignore_c_function -m src.worker"
Start-Sleep -Seconds 1

# 4. Frontend
Write-Host "[4/4] Starting React frontend..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot\frontend'; npm run dev"

Write-Host ""
Write-Host "========================================" -ForegroundColor Magenta
Write-Host " All services launched in PROFILE mode!" -ForegroundColor Magenta
Write-Host ""
Write-Host "  1. Submit tasks from http://localhost:5173"
Write-Host "  2. Press Ctrl+C in the 'Worker (VizTracer)' window when done"
Write-Host "  3. Run: .\.venv\Scripts\vizviewer.exe result.json"
Write-Host "========================================" -ForegroundColor Magenta
Write-Host ""
