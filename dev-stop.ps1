# ============================================================
#  dev-stop.ps1  —  Stop all PyTaskQ local dev services
#  Kills the FastAPI backend, Python worker, Vite frontend,
#  and stops Redis in WSL. Also clears the Redis queue.
#  Usage: .\dev-stop.ps1
# ============================================================

Write-Host ""
Write-Host "========================================" -ForegroundColor Red
Write-Host "   PyTaskQ  —  Stopping All Services" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red
Write-Host ""

# ── Kill Python processes (worker + uvicorn) ────────────────
Write-Host "Stopping Python processes (worker + uvicorn)..." -ForegroundColor Yellow
Get-Process -Name "python" -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process -Name "python3" -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "  Done." -ForegroundColor Green

# ── Kill Node/Vite process ──────────────────────────────────
Write-Host "Stopping Vite dev server (Node)..." -ForegroundColor Yellow
Get-Process -Name "node" -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "  Done." -ForegroundColor Green

# ── Stop Redis in WSL ───────────────────────────────────────
Write-Host "Stopping Redis in WSL..." -ForegroundColor Yellow
wsl -u root systemctl stop redis-server 2>$null
wsl -u root pkill redis-server 2>$null
Write-Host "  Done." -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Red
Write-Host "  All services stopped." -ForegroundColor Green

# ── Optional: flush Redis queue ─────────────────────────────
Write-Host ""
$flush = Read-Host "Do you also want to FLUSH the Redis queue? (clears all tasks) [y/N]"
if ($flush -eq "y" -or $flush -eq "Y") {
    Write-Host "Flushing Redis..." -ForegroundColor Yellow
    wsl redis-cli flushall
    Write-Host "  Redis flushed." -ForegroundColor Green
}

Write-Host ""
Write-Host "Goodbye!" -ForegroundColor Cyan
Write-Host ""
