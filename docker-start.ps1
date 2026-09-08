# ============================================================
#  docker-start.ps1  —  Run PyTaskQ via Docker Compose
#  Starts everything (Redis + Backend + Worker) in one command.
#  The frontend runs separately via Vite (it's a dev server).
#  Usage: .\docker-start.ps1
# ============================================================

$ProjectRoot = $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Blue
Write-Host "   PyTaskQ  —  Docker Launcher" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host ""

# ── Check Docker is running ─────────────────────────────────
Write-Host "Checking Docker engine..." -ForegroundColor Yellow
$dockerStatus = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] Docker is not running!" -ForegroundColor Red
    Write-Host "        Please open Docker Desktop and wait for the engine to start." -ForegroundColor Red
    Write-Host "        Look for the whale icon in your taskbar to turn green." -ForegroundColor Red
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "Docker is running." -ForegroundColor Green
Write-Host ""

# ── Check .env exists ───────────────────────────────────────
if (-not (Test-Path "$ProjectRoot\.env")) {
    Write-Host "[WARN] .env file not found. Copying from .env.example..." -ForegroundColor Yellow
    Copy-Item "$ProjectRoot\.env.example" "$ProjectRoot\.env"
}

# ── Ask: rebuild or just start ──────────────────────────────
Write-Host "Options:" -ForegroundColor Cyan
Write-Host "  [1] Start (fast — use existing images, no rebuild)" -ForegroundColor White
Write-Host "  [2] Rebuild + Start (slower — rebuilds images from scratch)" -ForegroundColor White
Write-Host ""
$choice = Read-Host "Enter 1 or 2"

Write-Host ""
if ($choice -eq "2") {
    Write-Host "Building and starting all containers..." -ForegroundColor Blue
    Set-Location $ProjectRoot
    docker compose up --build
} else {
    Write-Host "Starting all containers..." -ForegroundColor Blue
    Set-Location $ProjectRoot
    docker compose up
}
