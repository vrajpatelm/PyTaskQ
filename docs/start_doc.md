Terminal 1 — Redis (WSL)
─────────────────────────────────────
wsl sudo service redis-server start

Terminal 2 — Worker
─────────────────────────────────────
cd C:\Users\VRAJ\redis
.venv\Scripts\Activate.ps1       ← activate venv first
python worker.py                  ← now starts correctly

Terminal 3 — API Server  
─────────────────────────────────────
cd C:\Users\VRAJ\redis
.venv\Scripts\Activate.ps1
uvicorn app:app --reload
