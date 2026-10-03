#!/usr/bin/env bash
# ==============================================================================
# server_bottleneck_capture.sh — find WHERE a load test saturates the server
# ==============================================================================
#
# WHY THIS EXISTS
#   "The API was slow at 1000 users" is a symptom, not a diagnosis. This script
#   samples the machine WHILE Locust runs, so you can tell which layer hit its
#   ceiling: the uvicorn event loop, Redis, the worker's process pool, or the
#   kernel's socket/SSFD limits.
#
# HOW TO USE
#   1. Make sure Locust runs on a DIFFERENT machine (otherwise you are
#      measuring the load generator's CPU, not the server's).
#   2. On the server, from the repo root:
#          ./tests/load/server_bottleneck_capture.sh 150
#      (150 = seconds to sample; start it a few seconds before Locust)
#   3. The report lands in tests/load/reports/<timestamp>/:
#          samples.csv — one row / 2s: host load + per-container CPU/Net/IO/PIDs
#          summary.txt — Redis INFO/SLOWLOG/latency, FD limits, sockets, logs
#
# HOW TO READ THE REPORT (the whole point)
#   - web CPU% pinned near 100*Ncores ... single uvicorn event loop is the
#     bottleneck. Fix = fewer Redis round-trips per request and/or --workers.
#   - redis CPU% pinned .................. Redis itself is the bottleneck
#     (usually means very chatty clients, not throughput - Redis is fast).
#   - redis --latency-history p99 spikes while redis CPU% is LOW
#     .................................... fsync / AOF rewrite / fork stalls,
#     not command throughput. Check INFO persistence.
#   - open_fds close to "Max open files" or ss -s TCP counts in the hundreds
#     .................................... connection exhaustion. That is what
#     turns into your 503 "Redis is unavailable" responses.
#   - netstat "listen queue overflow" > 0 . the kernel accept queue overflowed:
#     clients got RemoteDisconnected/resets because nobody called accept().
#
# This script only reads state. It changes nothing on the server.
# ==============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

DURATION="${1:-120}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/tests/load/reports/$STAMP"
mkdir -p "$OUT"

SUMMARY="$OUT/summary.txt"
CSV="$OUT/samples.csv"

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    else
        docker-compose "$@"
    fi
}

section() {
    { echo; echo "================================================================================"; echo "=== $*"; echo "================================================================================"; } >>"$SUMMARY"
}

log() {
    echo "$*" | tee -a "$SUMMARY" >/dev/null
    echo "$*" >>"$SUMMARY"
}

# ── Header: what are we even measuring? ───────────────────────────────────────
section "ENVIRONMENT — $(date -Is)"
{
    echo "capture duration : ${DURATION}s"
    echo "host             : $(uname -srm) — $(nproc) CPU cores"
    echo "repo             : $ROOT"
} >>"$SUMMARY"

{
    echo
    echo "--- docker compose ps ---"
    compose ps 2>&1
} >>"$SUMMARY"

# Clear the slowlog so the run's slow commands are attributable to THIS test.
compose exec -T redis redis-cli SLOWLOG RESET >/dev/null 2>&1 || true

# ── Continuous sampling ───────────────────────────────────────────────────────
echo "ts,load1,load5,load15,runq,tcp_total,name,cpu_pct,mem,net_io,block_io,pids" >"$CSV"
END=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "$END" ]; do
    TS="$(date +%s)"
    read -r L1 L5 L15 RUNQ _ < /proc/loadavg
    TCP_TOTAL="$(ss -s 2>/dev/null | awk '/^TCP:/{print $2}' | head -1)"

    docker stats --no-stream --format \
        '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' 2>/dev/null |
        while IFS= read -r line; do
            echo "$TS,$L1,$L5,$L15,$RUNQ,${TCP_TOTAL:-NA},$line" >>"$CSV"
        done

    sleep 2
done

# ── Web container internals (FD limits are the 503 smoking gun) ───────────────
section "UVICORN / WEB CONTAINER"
WEB_ID="$(compose ps -q web 2>/dev/null | head -1)"
if [ -n "${WEB_ID:-}" ]; then
    {
        echo "--- processes in the web container (PID 1 must be uvicorn) ---"
        docker exec "$WEB_ID" sh -c '
            for p in /proc/[0-9]*; do
                c=$(tr "\0" " " <"$p/cmdline" 2>/dev/null)
                [ -n "$c" ] && echo "$(basename "$p"): $c"
            done' 2>&1

        echo
        echo "--- file descriptor limits (inside container = as the app sees it) ---"
        docker exec "$WEB_ID" sh -c '
            grep -i "max open files" /proc/1/limits
            echo "open_fds_now=$(ls /proc/1/fd 2>/dev/null | wc -l)"' 2>&1

        echo
        echo "--- open FDs by type (sockets vs files) ---"
        docker exec "$WEB_ID" sh -c 'ls -l /proc/1/fd 2>/dev/null | awk "{print \$NF}" | sort | uniq -c | sort -rn | head -15' 2>&1
    } >>"$SUMMARY"
else
    echo "web container not found" >>"$SUMMARY"
fi

# ── Redis ─────────────────────────────────────────────────────────────────────
section "REDIS"
{
    echo "--- INFO clients (connected_clients / blocked_clients) ---"
    compose exec -T redis redis-cli INFO clients 2>&1

    echo
    echo "--- INFO stats (throughput + rejected connections) ---"
    compose exec -T redis redis-cli INFO stats 2>&1 |
        grep -E "instantaneous_ops_per_sec|total_commands_processed|total_connections_received|rejected_connections|expired_keys|evicted_keys|keyspace_hits|keyspace_misses" || true

    echo
    echo "--- INFO cpu + persistence (fsync / AOF rewrite stalls) ---"
    compose exec -T redis redis-cli INFO cpu 2>&1
    compose exec -T redis redis-cli INFO persistence 2>&1 |
        grep -E "aof_last_write_status|aof_rewrite_in_progress|aof_current_size|rdb_last_bgsave_status|rdb_changes" || true

    echo
    echo "--- config that affects load behaviour (and security) ---"
    compose exec -T redis redis-cli CONFIG GET bind protected-mode maxclients requirepass tcp-backlog timeout appendfsync 2>&1

    echo
    echo "--- SLOWLOG (commands slower than slowlog-log-slower-than) ---"
    compose exec -T redis redis-cli SLOWLOG GET 25 2>&1

    echo
    echo "--- latency-history, ~6s (p99 spikes with low CPU% = fsync/fork) ---"
    compose exec -T redis timeout 6 redis-cli --latency-history -i 1 2>&1 || true
} >>"$SUMMARY"

# ── Kernel: sockets, accept queues, retransmits ───────────────────────────────
section "SOCKETS & KERNEL"
{
    echo "--- ss -s ---"
    ss -s 2>&1

    echo
    echo "--- listening sockets ---"
    ss -ltn 2>&1 | head -20

    echo
    echo "--- backlog knobs ---"
    sysctl net.core.somaxconn net.ipv4.tcp_max_syn_backlog 2>&1

    echo
    echo "--- listen/overflow/reset counters (overflow > 0 = RemoteDisconnected) ---"
    (netstat -s 2>/dev/null || cat /proc/net/netstat 2>/dev/null) |
        grep -iE "overflow|listen|reset|retransmit" | head -20

    echo
    echo "--- host file descriptor limit ---"
    ulimit -n 2>&1
} >>"$SUMMARY"

# ── Application logs during the window ────────────────────────────────────────
section "APP LOGS (errors / redis / open files)"
{
    for svc in web worker redis; do
        echo "--- $svc (last 80 matching lines) ---"
        compose logs --tail 400 "$svc" 2>&1 |
            grep -iE "error|exception|redis|open files|refused|reset|overload|503" |
            tail -80 || true
        echo
    done
} >>"$SUMMARY"

# ── Verdict helper: who actually burned the CPU? ──────────────────────────────
section "TOP CPU CONSUMERS (max % observed during capture)"
{
    echo "NOTE: docker CPU% is relative to ONE core, so 200% = both cores of a 2-core box."
    echo
    awk -F, 'NR>1 && $8+0 > max[$7] { max[$7] = $8+0 }
             END { for (n in max) printf "%8.1f%%  %s\n", max[n], n }' "$CSV" |
        sort -rn

    echo
    awk -F, 'NR>1 && $2+0 > m { m = $2+0 }
             END { printf "max host load1: %.2f  (cores: %s)\n", m, "'"$(nproc)"'" }' "$CSV"

    echo
    echo "Interpretation:"
    echo "  * one container near 100% x cores -> that process is the ceiling"
    echo "  * every container low but load high -> the LOAD GENERATOR shares the box"
    echo "    (Locust must not run on the server)"
} >>"$SUMMARY"

echo
echo "Report written to:"
echo "  $OUT/samples.csv"
echo "  $OUT/summary.txt"
echo
echo "Tip: run this alongside Locust from another machine, then compare the CSV's"
echo "     per-container CPU against the rps plateau in the Locust report."
