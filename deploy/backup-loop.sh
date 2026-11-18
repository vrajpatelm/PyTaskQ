#!/bin/sh
# Saves a consistent snapshot of PyTaskQ's Redis (all jobs and accounts).
# Runs inside the `backup` container:
#   every BACKUP_INTERVAL_HOURS hours (default)     sh /backup-loop.sh
#   once, right now                                 sh /backup-loop.sh once
#
# `redis-cli --rdb` asks Redis for a fresh snapshot and streams it over the
# network, so the copy is consistent even while Redis is busy.
set -eu

HOST="${REDIS_HOST:-redis}"
PORT="${REDIS_PORT:-6379}"
DIR="${BACKUP_DIR:-/backups}"
KEEP="${BACKUP_KEEP:-28}"
HOURS="${BACKUP_INTERVAL_HOURS:-6}"
# The password comes from REDISCLI_AUTH, which redis-cli reads by itself.

backup_once() {
    mkdir -p "$DIR"
    file="$DIR/pytaskq-$(date -u +%Y%m%d-%H%M%S).rdb"
    if redis-cli -h "$HOST" -p "$PORT" --rdb "$file.partial" >/dev/null 2>&1; then
        mv "$file.partial" "$file"
        echo "$(date -u '+%F %T') saved $file ($(du -h "$file" | cut -f1))"
    else
        rm -f "$file.partial"
        echo "$(date -u '+%F %T') backup FAILED: could not reach Redis at $HOST:$PORT" >&2
        return 1
    fi
    # Keep only the newest $KEEP backups
    ls -1t "$DIR"/pytaskq-*.rdb 2>/dev/null | tail -n +"$((KEEP + 1))" | while read -r old; do
        rm -f "$old"
    done
}

if [ "${1:-}" = "once" ]; then
    backup_once
    exit $?
fi

echo "PyTaskQ backups: every ${HOURS}h into $DIR, keeping the newest $KEEP"
while true; do
    backup_once || true
    sleep "$((HOURS * 3600))"
done
