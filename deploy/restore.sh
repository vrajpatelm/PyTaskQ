#!/usr/bin/env bash
# Put a PyTaskQ backup back (all jobs and accounts).
#
#   ./restore.sh backups/pytaskq-20261118-060000.rdb
#
# Steps, and why each one is needed:
#   1. Take a safety backup of the current data, in case you picked the wrong file.
#   2. Stop PyTaskQ so nothing writes while the data is swapped.
#   3. Replace Redis's data with the backup. Redis replays its append-only log
#      on startup and IGNORES dump.rdb when that log exists, so the old log
#      is removed rather than just copying the file in.
#   4. Start Redis once with the log switched off so it loads the backup, then
#      switch the log back on: Redis rewrites it from the restored data.
#   5. Start everything again and confirm the number of keys matches.
set -euo pipefail
cd "$(dirname "$0")"

FILE="${1:-}"
if [ -z "$FILE" ] || [ ! -f "$FILE" ]; then
  echo "usage: $0 backups/pytaskq-YYYYmmdd-HHMMSS.rdb [--yes]"
  echo "newest backups:"
  ls -1t backups/pytaskq-*.rdb 2>/dev/null | head -5 | sed 's/^/  /' || true
  exit 1
fi
[ -f .env ] || { echo "No .env next to docker-compose.prod.yml"; exit 1; }
set -a; . ./.env; set +a
: "${REDIS_PASSWORD:?REDIS_PASSWORD is not set in .env}"

COMPOSE=(docker compose -f docker-compose.prod.yml)
ABS="$(cd "$(dirname "$FILE")" && pwd)/$(basename "$FILE")"
TMP=pytaskq-restore
cli() { docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" "$TMP" redis-cli "$@"; }

if [ "${2:-}" != "--yes" ]; then
  read -r -p "Replace ALL current jobs and accounts with $(basename "$FILE")? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Cancelled. Nothing changed."; exit 1; }
fi

echo "1/5 Safety backup of the current data"
"${COMPOSE[@]}" exec -T backup sh /backup-loop.sh once || echo "    (skipped: the backup service isn't running)"

echo "2/5 Stopping PyTaskQ"
"${COMPOSE[@]}" stop web worker backup redis

echo "3/5 Replacing Redis data with $(basename "$FILE")"
"${COMPOSE[@]}" run --rm --no-deps -v "$ABS:/restore.rdb:ro" --entrypoint sh redis -c \
  'rm -rf /data/appendonlydir /data/appendonly.aof /data/dump.rdb && cp /restore.rdb /data/dump.rdb'

echo "4/5 Loading the backup and rebuilding the append-only log"
docker rm -f "$TMP" >/dev/null 2>&1 || true
"${COMPOSE[@]}" run -d --no-deps --name "$TMP" redis \
  redis-server --appendonly no --requirepass "$REDIS_PASSWORD" >/dev/null
for _ in $(seq 60); do cli ping 2>/dev/null | grep -q PONG && break; sleep 1; done
KEYS="$(cli dbsize)"
echo "    backup contains $KEYS keys"
cli config set appendonly yes >/dev/null
for _ in $(seq 120); do
  info="$(cli info persistence)"
  if echo "$info" | grep -q 'aof_enabled:1' && echo "$info" | grep -q 'aof_rewrite_in_progress:0' \
     && echo "$info" | grep -q 'aof_rewrite_scheduled:0'; then break; fi
  sleep 1
done
docker stop "$TMP" >/dev/null && docker rm "$TMP" >/dev/null

echo "5/5 Starting PyTaskQ"
"${COMPOSE[@]}" up -d
for _ in $(seq 60); do
  NOW="$("${COMPOSE[@]}" exec -T redis redis-cli dbsize 2>/dev/null || true)"
  [ "$NOW" = "$KEYS" ] && break
  sleep 1
done
if [ "$NOW" = "$KEYS" ]; then
  echo "Restored: Redis has $NOW keys, matching the backup."
else
  echo "WARNING: Redis has '$NOW' keys but the backup had $KEYS. Check: ${COMPOSE[*]} logs redis" >&2
  exit 1
fi
